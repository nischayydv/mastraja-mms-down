import asyncio
import sys
import os
import re
import base64
import logging
from datetime import datetime
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse

import aiohttp
import aiofiles
import aiosqlite
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import RPCError
from pyrogram.enums import ParseMode
from aiohttp import web

load_dotenv()

# ========= CONFIG ==========
BASE_URL      = "https://mastiraja.com"
API_ID        = int(os.getenv("API_ID", 0))
API_HASH      = os.getenv("API_HASH", "")
BOT_TOKEN     = os.getenv("BOT_TOKEN", "")
CHANNEL_ID    = os.getenv("CHANNEL_ID", "")
DOWNLOAD_DIR  = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")
DB_FILE       = os.getenv("DB_FILE", "/tmp/videos.db")
LOG_FILE      = os.getenv("LOG_FILE", "/tmp/crawler.log")
PORT          = int(os.getenv("PORT", 8000))
USER_AGENT    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

MAX_DOWNLOADS    = 5
REQUEST_TIMEOUT  = 30
MAX_RETRIES      = 3
# ===========================

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required env vars: API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID")

# ---------- Logging ----------
os.makedirs(os.path.dirname(LOG_FILE) or ".", exist_ok=True)
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setFormatter(fmt)
ch = logging.StreamHandler()
ch.setFormatter(fmt)
logger.addHandler(fh)
logger.addHandler(ch)


# ---------- Global State ----------
class CrawlerState:
    def __init__(self):
        self.running          = False
        self.paused           = False
        self.task: Optional[asyncio.Task] = None
        self.posts_queue: List[str] = []
        self.total_posts      = 0
        self.processed        = 0
        self.status           = "idle"
        self.lock             = asyncio.Lock()
        self.waiting_for_single = False
        self.started_at: Optional[datetime] = None

state = CrawlerState()


# ---------- Database ----------
async def init_db():
    os.makedirs(os.path.dirname(DB_FILE) or ".", exist_ok=True)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS videos (
                post_id     INTEGER PRIMARY KEY,
                title       TEXT,
                video_url   TEXT,
                file_path   TEXT,
                category    TEXT,
                tags        TEXT,
                duration    TEXT,
                views       TEXT,
                description TEXT,
                uploaded    INTEGER DEFAULT 0,
                upload_date TEXT,
                last_checked TEXT
            )
        """)
        await db.commit()


async def is_uploaded(post_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT 1 FROM videos WHERE post_id=? AND uploaded=1", (post_id,)
        ) as cur:
            return await cur.fetchone() is not None


async def mark_uploaded(post_id, file_path, title, video_url,
                        category, tags, duration, views, description):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            INSERT OR REPLACE INTO videos
            (post_id,title,video_url,file_path,category,tags,duration,
             views,description,uploaded,upload_date,last_checked)
            VALUES (?,?,?,?,?,?,?,?,?,1,datetime('now'),datetime('now'))
        """, (post_id, title, video_url, file_path,
              category, tags, duration, views, description))
        await db.commit()


async def get_uploaded(limit=20, offset=0) -> List[Dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT post_id,title,category,duration,upload_date "
            "FROM videos WHERE uploaded=1 ORDER BY upload_date DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ) as cur:
            rows = await cur.fetchall()
    return [{"post_id": r[0], "title": r[1], "category": r[2],
             "duration": r[3], "upload_date": r[4]} for r in rows]


async def count_uploaded() -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=1") as c:
            r = await c.fetchone()
    return r[0] if r else 0


async def count_pending() -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=0") as c:
            r = await c.fetchone()
    return r[0] if r else 0


# ---------- Scraping ----------
async def fetch_soup(session: aiohttp.ClientSession, url: str):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                headers={"User-Agent": USER_AGENT},
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:
                resp.raise_for_status()
                html = await resp.text()
                return BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.warning(f"Fetch attempt {attempt+1} failed for {url}: {e}")
            await asyncio.sleep(2 ** attempt)
    return None


def extract_post_links(soup: BeautifulSoup) -> List[str]:
    links = []
    for article in soup.find_all("article", class_="thumb-block"):
        a = article.find("a", href=True)
        if a:
            href = a["href"]
            if href.startswith("/"):
                href = urljoin(BASE_URL, href)
            if href not in links:
                links.append(href)
    return links


def extract_pagination_links(soup: BeautifulSoup) -> List[str]:
    pag = soup.find("div", class_="pagination")
    if not pag:
        return []
    urls = []
    for a in pag.find_all("a", href=True):
        h = a["href"]
        if "/page/" in h:
            full = urljoin(BASE_URL, h)
            if full not in urls:
                urls.append(full)
    return urls


def best_video_source(decoded_html: str) -> Optional[str]:
    soup = BeautifulSoup(decoded_html, "html.parser")
    sources = soup.find_all("source")
    if not sources:
        v = soup.find("video")
        return v.get("src") if v else None
    best, best_q = None, -1
    for s in sources:
        src = s.get("src")
        if not src:
            continue
        q = 0
        for attr in ("quality", "data-quality", "bitrate", "res"):
            val = s.get(attr)
            if val:
                m = re.search(r"(\d+)", str(val))
                if m:
                    q = int(m.group(1))
                    break
        if q == 0:
            m = re.search(r"(\d+)p", src)
            if m:
                q = int(m.group(1))
        if q > best_q:
            best_q, best = q, src
    return best or (sources[-1].get("src") if sources else None)


async def extract_video_info(session: aiohttp.ClientSession, post_url: str) -> Optional[Dict]:
    soup = await fetch_soup(session, post_url)
    if not soup:
        return None

    m = re.search(r"/(\d+)/?$", post_url)
    post_id = int(m.group(1)) if m else None

    h1 = soup.find("h1", itemprop="name")
    title = h1.get_text(strip=True) if h1 else "Untitled"

    iframe = soup.find("iframe", src=True)
    video_url = None
    if iframe:
        src = iframe["src"]
        q = urlparse(src).query
        if q.startswith("q="):
            try:
                decoded = base64.b64decode(q[2:]).decode("utf-8")
                video_url = best_video_source(decoded)
            except Exception as e:
                logger.error(f"Decode error {post_url}: {e}")

    if not video_url:
        vt = soup.find("video")
        if vt:
            video_url = vt.get("src") or (vt.find("source") or {}).get("src")

    if not video_url:
        return None

    cat_tag = soup.find("a", class_="label", title=True)
    category = cat_tag.get_text(strip=True) if cat_tag else "Uncategorized"

    tags = [t.get_text(strip=True)
            for t in soup.find_all("a", class_="label")
            if "fa-tag" in str(t) or "/tag/" in t.get("href", "")]

    dur   = soup.find("span", class_="duration")
    views = soup.find("span", class_="views")
    desc  = soup.find("div", class_="video-description")
    desc_text = ""
    if desc:
        p = desc.find("p")
        if p:
            desc_text = p.get_text(strip=True)

    return {
        "post_id":     post_id,
        "title":       title,
        "video_url":   video_url,
        "category":    category,
        "tags":        ", ".join(tags),
        "duration":    dur.get_text(strip=True) if dur else "",
        "views":       views.get_text(strip=True).replace("i", "").strip() if views else "",
        "description": desc_text,
    }


async def download_video(session: aiohttp.ClientSession, url: str, path: str) -> Optional[str]:
    if os.path.exists(path):
        return path
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                headers={"User-Agent": USER_AGENT, "Referer": BASE_URL},
                timeout=aiohttp.ClientTimeout(total=None)
            ) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get("content-length", 0))
                done  = 0
                async with aiofiles.open(path, "wb") as f:
                    async for chunk in resp.content.iter_chunked(65536):
                        await f.write(chunk)
                        done += len(chunk)
                        if total:
                            pct = done / total * 100
                            sys.stdout.write(f"\r  Download {os.path.basename(path)}: {pct:.1f}%")
                            sys.stdout.flush()
                sys.stdout.write("\n")
                return path
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} failed: {e}")
            if os.path.exists(path):
                os.remove(path)
            await asyncio.sleep(2 ** attempt)
    return None


async def upload_video(bot: Client, filepath: str, info: Dict) -> bool:
    caption = f"📹 *{info['title']}*\n"
    if info.get("category"):  caption += f"📂 {info['category']}\n"
    if info.get("tags"):      caption += f"🏷️ {info['tags']}\n"
    if info.get("duration"):  caption += f"⏱️ {info['duration']}\n"
    if info.get("description"):
        d = info["description"]
        caption += f"📝 {d[:200]}{'...' if len(d)>200 else ''}\n"
    caption += "\n@NY_BOTS"
    try:
        await bot.send_video(
            chat_id=CHANNEL_ID,
            video=filepath,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            supports_streaming=True,
        )
        return True
    except RPCError as e:
        logger.error(f"Upload RPCError: {e}")
        return False
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return False


# ---------- Crawler Task ----------
async def crawl_and_process(bot: Client):
    async with state.lock:
        if state.running:
            return
        state.running    = True
        state.paused     = False
        state.processed  = 0
        state.status     = "running"
        state.started_at = datetime.now()

    logger.info("Crawler started.")

    try:
        async with aiohttp.ClientSession() as session:
            # --- collect posts ---
            to_visit, visited, all_posts = [BASE_URL], set(), set()
            while to_visit and state.running:
                if state.paused:
                    await asyncio.sleep(1)
                    continue
                url = to_visit.pop()
                if url in visited:
                    continue
                visited.add(url)
                soup = await fetch_soup(session, url)
                if soup:
                    all_posts.update(extract_post_links(soup))
                    for p in extract_pagination_links(soup):
                        if p not in visited:
                            to_visit.append(p)
                await asyncio.sleep(0.2)

            if not state.running:
                return

            state.total_posts  = len(all_posts)
            state.posts_queue  = list(all_posts)
            logger.info(f"Found {len(all_posts)} posts.")

            # --- process posts ---
            sem = asyncio.Semaphore(MAX_DOWNLOADS)

            async def process(post_url):
                async with sem:
                    while state.paused and state.running:
                        await asyncio.sleep(1)
                    if not state.running:
                        return
                    try:
                        info = await extract_video_info(session, post_url)
                        if not info or not info["post_id"]:
                            return
                        if await is_uploaded(info["post_id"]):
                            return
                        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                        fname = os.path.join(
                            DOWNLOAD_DIR,
                            f"{info['post_id']}_{os.path.basename(info['video_url']).split('?')[0]}"
                        )
                        fp = await download_video(session, info["video_url"], fname)
                        if not fp:
                            return
                        ok = await upload_video(bot, fp, info)
                        if ok:
                            await mark_uploaded(
                                info["post_id"], fp, info["title"], info["video_url"],
                                info["category"], info["tags"], info["duration"],
                                info["views"], info["description"]
                            )
                            try:
                                os.remove(fp)
                            except Exception:
                                pass
                        else:
                            logger.warning(f"Upload failed: post {info['post_id']}")
                    except Exception as e:
                        logger.error(f"Error processing {post_url}: {e}")
                    finally:
                        state.processed += 1

            tasks = [asyncio.create_task(process(u)) for u in all_posts if state.running]
            await asyncio.gather(*tasks, return_exceptions=True)

    except asyncio.CancelledError:
        logger.info("Crawler task cancelled.")
    except Exception as e:
        logger.error(f"Crawler error: {e}")
    finally:
        state.running = False
        state.status  = "stopped"
        logger.info("Crawler finished.")


# ---------- Keyboards ----------
def main_menu():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start",   callback_data="start"),
         InlineKeyboardButton("⏸ Pause",   callback_data="pause"),
         InlineKeyboardButton("▶️ Resume",  callback_data="resume")],
        [InlineKeyboardButton("⏹ Stop",    callback_data="stop"),
         InlineKeyboardButton("📊 Status", callback_data="status")],
        [InlineKeyboardButton("📄 Logs",   callback_data="logs"),
         InlineKeyboardButton("📜 History",callback_data="history")],
        [InlineKeyboardButton("🔗 Single", callback_data="single"),
         InlineKeyboardButton("❓ Help",   callback_data="help")],
    ])


def history_kbd(page: int, total_pages: int):
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"history_{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="nop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"history_{page+1}"))
    return InlineKeyboardMarkup([nav, [InlineKeyboardButton("🔙 Back", callback_data="menu")]])


def cancel_kbd():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="menu")]])


# ---------- Bot ----------
def build_bot() -> Client:
    bot = Client(
        "mastiraja_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        workers=20,
    )

    # /start
    @bot.on_message(filters.command("start"))
    async def cmd_start(client, msg: Message):
        await msg.reply(
            "🤖 *MastiRaja Crawler Bot*\nChoose an action:",
            reply_markup=main_menu(),
            parse_mode=ParseMode.MARKDOWN,
        )

    # text messages (for single URL input)
    @bot.on_message(filters.text & ~filters.command(["start","pause","resume",
                                                      "stop","status","logs",
                                                      "history","single","help"]))
    async def handle_text(client, msg: Message):
        if state.waiting_for_single:
            await do_single(client, msg)
        else:
            await msg.reply("Use /start for the menu.")

    # callbacks
    @bot.on_callback_query()
    async def handle_cb(client, cb: CallbackQuery):
        d = cb.data

        if d == "start":
            if state.running:
                await cb.answer("Already running.", show_alert=True); return
            state.task = asyncio.create_task(crawl_and_process(client))
            await cb.answer("Crawler started.")
            await cb.message.edit_text("🔄 Crawler started.", reply_markup=main_menu())

        elif d == "pause":
            if not state.running:
                await cb.answer("Not running.", show_alert=True); return
            if state.paused:
                await cb.answer("Already paused.", show_alert=True); return
            state.paused = True
            state.status = "paused"
            await cb.answer("Paused.")
            await cb.message.edit_text("⏸ Paused.", reply_markup=main_menu())

        elif d == "resume":
            if not state.running:
                await cb.answer("Not running.", show_alert=True); return
            if not state.paused:
                await cb.answer("Not paused.", show_alert=True); return
            state.paused = False
            state.status = "running"
            await cb.answer("Resumed.")
            await cb.message.edit_text("▶️ Resumed.", reply_markup=main_menu())

        elif d == "stop":
            if not state.running:
                await cb.answer("Not running.", show_alert=True); return
            state.running = False
            state.paused  = False
            state.status  = "stopped"
            if state.task and not state.task.done():
                state.task.cancel()
                try: await state.task
                except Exception: pass
            await cb.answer("Stopped.")
            await cb.message.edit_text("⏹ Stopped.", reply_markup=main_menu())

        elif d == "status":
            pending  = await count_pending()
            uploaded = await count_uploaded()
            elapsed  = ""
            if state.started_at and state.running:
                secs = int((datetime.now() - state.started_at).total_seconds())
                elapsed = f"\n• Elapsed: {secs//3600:02d}h {(secs%3600)//60:02d}m {secs%60:02d}s"
            text = (
                f"📊 *Status*\n"
                f"• State: `{state.status}`\n"
                f"• Total found: {state.total_posts}\n"
                f"• Processed: {state.processed}\n"
                f"• DB pending: {pending}\n"
                f"• Uploaded: {uploaded}"
                f"{elapsed}"
            )
            await cb.answer()
            await cb.message.edit_text(text, reply_markup=main_menu(), parse_mode=ParseMode.MARKDOWN)

        elif d == "logs":
            try:
                with open(LOG_FILE, "r") as f:
                    lines = f.readlines()
                tail = "".join(lines[-100:]) if lines else "No logs yet."
                if len(tail) > 3800:
                    await cb.message.reply_document(LOG_FILE, caption="📄 Logs")
                    await cb.answer("Logs sent as file.")
                else:
                    await cb.answer()
                    await cb.message.edit_text(
                        f"📄 *Logs*\n```\n{tail}\n```",
                        reply_markup=main_menu(),
                        parse_mode=ParseMode.MARKDOWN
                    )
            except Exception as e:
                await cb.answer()
                await cb.message.edit_text(f"❌ {e}", reply_markup=main_menu())

        elif d == "history" or d.startswith("history_"):
            page = 1
            if "_" in d:
                part = d.split("_", 1)[1]
                if part.isdigit():
                    page = int(part)
            per   = 10
            total = await count_uploaded()
            pages = max(1, (total + per - 1) // per)
            rows  = await get_uploaded(limit=per, offset=(page-1)*per)
            if not rows:
                text = "📭 No uploaded videos yet."
                kbd  = main_menu()
            else:
                text = f"📜 *Uploaded (page {page}/{pages})*\n\n"
                for r in rows:
                    text += f"• {r['title'][:45]} | {r['category']} | {r['duration']}\n"
                kbd = history_kbd(page, pages)
            await cb.answer()
            await cb.message.edit_text(text, reply_markup=kbd, parse_mode=ParseMode.MARKDOWN)

        elif d == "single":
            state.waiting_for_single = True
            await cb.answer()
            await cb.message.edit_text(
                "🔗 Send the full post URL:\n`https://mastiraja.com/...`",
                reply_markup=cancel_kbd(),
                parse_mode=ParseMode.MARKDOWN
            )

        elif d == "help":
            await cb.answer()
            await cb.message.edit_text(
                "❓ *Help*\n"
                "▶️ Start — begin full crawl\n"
                "⏸ Pause/Resume — pause or resume crawl\n"
                "⏹ Stop — stop crawl\n"
                "📊 Status — current stats\n"
                "📄 Logs — recent log output\n"
                "📜 History — uploaded videos\n"
                "🔗 Single — upload one post by URL",
                reply_markup=main_menu(),
                parse_mode=ParseMode.MARKDOWN
            )

        elif d == "menu":
            state.waiting_for_single = False
            await cb.answer()
            await cb.message.edit_text("🤖 Main Menu", reply_markup=main_menu())

        elif d == "nop":
            await cb.answer()

        else:
            await cb.answer("Unknown action.")

    return bot


async def do_single(client: Client, msg: Message):
    state.waiting_for_single = False
    url = msg.text.strip()
    if not url.startswith("http"):
        await msg.reply("❌ Invalid URL. Must start with http.")
        return
    prog = await msg.reply("⏳ Processing...")
    try:
        async with aiohttp.ClientSession() as session:
            info = await extract_video_info(session, url)
            if not info:
                await prog.edit_text("❌ Could not extract info from that URL.")
                return
            if await is_uploaded(info["post_id"]):
                await prog.edit_text("ℹ️ Already uploaded.")
                return
            os.makedirs(DOWNLOAD_DIR, exist_ok=True)
            fname = os.path.join(
                DOWNLOAD_DIR,
                f"{info['post_id']}_{os.path.basename(info['video_url']).split('?')[0]}"
            )
            await prog.edit_text("⬇️ Downloading...")
            fp = await download_video(session, info["video_url"], fname)
            if not fp:
                await prog.edit_text("❌ Download failed.")
                return
            await prog.edit_text("⬆️ Uploading to channel...")
            ok = await upload_video(client, fp, info)
            if ok:
                await mark_uploaded(
                    info["post_id"], fp, info["title"], info["video_url"],
                    info["category"], info["tags"], info["duration"],
                    info["views"], info["description"]
                )
                try: os.remove(fp)
                except Exception: pass
                await prog.edit_text(f"✅ Uploaded: *{info['title']}*",
                                     parse_mode=ParseMode.MARKDOWN)
            else:
                await prog.edit_text("❌ Upload to channel failed.")
    except Exception as e:
        logger.error(f"Single URL error: {e}")
        await prog.edit_text(f"❌ Error: {e}")
    finally:
        await msg.reply("Back to menu:", reply_markup=main_menu())


# ---------- Health check server ----------
async def health_server():
    app = web.Application()
    async def ok(r): return web.Response(text="OK")
    app.router.add_get("/", ok)
    app.router.add_get("/health", ok)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health server on port {PORT}")
    await asyncio.Event().wait()


# ---------- Main ----------
async def main():
    await init_db()
    asyncio.create_task(health_server())

    bot = build_bot()
    logger.info("Starting bot...")
    await bot.start()
    logger.info("Bot running. Press Ctrl+C to stop.")
    await asyncio.Event().wait()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down.")
