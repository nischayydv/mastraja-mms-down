#!/usr/bin/env python3
"""
MastiRaja Crawler – Telegram Bot Manager with HTTP Health Check
Author: Potato
"""

import asyncio
import sys
import os
import re
import base64
import logging
from datetime import datetime
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse

# ---------- FIX 1: Create event loop early ----------
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ---------- FIX 2: Patch Pyrogram's Identifier ----------
import aiohttp
import aiofiles
import aiosqlite
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pyrogram import Client
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
)
from pyrogram.errors import RPCError, Unauthorized, InvalidApiKey
from pyrogram.enums import ParseMode

from pyrogram.types.pyromod import Identifier
if not hasattr(Identifier, '__annotations__'):
    Identifier.__annotations__ = {}

load_dotenv()

# ========= CONFIG ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")
DB_FILE = os.getenv("DB_FILE", "/tmp/videos.db")
LOG_FILE = os.getenv("LOG_FILE", "/tmp/crawler.log")
PORT = int(os.getenv("PORT", 8000))
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

MAX_PAGE_FETCH = 5
MAX_VIDEO_EXTRACT = 10
MAX_DOWNLOADS = 5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
# ===========================

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required environment variables")

# ---------- Logging ----------
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

# ---------- Global State ----------
class CrawlerState:
    def __init__(self):
        self.running = False
        self.paused = False
        self.task: Optional[asyncio.Task] = None
        self.posts_queue: List[str] = []
        self.total_posts = 0
        self.processed = 0
        self.status = "idle"
        self.lock = asyncio.Lock()
        self.waiting_for_single = False

state = CrawlerState()

# ---------- Database ----------
async def init_db():
    os.makedirs(os.path.dirname(DB_FILE) or '.', exist_ok=True)
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS videos (
                post_id INTEGER PRIMARY KEY,
                title TEXT,
                video_url TEXT,
                file_path TEXT,
                category TEXT,
                tags TEXT,
                duration TEXT,
                views TEXT,
                description TEXT,
                uploaded INTEGER DEFAULT 0,
                upload_date TEXT,
                last_checked TEXT
            )
        ''')
        await db.commit()

async def is_video_uploaded(post_id: int) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT 1 FROM videos WHERE post_id = ? AND uploaded = 1", (post_id,)) as cur:
            row = await cur.fetchone()
            return row is not None

async def mark_uploaded(post_id, file_path, title, video_url, category, tags, duration, views, description):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT OR REPLACE INTO videos
            (post_id, title, video_url, file_path, category, tags, duration, views, description, uploaded, upload_date, last_checked)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
        ''', (post_id, title, video_url, file_path, category, tags, duration, views, description))
        await db.commit()

async def update_last_checked(post_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE videos SET last_checked = datetime('now') WHERE post_id = ?", (post_id,))
        await db.commit()

async def get_all_uploaded(limit: int = 20, offset: int = 0) -> List[Dict]:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT post_id, title, category, duration, upload_date FROM videos WHERE uploaded=1 ORDER BY upload_date DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ) as cur:
            rows = await cur.fetchall()
            return [{"post_id": r[0], "title": r[1], "category": r[2], "duration": r[3], "upload_date": r[4]} for r in rows]

async def get_pending_count() -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=0") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

async def get_total_uploaded_count() -> int:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=1") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

# ---------- Scraping Helpers ----------
async def fetch_soup(session: aiohttp.ClientSession, url: str):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                html = await resp.text()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.warning(f"Fetch {url} attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    return None

def extract_post_links(soup: BeautifulSoup) -> List[str]:
    links = []
    for article in soup.find_all('article', class_='thumb-block'):
        a = article.find('a', href=True)
        if a:
            href = a['href']
            if href.startswith('/'):
                href = urljoin(BASE_URL, href)
            if href not in links:
                links.append(href)
    return links

def extract_pagination_links(soup: BeautifulSoup) -> List[str]:
    pag = soup.find('div', class_='pagination')
    if not pag:
        return []
    urls = []
    for a in pag.find_all('a', href=True):
        h = a['href']
        if '/page/' in h:
            full = urljoin(BASE_URL, h)
            if full not in urls:
                urls.append(full)
    return urls

def get_highest_quality_source(decoded_html: str) -> Optional[str]:
    soup = BeautifulSoup(decoded_html, 'html.parser')
    sources = soup.find_all('source')
    if not sources:
        video = soup.find('video')
        if video and video.get('src'):
            return video['src']
        return None
    best = None
    best_quality = -1
    for src_tag in sources:
        src = src_tag.get('src')
        if not src:
            continue
        quality = None
        for attr in ['quality', 'data-quality', 'bitrate', 'res']:
            val = src_tag.get(attr)
            if val:
                try:
                    num = re.search(r'(\d+)', str(val))
                    if num:
                        quality = int(num.group(1))
                        break
                except:
                    pass
        if quality is None:
            m = re.search(r'(\d+)p', src)
            if m:
                quality = int(m.group(1))
            else:
                quality = 0
        if quality > best_quality:
            best_quality = quality
            best = src
    return best if best else (sources[-1].get('src') if sources else None)

async def extract_video_info(session: aiohttp.ClientSession, post_url: str) -> Optional[Dict]:
    soup = await fetch_soup(session, post_url)
    if not soup:
        return None
    post_id = None
    m = re.search(r'/(\d+)/?$', post_url)
    if m:
        post_id = int(m.group(1))
    title_tag = soup.find('h1', itemprop='name')
    title = title_tag.get_text(strip=True) if title_tag else "Untitled"
    iframe = soup.find('iframe', src=True)
    video_url = None
    if iframe:
        src = iframe['src']
        parsed = urlparse(src)
        q = parsed.query
        if q.startswith('q='):
            b64 = q[2:]
            try:
                decoded = base64.b64decode(b64).decode('utf-8')
                video_url = get_highest_quality_source(decoded)
            except Exception as e:
                logger.error(f"Decode error for {post_url}: {e}")
    if not video_url:
        video_tag = soup.find('video')
        if video_tag:
            video_url = video_tag.get('src')
            if not video_url:
                source = video_tag.find('source')
                if source:
                    video_url = source.get('src')
    if not video_url:
        return None
    cat_tag = soup.find('a', class_='label', title=True)
    category = cat_tag.get_text(strip=True) if cat_tag else "Uncategorized"
    tags = []
    for t in soup.find_all('a', class_='label'):
        if 'fa-tag' in str(t) or '/tag/' in t.get('href', ''):
            tags.append(t.get_text(strip=True))
    tags_str = ', '.join(tags)
    dur = soup.find('span', class_='duration')
    duration = dur.get_text(strip=True) if dur else ''
    views_span = soup.find('span', class_='views')
    views = views_span.get_text(strip=True).replace('i', '').strip() if views_span else ''
    desc_div = soup.find('div', class_='video-description')
    desc = ''
    if desc_div:
        p = desc_div.find('p')
        if p:
            desc = p.get_text(strip=True)
    return {
        'post_id': post_id,
        'title': title,
        'video_url': video_url,
        'category': category,
        'tags': tags_str,
        'duration': duration,
        'views': views,
        'description': desc,
    }

async def download_video(session: aiohttp.ClientSession, video_url: str, filepath: str) -> Optional[str]:
    if os.path.exists(filepath):
        return filepath
    for attempt in range(MAX_RETRIES):
        try:
            headers = {'User-Agent': USER_AGENT, 'Referer': BASE_URL}
            async with session.get(video_url, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                total = int(resp.headers.get('content-length', 0))
                downloaded = 0
                async with aiofiles.open(filepath, 'wb') as f:
                    async for chunk in resp.content.iter_chunked(8192):
                        await f.write(chunk)
                        downloaded += len(chunk)
                        if total:
                            sys.stdout.write(f"\rDownloading {os.path.basename(filepath)}: {downloaded/total*100:.1f}%")
                            sys.stdout.flush()
                sys.stdout.write("\n")
                return filepath
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    return None

async def upload_video(client: Client, filepath: str, info: Dict) -> bool:
    caption = f"📹 *{info['title']}*\n"
    if info['category']:
        caption += f"📂 Category: {info['category']}\n"
    if info['tags']:
        caption += f"🏷️ Tags: {info['tags']}\n"
    if info['duration']:
        caption += f"⏱️ Duration: {info['duration']}\n"
    if info['description']:
        desc_short = info['description'][:200] + ('...' if len(info['description']) > 200 else '')
        caption += f"📝 {desc_short}\n"
    caption += f"\nUploaded by @NY_BOTS"
    try:
        await client.send_video(
            chat_id=CHANNEL_ID,
            video=filepath,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            supports_streaming=True,
            progress=lambda current, total: sys.stdout.write(f"\rUploading {os.path.basename(filepath)}: {current/total*100:.1f}%") or sys.stdout.flush()
        )
        sys.stdout.write("\n")
        return True
    except RPCError as e:
        logger.error(f"Upload error: {e}")
        return False

# ---------- Crawler Task ----------
async def crawl_and_process():
    async with state.lock:
        if state.running:
            return
        state.running = True
        state.paused = False
        state.status = "running"
        logger.info("Crawler started.")

    async with aiohttp.ClientSession() as session:
        to_visit = [BASE_URL]
        visited = set()
        all_posts = set()
        while to_visit and state.running:
            if state.paused:
                await asyncio.sleep(2)
                continue
            url = to_visit.pop()
            if url in visited:
                continue
            visited.add(url)
            soup = await fetch_soup(session, url)
            if not soup:
                continue
            all_posts.update(extract_post_links(soup))
            for p in extract_pagination_links(soup):
                if p not in visited:
                    to_visit.append(p)
            await asyncio.sleep(0.3)
        if not state.running:
            state.status = "stopped"
            state.running = False
            return
        state.total_posts = len(all_posts)
        state.posts_queue = list(all_posts)
        logger.info(f"Found {len(all_posts)} posts.")

        client = Client("crawler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, workers=10)
        await client.start()
        logger.info("Pyrogram upload client started.")

        sem = asyncio.Semaphore(MAX_DOWNLOADS)

        async def limited_process(post_url):
            async with sem:
                while state.paused and state.running:
                    await asyncio.sleep(1)
                if not state.running:
                    return
                info = await extract_video_info(session, post_url)
                if not info or not info['post_id']:
                    return
                if await is_video_uploaded(info['post_id']):
                    return
                os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                filename = os.path.join(DOWNLOAD_DIR, f"{info['post_id']}_{os.path.basename(info['video_url'])}")
                filepath = await download_video(session, info['video_url'], filename)
                if not filepath:
                    return
                success = await upload_video(client, filepath, info)
                if success:
                    await mark_uploaded(
                        info['post_id'], filepath, info['title'], info['video_url'],
                        info['category'], info['tags'], info['duration'],
                        info['views'], info['description']
                    )
                    try:
                        os.remove(filepath)
                    except:
                        pass
                else:
                    logger.warning(f"Upload failed for post {info['post_id']}, file kept.")
                state.processed += 1

        tasks = []
        for url in all_posts:
            if not state.running:
                break
            tasks.append(asyncio.create_task(limited_process(url)))
            while state.paused and state.running:
                await asyncio.sleep(1)
            if not state.running:
                break
        await asyncio.gather(*tasks)
        await client.stop()
        logger.info("Crawler finished.")
        state.running = False
        state.status = "stopped"

# ---------- Inline Keyboards ----------
def get_main_menu():
    buttons = [
        [
            InlineKeyboardButton("▶️ Start", callback_data="start"),
            InlineKeyboardButton("⏸ Pause", callback_data="pause"),
            InlineKeyboardButton("▶️ Resume", callback_data="resume"),
        ],
        [
            InlineKeyboardButton("⏹ Stop", callback_data="stop"),
            InlineKeyboardButton("📊 Status", callback_data="status"),
        ],
        [
            InlineKeyboardButton("📄 Logs", callback_data="logs"),
            InlineKeyboardButton("📜 History", callback_data="history"),
        ],
        [
            InlineKeyboardButton("🔗 Single Download", callback_data="single"),
            InlineKeyboardButton("❓ Help", callback_data="help"),
            InlineKeyboardButton("🏓 Ping", callback_data="ping"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)

def get_history_pagination(page: int, total_pages: int):
    buttons = []
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("◀️ Prev", callback_data=f"history_{page-1}"))
    nav.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="history_nop"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("Next ▶️", callback_data=f"history_{page+1}"))
    if nav:
        buttons.append(nav)
    buttons.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def get_single_cancel():
    return InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel", callback_data="menu")]])

# ---------- Bot Handlers ----------
async def start_cmd(client: Client, message: Message):
    await message.reply(
        "🤖 *MastiRaja Crawler Bot*\nUse the buttons below.",
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def ping_cmd(client: Client, message: Message):
    await message.reply("🏓 Pong! Bot is alive and responsive.")

async def start_crawler_cb(client: Client, callback: CallbackQuery):
    if state.running:
        await callback.answer("Already running.", show_alert=True)
        return
    state.task = asyncio.create_task(crawl_and_process())
    await callback.answer("Started.", show_alert=True)
    await callback.message.edit_text("🔄 Crawler started.", reply_markup=get_main_menu())

async def pause_crawler_cb(client: Client, callback: CallbackQuery):
    if not state.running:
        await callback.answer("Not running.", show_alert=True)
        return
    if state.paused:
        await callback.answer("Already paused.", show_alert=True)
        return
    state.paused = True
    state.status = "paused"
    await callback.answer("Paused.", show_alert=True)
    await callback.message.edit_text("⏸ Paused.", reply_markup=get_main_menu())

async def resume_crawler_cb(client: Client, callback: CallbackQuery):
    if not state.running:
        await callback.answer("Not running.", show_alert=True)
        return
    if not state.paused:
        await callback.answer("Not paused.", show_alert=True)
        return
    state.paused = False
    state.status = "running"
    await callback.answer("Resumed.", show_alert=True)
    await callback.message.edit_text("▶️ Resumed.", reply_markup=get_main_menu())

async def stop_crawler_cb(client: Client, callback: CallbackQuery):
    if not state.running:
        await callback.answer("Not running.", show_alert=True)
        return
    state.running = False
    state.paused = False
    state.status = "stopped"
    if state.task and not state.task.done():
        state.task.cancel()
        try:
            await state.task
        except:
            pass
    await callback.answer("Stopped.", show_alert=True)
    await callback.message.edit_text("⏹ Stopped.", reply_markup=get_main_menu())

async def status_cb(client: Client, callback: CallbackQuery):
    pending = await get_pending_count()
    uploaded = await get_total_uploaded_count()
    text = (
        f"📊 *Status*\n"
        f"• State: `{state.status}`\n"
        f"• Running: {state.running}\n"
        f"• Paused: {state.paused}\n"
        f"• Total: {state.total_posts}\n"
        f"• Processed: {state.processed}\n"
        f"• Pending DB: {pending}\n"
        f"• Uploaded: {uploaded}"
    )
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=get_main_menu(), parse_mode=ParseMode.MARKDOWN)

async def logs_cb(client: Client, callback: CallbackQuery):
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
            tail = lines[-200:] if len(lines) > 200 else lines
            if not tail:
                text = "📄 No logs yet."
            else:
                txt = ''.join(tail)
                if len(txt) > 4000:
                    await callback.message.reply_document(document=LOG_FILE, caption="📄 Logs", reply_markup=get_main_menu())
                    await callback.answer("Logs sent as file.")
                    return
                else:
                    text = f"📄 *Logs*\n```\n{txt}```"
    except Exception as e:
        text = f"❌ Error: {e}"
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=get_main_menu(), parse_mode=ParseMode.MARKDOWN)

async def history_cb(client: Client, callback: CallbackQuery, page: int = 1):
    per_page = 10
    offset = (page - 1) * per_page
    rows = await get_all_uploaded(limit=per_page, offset=offset)
    total = await get_total_uploaded_count()
    total_pages = (total + per_page - 1) // per_page if total > 0 else 1
    if not rows:
        text = "📭 No uploaded videos yet."
        reply_markup = get_main_menu()
    else:
        text = f"📜 *Uploaded (Page {page}/{total_pages})*\n\n"
        for row in rows:
            text += f"• {row['title'][:50]} | {row['category']} | {row['duration']}\n"
        reply_markup = get_history_pagination(page, total_pages)
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

async def single_prompt_cb(client: Client, callback: CallbackQuery):
    state.waiting_for_single = True
    await callback.answer()
    await callback.message.edit_text(
        "🔗 Send the full URL of the video post.\nExample: `https://mastiraja.com/...`",
        reply_markup=get_single_cancel(),
        parse_mode=ParseMode.MARKDOWN
    )

async def handle_single_url(client: Client, message: Message):
    if not state.waiting_for_single:
        return
    state.waiting_for_single = False
    url = message.text.strip()
    if not url.startswith('http'):
        await message.reply("❌ Invalid URL.")
        return
    progress = await message.reply("⏳ Processing single video...")
    async with aiohttp.ClientSession() as session:
        info = await extract_video_info(session, url)
        if not info:
            await progress.edit_text("❌ Could not extract info.")
            return
        if await is_video_uploaded(info['post_id']):
            await progress.edit_text("ℹ️ Already uploaded.")
            return
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        filename = os.path.join(DOWNLOAD_DIR, f"{info['post_id']}_{os.path.basename(info['video_url'])}")
        filepath = await download_video(session, info['video_url'], filename)
        if not filepath:
            await progress.edit_text("❌ Download failed.")
            return
        upload_client = Client("single_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
        await upload_client.start()
        success = await upload_video(upload_client, filepath, info)
        await upload_client.stop()
        if success:
            await mark_uploaded(info['post_id'], filepath, info['title'], info['video_url'],
                                info['category'], info['tags'], info['duration'],
                                info['views'], info['description'])
            try:
                os.remove(filepath)
            except:
                pass
            await progress.edit_text(f"✅ Uploaded: {info['title']}")
        else:
            await progress.edit_text("❌ Upload failed.")
    await progress.reply("Back to menu:", reply_markup=get_main_menu())

async def help_cb(client: Client, callback: CallbackQuery):
    text = "🤖 Commands: start, pause, resume, stop, status, logs, history, single, ping. Use /start for menu."
    await callback.answer()
    await callback.message.edit_text(text, reply_markup=get_main_menu())

async def ping_cb(client: Client, callback: CallbackQuery):
    await callback.answer("🏓 Pong! Bot is alive.", show_alert=True)

async def menu_cb(client: Client, callback: CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("🤖 Main Menu", reply_markup=get_main_menu())

# ---------- HTTP Health Check Server ----------
async def http_server():
    from aiohttp import web
    app = web.Application()
    async def health(request):
        return web.Response(text="OK")
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host='0.0.0.0', port=PORT)
    await site.start()
    logger.info(f"HTTP health server running on port {PORT}")
    await asyncio.Event().wait()

# ---------- Main ----------
async def main():
    await init_db()
    asyncio.create_task(http_server())

    app = Client(
        "mastiraja_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        workers=20
    )

    @app.on_message()
    async def handle_msg(client, message):
        if not message.text:
            return
        if state.waiting_for_single:
            await handle_single_url(client, message)
            return
        if message.text.startswith('/'):
            cmd = message.text.split()[0].lower()
            if cmd == '/start':
                await start_cmd(client, message)
            elif cmd == '/ping':
                await ping_cmd(client, message)
            elif cmd == '/pause':
                if not state.running:
                    await message.reply("Not running.")
                elif state.paused:
                    await message.reply("Already paused.")
                else:
                    state.paused = True
                    state.status = "paused"
                    await message.reply("⏸ Paused.")
            elif cmd == '/resume':
                if not state.running:
                    await message.reply("Not running.")
                elif not state.paused:
                    await message.reply("Not paused.")
                else:
                    state.paused = False
                    state.status = "running"
                    await message.reply("▶️ Resumed.")
            elif cmd == '/stop':
                if not state.running:
                    await message.reply("Not running.")
                else:
                    state.running = False
                    state.paused = False
                    state.status = "stopped"
                    await message.reply("⏹ Stopped.")
            elif cmd == '/status':
                pending = await get_pending_count()
                uploaded = await get_total_uploaded_count()
                await message.reply(
                    f"📊 *Status*\n• State: `{state.status}`\n• Total: {state.total_posts}\n• Processed: {state.processed}\n• Pending DB: {pending}\n• Uploaded: {uploaded}",
                    parse_mode=ParseMode.MARKDOWN
                )
            elif cmd == '/logs':
                try:
                    with open(LOG_FILE, 'r') as f:
                        lines = f.readlines()
                        tail = lines[-200:] if len(lines) > 200 else lines
                        if tail:
                            txt = ''.join(tail)
                            if len(txt) > 4000:
                                await message.reply_document(document=LOG_FILE, caption="Logs")
                            else:
                                await message.reply(f"📄 *Logs*\n```\n{txt}```", parse_mode=ParseMode.MARKDOWN)
                        else:
                            await message.reply("No logs.")
                except:
                    await message.reply("Error reading logs.")
            elif cmd == '/history':
                rows = await get_all_uploaded(limit=10, offset=0)
                total = await get_total_uploaded_count()
                total_pages = (total + 9) // 10 if total > 0 else 1
                if rows:
                    text = f"📜 *History (1/{total_pages})*\n\n"
                    for r in rows:
                        text += f"• {r['title'][:50]} | {r['category']}\n"
                    await message.reply(text, reply_markup=get_history_pagination(1, total_pages), parse_mode=ParseMode.MARKDOWN)
                else:
                    await message.reply("No uploaded videos.")
            elif cmd == '/single':
                state.waiting_for_single = True
                await message.reply("Send the URL:", reply_markup=get_single_cancel())
            elif cmd == '/help':
                await message.reply("Commands: /start, /ping, /pause, /resume, /stop, /status, /logs, /history, /single, /help")
            else:
                await message.reply("Unknown. Use /help.")
        else:
            await message.reply("Use /start for menu.")

    @app.on_callback_query()
    async def handle_cb(client, callback):
        data = callback.data
        if data == "start":
            await start_crawler_cb(client, callback)
        elif data == "pause":
            await pause_crawler_cb(client, callback)
        elif data == "resume":
            await resume_crawler_cb(client, callback)
        elif data == "stop":
            await stop_crawler_cb(client, callback)
        elif data == "status":
            await status_cb(client, callback)
        elif data == "logs":
            await logs_cb(client, callback)
        elif data == "history":
            await history_cb(client, callback, page=1)
        elif data == "single":
            await single_prompt_cb(client, callback)
        elif data == "help":
            await help_cb(client, callback)
        elif data == "ping":
            await ping_cb(client, callback)
        elif data == "menu":
            await menu_cb(client, callback)
        elif data.startswith("history_"):
            parts = data.split("_")
            if len(parts) == 2 and parts[1].isdigit():
                await history_cb(client, callback, page=int(parts[1]))
            else:
                await callback.answer("Invalid page.")
        elif data == "history_nop":
            await callback.answer("Current page.")
        else:
            await callback.answer("Unknown action.")

    try:
        logger.info("Starting bot...")
        await app.start()
        logger.info("Bot started successfully.")
        await asyncio.Event().wait()
    except Unauthorized:
        logger.error("BOT_TOKEN is invalid! Please check your token.")
    except InvalidApiKey:
        logger.error("API_ID or API_HASH is invalid!")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    asyncio.run(main())
