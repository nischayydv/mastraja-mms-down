#!/usr/bin/env python3
"""
MastiRaja Crawler – Telegram Bot Manager (Command‑Only)
Controls: /starttask, /pause, /resume, /stop, /status, /logs, /history, /single, /ping, /help
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

# ---------- FIX 1: Event loop ----------
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# ---------- FIX 2: Patch Identifier ----------
import aiohttp
import aiofiles
import aiosqlite
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import RPCError, Unauthorized, BadRequest
from pyrogram.enums import ParseMode

try:
    from pyrogram.types.pyromod import Identifier
    if not hasattr(Identifier, '__annotations__'):
        setattr(Identifier, '__annotations__', {})
    Identifier.__annotations__ = {}

    _original_matches = Identifier.matches
    def _patched_matches(self, data):
        try:
            return _original_matches(self, data)
        except AttributeError:
            return False
    Identifier.matches = _patched_matches
except ImportError:
    pass  # Fallback if pyromod is not installed/present

load_dotenv()

# ========= CONFIG ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")
DB_FILE = os.getenv("DB_FILE", "/tmp/videos.db")
LOG_FILE = os.getenv("LOG_FILE", "/tmp/crawler.log")
PORT = int(os.getenv("PORT", 8000))
AUTO_START = os.getenv("AUTO_START", "true").lower() == "true"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

MAX_PAGE_FETCH = 5
MAX_VIDEO_EXTRACT = 10
MAX_DOWNLOADS = 5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# ---------- FIX 3: Dynamic Channel ID Type Casting ----------
raw_channel_id = os.getenv("CHANNEL_ID", "").strip()
if raw_channel_id.startswith("-100") or raw_channel_id.isdigit() or (raw_channel_id.startswith("-") and raw_channel_id[1:].isdigit()):
    CHANNEL_ID = int(raw_channel_id)
else:
    CHANNEL_ID = raw_channel_id  # Keep as string if it's a username like @MyChannel

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required environment variables (API_ID, API_HASH, BOT_TOKEN, or CHANNEL_ID)")

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
                    async with resp.content.iter_chunked(65536) as chunks:
                        async for chunk in chunks:
                            await f.write(chunk)
                            downloaded += len(chunk)
                            if total:
                                sys.stdout.write(f"\r[DOWNLOADING] {os.path.basename(filepath)}: {downloaded/total*100:.1f}%")
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
        def upload_progress(current, total):
            if total > 0:
                sys.stdout.write(f"\r[UPLOADING] {os.path.basename(filepath)}: {current/total*100:.1f}%")
                sys.stdout.flush()

        await client.send_video(
            chat_id=CHANNEL_ID,
            video=filepath,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            supports_streaming=True,
            progress=upload_progress
        )
        sys.stdout.write("\n")
        return True
    except RPCError as e:
        sys.stdout.write("\n")
        logger.error(f"Upload error: {e}")
        return False

# ---------- Crawler Task ----------
async def crawl_and_process():
    async with state.lock:
        if state.running:
            logger.info("Crawler already running.")
            return
        state.running = True
        state.paused = False
        state.status = "running"
        state.processed = 0
        logger.info("Crawler process initiated.")

    async with aiohttp.ClientSession() as session:
        to_visit = [BASE_URL]
        visited = set()
        all_posts = set()
        
        logger.info("Collecting system post URLs...")
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
            
            extracted = extract_post_links(soup)
            all_posts.update(extracted)
            
            # Print changing real-time collection telemetry
            sys.stdout.write(f"\r[SCRAPER] Collected Links: {len(all_posts)} | Visited: {len(visited)}")
            sys.stdout.flush()

            for p in extract_pagination_links(soup):
                if p not in visited:
                    to_visit.append(p)
            await asyncio.sleep(0.3)
        
        sys.stdout.write("\n")
        
        if not state.running:
            state.status = "stopped"
            state.running = False
            logger.info("Crawler stopped during initialization collection phase.")
            return
            
        state.total_posts = len(all_posts)
        logger.info(f"Target Queue Loaded. Total unique links found: {state.total_posts}")

        client = Client("crawler_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, workers=10)
        await client.start()

        sem = asyncio.Semaphore(MAX_DOWNLOADS)

        async def limited_process(post_url, index):
            async with sem:
                while state.paused and state.running:
                    await asyncio.sleep(1)
                if not state.running:
                    return
                
                logger.info(f"[Task {index}/{state.total_posts}] Parsing payload info...")
                info = await extract_video_info(session, post_url)
                if not info or not info['post_id']:
                    return
                if await is_video_uploaded(info['post_id']):
                    logger.info(f"Skipping Post ID {info['post_id']} - already indexed in database.")
                    state.processed += 1
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
                    logger.warning(f"Upload failed for target {info['post_id']}. Media preserved.")
                state.processed += 1

        tasks = []
        for idx, url in enumerate(all_posts, start=1):
            if not state.running:
                break
            tasks.append(asyncio.create_task(limited_process(url, idx)))
            while state.paused and state.running:
                await asyncio.sleep(1)
        
        if tasks:
            await asyncio.gather(*tasks)
            
        await client.stop()
        logger.info("Crawler run successfully finished.")
        state.running = False
        state.status = "stopped"

# ---------- Command Handlers ----------
async def start_task_cmd(client: Client, message: Message):
    if state.running:
        await message.reply("⚠️ Crawler is already running.")
        return
    state.task = asyncio.create_task(crawl_and_process())
    await message.reply("✅ Crawler started. Use /status to monitor.")

async def pause_cmd(client: Client, message: Message):
    if not state.running:
        await message.reply("⚠️ Crawler is not running.")
        return
    if state.paused:
        await message.reply("⚠️ Already paused.")
        return
    state.paused = True
    state.status = "paused"
    await message.reply("⏸ Crawler paused. Use /resume to continue.")

async def resume_cmd(client: Client, message: Message):
    if not state.running:
        await message.reply("⚠️ Crawler is not running.")
        return
    if not state.paused:
        await message.reply("⚠️ Not paused.")
        return
    state.paused = False
    state.status = "running"
    await message.reply("▶️ Crawler resumed.")

async def stop_cmd(client: Client, message: Message):
    if not state.running:
        await message.reply("⚠️ Crawler is not running.")
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
    await message.reply("⏹ Crawler stopped.")

async def status_cmd(client: Client, message: Message):
    pending = await get_pending_count()
    uploaded = await get_total_uploaded_count()
    text = (
        f"📊 *Crawler Status*\n"
        f"• State: `{state.status}`\n"
        f"• Running: {state.running}\n"
        f"• Paused: {state.paused}\n"
        f"• Total Posts Found: {state.total_posts}\n"
        f"• Processed: {state.processed}\n"
        f"• Pending in DB: {pending}\n"
        f"• Uploaded: {uploaded}"
    )
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

async def logs_cmd(client: Client, message: Message):
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()
            tail = lines[-200:] if len(lines) > 200 else lines
            if not tail:
                await message.reply("📄 No logs yet.")
                return
            txt = ''.join(tail)
            if len(txt) > 4000:
                await message.reply_document(document=LOG_FILE, caption="📄 Last 200 lines of logs")
            else:
                await message.reply(f"📄 *Logs (last 200 lines)*\n```\n{txt}```", parse_mode=ParseMode.MARKDOWN)
    except Exception as e:
        await message.reply(f"❌ Error reading logs: {e}")

async def history_cmd(client: Client, message: Message):
    rows = await get_all_uploaded(limit=20, offset=0)
    if not rows:
        await message.reply("📭 No uploaded videos yet.")
        return
    text = "📜 *Last 20 uploaded videos*\n\n"
    for row in rows:
        text += f"• {row['title'][:50]} | {row['category']} | {row['duration']}\n"
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

async def single_cmd(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Usage: `/single <post_url>`\nExample: `/single https://mastiraja.com/...`")
        return
    url = args[1].strip()
    progress_msg = await message.reply("⏳ Processing single video...")
    async with aiohttp.ClientSession() as session:
        info = await extract_video_info(session, url)
        if not info:
            await progress_msg.edit_text("❌ Could not extract video info.")
            return
        if await is_video_uploaded(info['post_id']):
            await progress_msg.edit_text(f"ℹ️ Video already uploaded: {info['title']}")
            return
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        filename = os.path.join(DOWNLOAD_DIR, f"{info['post_id']}_{os.path.basename(info['video_url'])}")
        filepath = await download_video(session, info['video_url'], filename)
        if not filepath:
            await progress_msg.edit_text("❌ Download failed.")
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
            await progress_msg.edit_text(f"✅ Uploaded: {info['title']}")
        else:
            await progress_msg.edit_text("❌ Upload failed.")

async def ping_cmd(client: Client, message: Message):
    await message.reply("🏓 Pong! Bot is alive and responsive.")

async def help_cmd(client: Client, message: Message):
    text = (
        "🤖 *MastiRaja Crawler Bot*\n\n"
        "Available commands:\n"
        "/starttask – Start the crawler\n"
        "/pause – Pause the crawler\n"
        "/resume – Resume the crawler\n"
        "/stop – Stop the crawler\n"
        "/status – Show current status\n"
        "/logs – Show last 200 log lines\n"
        "/history – Show last 20 uploaded videos\n"
        "/single <url> – Download & upload a single video\n"
        "/ping – Test bot responsiveness\n"
        "/help – Show this message"
    )
    await message.reply(text, parse_mode=ParseMode.MARKDOWN)

# ---------- HTTP Health Check ----------
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
        cmd = message.text.split()[0].lower()
        if cmd == '/starttask':
            await start_task_cmd(client, message)
        elif cmd == '/pause':
            await pause_cmd(client, message)
        elif cmd == '/resume':
            await resume_cmd(client, message)
        elif cmd == '/stop':
            await stop_cmd(client, message)
        elif cmd == '/status':
            await status_cmd(client, message)
        elif cmd == '/logs':
            await logs_cmd(client, message)
        elif cmd == '/history':
            await history_cmd(client, message)
        elif cmd.startswith('/single'):
            await single_cmd(client, message)
        elif cmd == '/ping':
            await ping_cmd(client, message)
        elif cmd == '/help':
            await help_cmd(client, message)
        else:
            await message.reply("❌ Unknown command. Use /help for available commands.")

    try:
        logger.info("Starting bot...")
        await app.start()
        logger.info("Bot started successfully.")

        # Validate channel
        try:
            chat = await app.get_chat(CHANNEL_ID)
            logger.info(f"Channel validated: {chat.title} (ID: {chat.id})")
        except Exception as e:
            logger.error(f"Failed to access channel: {e}")
            logger.error("Make sure CHANNEL_ID is correct and the bot is an admin.")

        # Auto‑start if enabled
        if AUTO_START:
            logger.info("Auto‑start enabled. Starting crawler now...")
            state.task = asyncio.create_task(crawl_and_process())
            await asyncio.sleep(1)

        await asyncio.Event().wait()
    except Unauthorized:
        logger.error("BOT_TOKEN is invalid! Please check your token.")
    except BadRequest as e:
        logger.error(f"BadRequest: {e}")
    except Exception as e:
        logger.error(f"Failed to start bot: {e}")

if __name__ == "__main__":
    asyncio.run(main())
