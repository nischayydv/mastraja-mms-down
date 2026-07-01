#!/usr/bin/env python3
"""
MastiRaja Crawler – Telegram Bot Manager (Pyroblack)
Author: Potato
"""

import asyncio
import os
import sys
import logging

# --- FIX: Create event loop early to avoid RuntimeError in Python 3.14 ---
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# Now import everything else
import aiohttp
import aiofiles
import aiosqlite
import re
import base64
from datetime import datetime
from urllib.parse import urljoin, urlparse
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from pyrogram import Client
from pyrogram.errors import RPCError

# ---------- Load .env ----------
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
        self.task = None
        self.posts_queue = []
        self.total_posts = 0
        self.processed = 0
        self.status = "idle"
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()

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

async def is_video_uploaded(post_id):
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

async def get_all_uploaded(limit=20):
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT post_id, title, category, duration, upload_date FROM videos WHERE uploaded=1 ORDER BY upload_date DESC LIMIT ?", (limit,)) as cur:
            rows = await cur.fetchall()
            return [{"post_id": r[0], "title": r[1], "category": r[2], "duration": r[3], "upload_date": r[4]} for r in rows]

async def get_pending_count():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=0") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

# ---------- Core Functions ----------
async def fetch_soup(session, url):
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

def extract_post_links(soup):
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

def extract_pagination_links(soup):
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

def get_highest_quality_source(decoded_html):
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

async def extract_video_info(session, post_url):
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

async def download_video(session, video_url, filepath):
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

async def upload_video(client, filepath, info):
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
            parse_mode="markdown",
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
        state.stop_event.clear()
        logger.info("Crawler started.")

    async with aiohttp.ClientSession() as session:
        # Collect all posts
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
        logger.info("pyroblack upload client started.")

        sem = asyncio.Semaphore(MAX_DOWNLOADS)
        async def limited_process(post_url):
            nonlocal state
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
                    try: os.remove(filepath)
                    except: pass
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

# ---------- Bot Handlers ----------
async def start_cmd(client, message):
    if state.running:
        await message.reply("⚠️ Already running.")
        return
    state.task = asyncio.create_task(crawl_and_process())
    await message.reply("✅ Crawler started.")

async def pause_cmd(client, message):
    if not state.running:
        await message.reply("⚠️ Not running.")
        return
    if state.paused:
        await message.reply("⚠️ Already paused.")
        return
    state.paused = True
    state.status = "paused"
    await message.reply("⏸ Paused.")

async def resume_cmd(client, message):
    if not state.running:
        await message.reply("⚠️ Not running.")
        return
    if not state.paused:
        await message.reply("⚠️ Not paused.")
        return
    state.paused = False
    state.status = "running"
    await message.reply("▶️ Resumed.")

async def stop_cmd(client, message):
    if not state.running:
        await message.reply("⚠️ Not running.")
        return
    state.running = False
    state.paused = False
    state.status = "stopped"
    if state.task and not state.task.done():
        state.task.cancel()
        try: await state.task
        except: pass
    await message.reply("⏹ Stopped.")

async def status_cmd(client, message):
    text = (
        f"📊 *Status*\n"
        f"• State: `{state.status}`\n"
        f"• Running: {state.running}\n"
        f"• Paused: {state.paused}\n"
        f"• Total: {state.total_posts}\n"
        f"• Processed: {state.processed}\n"
        f"• Pending DB: {await get_pending_count()}"
    )
    await message.reply(text, parse_mode="markdown")

async def single_cmd(client, message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("Usage: `/single <url>`")
        return
    url = args[1].strip()
    await message.reply("⏳ Processing single video...")
    async with aiohttp.ClientSession() as session:
        info = await extract_video_info(session, url)
        if not info:
            await message.reply("❌ Could not extract info.")
            return
        if await is_video_uploaded(info['post_id']):
            await message.reply("ℹ️ Already uploaded.")
            return
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        filename = os.path.join(DOWNLOAD_DIR, f"{info['post_id']}_{os.path.basename(info['video_url'])}")
        filepath = await download_video(session, info['video_url'], filename)
        if not filepath:
            await message.reply("❌ Download failed.")
            return
        upload_client = Client("single_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
        await upload_client.start()
        success = await upload_video(upload_client, filepath, info)
        await upload_client.stop()
        if success:
            await mark_uploaded(info['post_id'], filepath, info['title'], info['video_url'],
                                info['category'], info['tags'], info['duration'],
                                info['views'], info['description'])
            try: os.remove(filepath)
            except: pass
            await message.reply(f"✅ Uploaded: {info['title']}")
        else:
            await message.reply("❌ Upload failed.")

async def logs_cmd(client, message):
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
            tail = lines[-200:] if len(lines) > 200 else lines
            if not tail:
                await message.reply("📄 No logs.")
                return
            txt = ''.join(tail)
            if len(txt) > 4000:
                await message.reply_document(document=LOG_FILE, caption="📄 Last 200 lines")
            else:
                await message.reply(f"📄 *Logs*\n```\n{txt}```", parse_mode="markdown")
    except Exception as e:
        await message.reply(f"❌ Error: {e}")

async def history_cmd(client, message):
    rows = await get_all_uploaded(20)
    if not rows:
        await message.reply("📭 No uploaded videos.")
        return
    txt = "📜 *Last 20 uploaded*\n\n"
    for row in rows:
        txt += f"• {row['title'][:50]} | {row['category']}\n"
    await message.reply(txt, parse_mode="markdown")

async def help_cmd(client, message):
    txt = (
        "🤖 *MastiRaja Crawler Bot*\n"
        "/start – Start crawler\n"
        "/pause – Pause\n"
        "/resume – Resume\n"
        "/stop – Stop\n"
        "/status – Show status\n"
        "/single <url> – Download single\n"
        "/logs – Show logs\n"
        "/history – Show history\n"
        "/help – This message"
    )
    await message.reply(txt, parse_mode="markdown")

# ---------- Main ----------
async def main():
    await init_db()
    app = Client("mastiraja_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True, workers=20)

    @app.on_message()
    async def handle(client, message):
        if not message.text or not message.text.startswith('/'):
            return
        cmd = message.text.split()[0].lower()
        if cmd == '/start': await start_cmd(client, message)
        elif cmd == '/pause': await pause_cmd(client, message)
        elif cmd == '/resume': await resume_cmd(client, message)
        elif cmd == '/stop': await stop_cmd(client, message)
        elif cmd == '/status': await status_cmd(client, message)
        elif cmd == '/single': await single_cmd(client, message)
        elif cmd == '/logs': await logs_cmd(client, message)
        elif cmd == '/history': await history_cmd(client, message)
        elif cmd == '/help': await help_cmd(client, message)
        else: await message.reply("❌ Unknown. Use /help")

    logger.info("Bot started.")
    await app.start()
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())
