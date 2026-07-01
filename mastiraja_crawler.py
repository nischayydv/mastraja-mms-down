#!/usr/bin/env python3
"""
MastiRaja Crawler – Telegram Bot Manager (Slug-Based Live Channel Verification - No DB)
Controls: /starttask, /pause, /resume, /stop, /status, /logs, /single, /ping, /help
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

# ---------- Event Loop Setup ----------
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

import aiohttp
import aiofiles
import yt_dlp
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pyrogram import Client
from pyrogram.types import Message
from pyrogram.errors import RPCError, Unauthorized, BadRequest, FloodWait
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
    pass

load_dotenv()

# ========= CONFIGURATION ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
LOG_FILE = os.getenv("LOG_FILE", "crawler.log")
PORT = int(os.getenv("PORT", 8080))  
AUTO_START = os.getenv("AUTO_START", "true").lower() == "true"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

MAX_PAGES_TO_SCAN = 15  
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# ---------- Channel ID Casting ----------
raw_channel_id = os.getenv("CHANNEL_ID", "").strip()
if raw_channel_id.startswith("-100") or raw_channel_id.isdigit() or (raw_channel_id.startswith("-") and raw_channel_id[1:].isdigit()):
    CHANNEL_ID = int(raw_channel_id)
else:
    CHANNEL_ID = raw_channel_id

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required environment variables (API_ID, API_HASH, BOT_TOKEN, or CHANNEL_ID)")

# ---------- Logging Setup ----------
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

# ---------- Global Engine State ----------
class CrawlerState:
    def __init__(self):
        self.running = False
        self.paused = False
        self.task: Optional[asyncio.Task] = None
        self.http_task: Optional[asyncio.Task] = None
        self.total_posts = 0
        self.processed = 0
        self.status = "idle"
        self.lock = asyncio.Lock()
        
        self.status_msg: Optional[Message] = None
        self.current_stage = "Idle"       
        self.current_title = "None"
        self.download_pct = 0.0
        self.upload_pct = 0.0
        self.current_page = 1

state = CrawlerState()

def clean_for_tg(text: str) -> str:
    if not text: return ""
    return text.replace("*", "").replace("_", "").replace("`", "").replace("<", "[").replace(">", "]")

def extract_slug_id(url: str) -> str:
    """Extracts a unique alphanumeric slug string from text-based URLs to act as post_id"""
    try:
        parsed = urlparse(url)
        slug = parsed.path.strip('/')
        if '/' in slug:
            slug = slug.split('/')[-1]
        return slug if slug else "unknown_target"
    except Exception:
        return "unknown_target"

# ---------- UI Progress Monitors ----------
def make_progress_bar(percentage: float, length: int = 10) -> str:
    percentage = max(0.0, min(100.0, percentage))
    filled = int(length * percentage / 100)
    return "█" * filled + "░" * (length - filled)

def build_live_status_text() -> str:
    total = state.total_posts
    proc = state.processed
    overall_pct = (proc / total * 100) if total > 0 else 0.0
    overall_bar = make_progress_bar(overall_pct, length=12)
    cleaned_title = clean_for_tg(state.current_title)
    
    text = (
        f"🔄 **MastiRaja Live Automation Monitor**\n"
        f"• System State: `{state.status.upper()}`\n"
        f"• Current Scan Page: `{state.current_page}`\n"
        f"─────────────────────\n"
    )
    
    if state.current_stage == "Scraping":
        text += f"🛰️ **Phase:** Reading Page {state.current_page} links from site index..."
    elif state.current_stage in ["Checking Channel", "Extracting Info", "Downloading", "Uploading"]:
        text += (
            f"📦 **Phase: Processing Media Pipeline**\n"
            f"• Page Queue: `{proc} / {total}` links handled\n"
            f"• Batch Ratio: `[{overall_bar}] {overall_pct:.1f}%`\n\n"
            f"🎬 **Active Task Target:**\n"
            f"• Title: `{cleaned_title[:55]}`\n"
            f"• Sub-Task: `{state.current_stage}`\n"
        )
        if state.current_stage == "Downloading":
            dl_bar = make_progress_bar(state.download_pct, length=10)
            text += f"• **Network Input (yt-dlp):** `[{dl_bar}] {state.download_pct:.1f}%`\n"
        elif state.current_stage == "Uploading":
            ul_bar = make_progress_bar(state.upload_pct, length=10)
            text += f"• **Data Output (Telegram UI):** `[{ul_bar}] {state.upload_pct:.1f}%`\n"
    elif state.current_stage == "Finished":
        text += f"✅ **Run Finished!** All pages synced perfectly up to page `{state.current_page}`."
    else:
        text += "💤 Engine standing by on sleep interval."
    return text

async def live_ui_refresh_loop(client: Client):
    last_text = ""
    while state.running:
        current_text = build_live_status_text()
        if current_text != last_text:
            if state.status_msg:
                try:
                    await state.status_msg.edit_text(current_text, parse_mode=ParseMode.MARKDOWN)
                    last_text = current_text
                except FloodWait as e:
                    await asyncio.sleep(e.value + 1)
                except Exception:
                    pass
        await asyncio.sleep(3.5)

# ---------- Pure Channel Verifier (Slug Mode) ----------
async def is_video_uploaded(client: Client, post_id: str) -> bool:
    """Performs a live look up inside the Telegram Channel directly using the text slug ID string."""
    try:
        search_query = f"🆔 ID: {post_id}"
        async for msg in client.search_messages(chat_id=CHANNEL_ID, query=search_query, limit=1):
            if msg:
                logger.info(f"🎯 Target Slug [{post_id}] found inside channel messages. Skipping download.")
                return True
    except Exception as e:
        logger.error(f"Failed to execute Live Channel Search for Slug ID {post_id}: {e}")
    return False

# ---------- Extraction Architecture ----------
async def fetch_soup(session: aiohttp.ClientSession, url: str):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                html = await resp.text()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.warning(f"Fetch failed on {url} (Attempt {attempt+1}): {e}")
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
        if not src: continue
        quality = None
        for attr in ['quality', 'data-quality', 'bitrate', 'res']:
            val = src_tag.get(attr)
            if val:
                try:
                    num = re.search(r'(\d+)', str(val))
                    if num:
                        quality = int(num.group(1))
                        break
                except: pass
        if quality is None:
            m = re.search(r'(\d+)p', src)
            if m: quality = int(m.group(1))
            else: quality = 0
        if quality > best_quality:
            best_quality = quality
            best = src
    return best if best else (sources[-1].get('src') if sources else None)

async def extract_video_info(session: aiohttp.ClientSession, post_url: str) -> Optional[Dict]:
    soup = await fetch_soup(session, post_url)
    if not soup: return None
    
    post_id = extract_slug_id(post_url)
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
                if source: video_url = source.get('src')
    if not video_url: return None
    cat_tag = soup.find('a', class_='label', title=True)
    category = cat_tag.get_text(strip=True) if cat_tag else "Uncategorized"
    tags = [t.get_text(strip=True) for t in soup.find_all('a', class_='label') if 'fa-tag' in str(t) or '/tag/' in t.get('href', '')]
    tags_str = ', '.join(tags)
    dur = soup.find('span', class_='duration')
    duration = dur.get_text(strip=True) if dur else ''
    views_span = soup.find('span', class_='views')
    views = views_span.get_text(strip=True).replace('i', '').strip() if views_span else ''
    desc_div = soup.find('div', class_='video-description')
    desc = desc_div.find('p').get_text(strip=True) if (desc_div and desc_div.find('p')) else ''
    
    return {
        'post_id': post_id, 'title': title, 'video_url': video_url, 'category': category,
        'tags': tags_str, 'duration': duration, 'views': views, 'description': desc,
    }

# ---------- Core Downloader ----------
async def download_video(post_id: str, video_url: str, download_dir: str) -> Optional[str]:
    os.makedirs(download_dir, exist_ok=True)
    # Sanitize slug safely for system filenames
    safe_post_id = re.sub(r'[^a-zA-Z0-9_-]', '', post_id)[:50]
    outtmpl = os.path.join(download_dir, f"{safe_post_id}_%(title)s.%(ext)s")
    downloaded_file = None

    def progress_hook(d):
        nonlocal downloaded_file
        if d['status'] == 'finished':
            downloaded_file = d['filename']
            state.download_pct = 100.0
        elif d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)
            if total > 0:
                state.download_pct = (downloaded / total) * 100

    ydl_opts = {
        'outtmpl': outtmpl, 'quiet': True, 'no_warnings': True,
        'progress_hooks': [progress_hook],
        'http_headers': {'User-Agent': USER_AGENT, 'Referer': BASE_URL}
    }

    try:
        await asyncio.to_thread(lambda: yt_dlp.YoutubeDL(ydl_opts).download([video_url]))
        if downloaded_file and os.path.exists(downloaded_file): return downloaded_file
        for file in os.listdir(download_dir):
            if file.startswith(f"{safe_post_id}_"): return os.path.join(download_dir, file)
    except Exception as e:
        logger.error(f"yt-dlp Core Error: {e}")
    return None

async def upload_video(client: Client, filepath: str, info: Dict) -> bool:
    clean_title = clean_for_tg(info['title'])
    clean_cat = clean_for_tg(info['category'])
    clean_tags = clean_for_tg(info['tags'])
    clean_dur = clean_for_tg(info['duration'])
    clean_desc = clean_for_tg(info['description'])

    # Anchor Tracking Line using the full Slug String
    caption = f"📹 Title: {clean_title}\n"
    caption += f"🆔 ID: {info['post_id']}\n"
    if clean_cat: caption += f"📂 Category: {clean_cat}\n"
    if clean_tags: caption += f"🏷️ Tags: {clean_tags}\n"
    if clean_dur: caption += f"⏱️ Duration: {clean_dur}\n"
    if clean_desc: caption += f"📝 {clean_desc[:200]}...\n"
    caption += f"\nUploaded by @NY_BOTS"
    
    try:
        def upload_progress(current, total):
            if total > 0: state.upload_pct = (current / total) * 100

        await client.send_video(
            chat_id=CHANNEL_ID, video=filepath, caption=caption,
            parse_mode=ParseMode.MARKDOWN, supports_streaming=True, progress=upload_progress
        )
        return True
    except RPCError as e:
        logger.error(f"Telegram Upload Error: {e}")
        return False

# ---------- Dynamic Verification Pipeline Engine ----------
async def crawl_and_process(bot_client: Client):
    async with state.lock:
        if state.running: return
        state.running = True
        state.paused = False
        state.status = "running"
    
    ui_task = asyncio.create_task(live_ui_refresh_loop(bot_client))
    
    async with aiohttp.ClientSession() as session:
        state.current_page = 1
        
        while state.current_page <= MAX_PAGES_TO_SCAN and state.running:
            while state.paused and state.running: await asyncio.sleep(1)
            if not state.running: break
                
            url = BASE_URL if state.current_page == 1 else f"{BASE_URL}/page/{state.current_page}/"
            logger.info(f"Scanning target index feed page {state.current_page}...")
            state.current_stage = "Scraping"
            
            soup = await fetch_soup(session, url)
            if not soup: break
                
            links = extract_post_links(soup)
            if not links: break
                
            state.total_posts = len(links)
            state.processed = 0
            page_had_new_video = False
            
            # Pure One-by-One Pipeline Layout
            for target_url in links:
                while state.paused and state.running: await asyncio.sleep(1)
                if not state.running: break
                    
                slug_id = extract_slug_id(target_url)
                if slug_id:
                    state.current_stage = "Checking Channel"
                    state.current_title = f"Checking target ID: {slug_id}"
                    
                    # Search using text slug tracking key layout
                    if await is_video_uploaded(bot_client, slug_id):
                        logger.info(f"[Page {state.current_page}] Slug [{slug_id}] already exists on channel. Skipping.")
                        state.processed += 1
                        continue 
                
                page_had_new_video = True
                state.current_stage = "Extracting Info"
                state.current_title = "Fetching remote metadata..."
                
                info = await extract_video_info(session, target_url)
                if not info or not info['post_id']:
                    state.processed += 1
                    continue
                    
                state.current_title = info['title']
                state.download_pct = 0.0
                state.upload_pct = 0.0
                
                # Step 1: Download first
                state.current_stage = "Downloading"
                logger.info(f"Downloading Target Slug [{info['post_id']}]: {info['title']}")
                filepath = await download_video(info['post_id'], info['video_url'], DOWNLOAD_DIR)
                if not filepath:
                    state.processed += 1
                    continue
                    
                # Step 2: Upload before proceeding
                state.current_stage = "Uploading"
                logger.info(f"Uploading Target directly to channel: {info['title']}")
                success = await upload_video(bot_client, filepath, info)
                if success:
                    try: os.remove(filepath)
                    except Exception: pass
                
                state.processed += 1
                await asyncio.sleep(2) 
            
            if not page_had_new_video:
                logger.info(f"Page {state.current_page} matches live channel logs perfectly. Channel caught up.")
                break
                
            state.current_page += 1
            
    state.current_stage = "Finished"
    state.running = False
    state.status = "stopped"
    await asyncio.sleep(1)
    ui_task.cancel()

# ---------- Command Routines ----------
async def start_task_cmd(client: Client, message: Message):
    if state.running:
        await message.reply("⚠️ Crawler is already running.")
        return
    state.status_msg = await message.reply("🚀 **Initializing Slug-Based Live Tracker Layout...**")
    state.task = asyncio.create_task(crawl_and_process(client))

async def pause_cmd(client: Client, message: Message):
    if not state.running or state.paused: return
    state.paused = True
    state.status = "paused"
    await message.reply("⏸ Crawler engine paused.")

async def resume_cmd(client: Client, message: Message):
    if not state.running or not state.paused: return
    state.paused = False
    state.status = "running"
    await message.reply("▶️ Crawler engine resumed.")

async def stop_cmd(client: Client, message: Message):
    if not state.running: return
    state.running = False
    state.paused = False
    state.status = "stopped"
    state.current_stage = "Idle"
    if state.task and not state.task.done(): state.task.cancel()
    await message.reply("⏹ Crawler stopped.")

async def status_cmd(client: Client, message: Message):
    await message.reply(
        f"📊 **Engine Status (Slug Mode)**\n"
        f"• State: `{state.status.upper()}`\n"
        f"• Current Page: `{state.current_page}`\n"
        f"• Processing Page Feed: `{state.processed}/{state.total_posts}`"
    )

async def logs_cmd(client: Client, message: Message):
    try:
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            lines = f.readlines()[-200:]
            if not lines:
                await message.reply("📄 System log history is currently empty.")
                return
            txt = ''.join(lines)
            if len(txt) > 4000:
                await message.reply_document(document=LOG_FILE, caption="📄 System Logs")
            else:
                await message.reply(f"📄 Logs:\n```\n{txt}```")
    except Exception as e:
        await message.reply(f"❌ Error compiling logs: {e}")

async def single_cmd(client: Client, message: Message):
    args = message.text.split(maxsplit=1)
    if len(args) < 2:
        await message.reply("❌ Usage: /single URL")
        return
    url = args[1].strip()
    progress_msg = await message.reply("⏳ Initializing single verification task...")
    async with aiohttp.ClientSession() as session:
        info = await extract_video_info(session, url)
        if not info:
            await progress_msg.edit_text("❌ Action cancelled. Invalid video structure.")
            return
        if await is_video_uploaded(client, info['post_id']):
            await progress_msg.edit_text("❌ Action cancelled. Slug already matches content on channel.")
            return
        filepath = await download_video(info['post_id'], info['video_url'], DOWNLOAD_DIR)
        if filepath and await upload_video(client, filepath, info):
            try: os.remove(filepath)
            except Exception: pass
            await progress_msg.edit_text(f"✅ Uploaded: {info['title']}")
        else:
            await progress_msg.edit_text("❌ Single processing pipeline failed.")

async def ping_cmd(client: Client, message: Message):
    await message.reply("🏓 Pong! Core engine runtime is online.")

async def help_cmd(client: Client, message: Message):
    text = (
        "🤖 MastiRaja Smart Sync Controller (Slug Track Mode)\n\n"
        "/starttask - Launch live-tracker interface\n"
        "/pause - Freeze transfers\n"
        "/resume - Unfreeze transfers\n"
        "/stop - Terminate tracking operations\n"
        "/status - View current live tracking progress\n"
        "/logs - Check system console outputs\n"
        "/single URL - Process isolated custom asset URL\n"
        "/ping - Verify active server response"
    )
    await message.reply(text)

# ---------- Network Port Interface ----------
async def http_server():
    from aiohttp import web
    app = web.Application()
    async def health(request): return web.Response(text="OK")
    app.router.add_get('/', health)
    app.router.add_get('/health', health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, host='0.0.0.0', port=PORT).start()
    logger.info(f"HTTP Server health check working fine on port {PORT}")
    await asyncio.Event().wait()

# ---------- Program Main Loop ----------
async def main():
    state.http_task = asyncio.create_task(http_server())

    app = Client(
        "mastiraja_bot",
        api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN,
        in_memory=True, workers=20
    )

    @app.on_message()
    async def handle_msg(client, message):
        if not message.text: return
        cmd = message.text.split()[0].lower()
        if cmd == '/starttask': await start_task_cmd(client, message)
        elif cmd == '/pause': await pause_cmd(client, message)
        elif cmd == '/resume': await resume_cmd(client, message)
        elif cmd == '/stop': await stop_cmd(client, message)
        elif cmd == '/status': await status_cmd(client, message)
        elif cmd == '/logs': await logs_cmd(client, message)
        elif cmd.startswith('/single'): await single_cmd(client, message)
        elif cmd == '/ping': await ping_cmd(client, message)
        elif cmd == '/help': await help_cmd(client, message)

    try:
        logger.info("Starting bot...")
        await app.start()
        logger.info("Bot started successfully in Pure Channel Tracking Slug mode.")

        if AUTO_START:
            logger.info("Auto‑start flag is true. Invoking processing daemon...")
            state.task = asyncio.create_task(crawl_and_process(app))

        await asyncio.Event().wait()
    except Exception as e:
        logger.error(f"Execution Error during runtime init: {e}")

if __name__ == "__main__":
    asyncio.run(main())
