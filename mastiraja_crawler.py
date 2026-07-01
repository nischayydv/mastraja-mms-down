#!/usr/bin/env python3
"""
MastiRaja Crawler – Telegram Bot Manager (Pyrogram)
Author: Potato
Features: Inline keyboard control, full crawler management, single download, logs, history.
"""

import asyncio
import aiohttp
import aiofiles
import aiosqlite
import os
import re
import sys
import base64
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urljoin, urlparse

# ---------- FIX: Create event loop early to avoid RuntimeError in Python 3.14 ----------
try:
    loop = asyncio.get_running_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from dotenv import load_dotenv
from bs4 import BeautifulSoup

# ---------- Pyrogram imports ----------
from pyrogram import Client, filters
from pyrogram.types import (
    Message, InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Document, InputMediaVideo
)
from pyrogram.errors import RPCError, FloodWait
from pyrogram.enums import ParseMode

load_dotenv()

# ========= CONFIG (from environment) ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")
DB_FILE = os.getenv("DB_FILE", "/tmp/videos.db")
LOG_FILE = os.getenv("LOG_FILE", "/tmp/crawler.log")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Concurrency limits (adjustable)
MAX_PAGE_FETCH = 5
MAX_VIDEO_EXTRACT = 10
MAX_DOWNLOADS = 5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
# ================================================

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID")

# ---------- Logging ----------
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

# ---------- Global Crawler State ----------
class CrawlerState:
    """Holds the current state of the crawler."""
    def __init__(self):
        self.running = False
        self.paused = False
        self.task: Optional[asyncio.Task] = None
        self.posts_queue: List[str] = []
        self.total_posts = 0
        self.processed = 0
        self.status = "idle"  # idle, running, paused, stopped
        self.lock = asyncio.Lock()
        self.stop_event = asyncio.Event()

state = CrawlerState()

# ---------- Database Helpers (async) ----------
async def init_db():
    """Initialize the SQLite database."""
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
    logger.info("Database initialized.")

async def is_video_uploaded(post_id: int) -> bool:
    """Check if a video has already been uploaded."""
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT 1 FROM videos WHERE post_id = ? AND uploaded = 1", (post_id,)) as cur:
            row = await cur.fetchone()
            return row is not None

async def mark_uploaded(
    post_id: int,
    file_path: str,
    title: str,
    video_url: str,
    category: str,
    tags: str,
    duration: str,
    views: str,
    description: str
):
    """Mark a video as uploaded and store its metadata."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute('''
            INSERT OR REPLACE INTO videos
            (post_id, title, video_url, file_path, category, tags, duration, views, description, uploaded, upload_date, last_checked)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, datetime('now'), datetime('now'))
        ''', (post_id, title, video_url, file_path, category, tags, duration, views, description))
        await db.commit()

async def update_last_checked(post_id: int):
    """Update the last_checked timestamp for a video."""
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE videos SET last_checked = datetime('now') WHERE post_id = ?", (post_id,))
        await db.commit()

async def get_all_uploaded(limit: int = 20, offset: int = 0) -> List[Dict]:
    """Retrieve uploaded videos with pagination."""
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute(
            "SELECT post_id, title, category, duration, upload_date FROM videos WHERE uploaded=1 ORDER BY upload_date DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ) as cur:
            rows = await cur.fetchall()
            return [
                {"post_id": r[0], "title": r[1], "category": r[2], "duration": r[3], "upload_date": r[4]}
                for r in rows
            ]

async def get_pending_count() -> int:
    """Get the number of videos that are not yet uploaded (stored in DB but not uploaded)."""
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=0") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

async def get_total_uploaded_count() -> int:
    """Get the total number of uploaded videos."""
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM videos WHERE uploaded=1") as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

# ---------- Core Scraping & Download Functions ----------
async def fetch_soup(session: aiohttp.ClientSession, url: str) -> Optional[BeautifulSoup]:
    """Fetch a URL and return a BeautifulSoup object, with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT) as resp:
                resp.raise_for_status()
                html = await resp.text()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.warning(f"Fetch {url} attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts.")
    return None

def extract_post_links(soup: BeautifulSoup) -> List[str]:
    """Extract all post URLs from a paginated page."""
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
    """Extract pagination links from the page."""
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
    """
    Parse the decoded video player HTML and return the source with the highest quality.
    Falls back to the first source if no quality attribute is found.
    """
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
        # Try various quality attributes
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
            # Try to infer from filename (e.g., 1080p)
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
    """
    Extract video metadata and the highest quality video URL from a post page.
    """
    soup = await fetch_soup(session, post_url)
    if not soup:
        return None

    # Extract post ID
    post_id = None
    m = re.search(r'/(\d+)/?$', post_url)
    if m:
        post_id = int(m.group(1))

    # Title
    title_tag = soup.find('h1', itemprop='name')
    title = title_tag.get_text(strip=True) if title_tag else "Untitled"

    # Video URL from iframe or video tag
    video_url = None
    iframe = soup.find('iframe', src=True)
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
        logger.warning(f"No video URL found for {post_url}")
        return None

    # Category
    cat_tag = soup.find('a', class_='label', title=True)
    category = cat_tag.get_text(strip=True) if cat_tag else "Uncategorized"

    # Tags
    tags = []
    for t in soup.find_all('a', class_='label'):
        if 'fa-tag' in str(t) or '/tag/' in t.get('href', ''):
            tags.append(t.get_text(strip=True))
    tags_str = ', '.join(tags)

    # Duration
    dur = soup.find('span', class_='duration')
    duration = dur.get_text(strip=True) if dur else ''

    # Views (we store it but may not display)
    views_span = soup.find('span', class_='views')
    views = views_span.get_text(strip=True).replace('i', '').strip() if views_span else ''

    # Description
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

async def download_video(
    session: aiohttp.ClientSession,
    video_url: str,
    filepath: str
) -> Optional[str]:
    """
    Download a video file with retries and progress display.
    Returns the filepath on success, None on failure.
    """
    if os.path.exists(filepath):
        logger.info(f"File already exists: {filepath}")
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
                            # Progress output to console (can be captured in logs)
                            sys.stdout.write(f"\rDownloading {os.path.basename(filepath)}: {downloaded/total*100:.1f}%")
                            sys.stdout.flush()
                sys.stdout.write("\n")
                logger.info(f"Downloaded {filepath} ({downloaded} bytes)")
                return filepath
        except Exception as e:
            logger.warning(f"Download attempt {attempt+1} failed: {e}")
            await asyncio.sleep(2 ** attempt)
    logger.error(f"Failed to download {video_url}")
    return None

async def upload_video(
    client: Client,
    filepath: str,
    info: Dict
) -> bool:
    """
    Upload a video to the Telegram channel with a formatted caption.
    Returns True on success, False otherwise.
    """
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
        # Using send_video with progress callback
        await client.send_video(
            chat_id=CHANNEL_ID,
            video=filepath,
            caption=caption,
            parse_mode=ParseMode.MARKDOWN,
            supports_streaming=True,
            progress=lambda current, total: sys.stdout.write(
                f"\rUploading {os.path.basename(filepath)}: {current/total*100:.1f}%"
            ) or sys.stdout.flush()
        )
        sys.stdout.write("\n")
        logger.info(f"Uploaded {filepath}")
        return True
    except RPCError as e:
        logger.error(f"Upload error for {filepath}: {e}")
        return False

# ---------- The Main Crawler Task ----------
async def crawl_and_process():
    """
    The main crawler coroutine. It collects all post URLs, then processes each
    one (download + upload) with concurrency control.
    """
    async with state.lock:
        if state.running:
            logger.warning("Crawler already running")
            return
        state.running = True
        state.paused = False
        state.status = "running"
        state.stop_event.clear()
        logger.info("Crawler started.")

    async with aiohttp.ClientSession() as session:
        # Step 1: Collect all post URLs from all pagination pages
        logger.info("Collecting all video URLs...")
        to_visit = [BASE_URL]
        visited = set()
        all_posts = set()

        while to_visit and state.running:
            if state.paused:
                # Sleep while paused, but check stop_event periodically
                await asyncio.sleep(2)
                continue

            url = to_visit.pop()
            if url in visited:
                continue
            visited.add(url)

            soup = await fetch_soup(session, url)
            if not soup:
                continue

            # Extract posts from current page
            posts = extract_post_links(soup)
            all_posts.update(posts)

            # Add pagination links
            for p in extract_pagination_links(soup):
                if p not in visited:
                    to_visit.append(p)

            # Small delay to avoid overloading the server
            await asyncio.sleep(0.3)

        # If crawler was stopped during collection, exit gracefully
        if not state.running:
            logger.info("Crawler stopped during collection.")
            state.status = "stopped"
            state.running = False
            return

        state.total_posts = len(all_posts)
        state.posts_queue = list(all_posts)
        logger.info(f"Found {len(all_posts)} posts.")

        # Step 2: Start pyrogram client for uploads
        client = Client(
            "crawler_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True,
            workers=10
        )
        await client.start()
        logger.info("Pyrogram upload client started.")

        # Step 3: Process posts with concurrency control
        sem = asyncio.Semaphore(MAX_DOWNLOADS)
        processed = 0

        async def limited_process(post_url: str):
            nonlocal processed
            async with sem:
                # Check pause/stop status before processing
                while state.paused and state.running:
                    await asyncio.sleep(1)
                if not state.running:
                    return

                info = await extract_video_info(session, post_url)
                if not info or not info['post_id']:
                    return

                post_id = info['post_id']

                # Skip if already uploaded
                if await is_video_uploaded(post_id):
                    logger.info(f"Post {post_id} already uploaded, skipping.")
                    await update_last_checked(post_id)
                    return

                # Download
                os.makedirs(DOWNLOAD_DIR, exist_ok=True)
                filename = os.path.join(DOWNLOAD_DIR, f"{post_id}_{os.path.basename(info['video_url'])}")
                filepath = await download_video(session, info['video_url'], filename)
                if not filepath:
                    logger.warning(f"Download failed for post {post_id}, skipping.")
                    return

                # Upload
                success = await upload_video(client, filepath, info)
                if success:
                    await mark_uploaded(
                        post_id, filepath, info['title'], info['video_url'],
                        info['category'], info['tags'], info['duration'],
                        info['views'], info['description']
                    )
                    # Clean up local file to save space
                    try:
                        os.remove(filepath)
                        logger.info(f"Deleted local file {filepath}")
                    except OSError:
                        pass
                else:
                    logger.warning(f"Upload failed for post {post_id}, file kept for retry.")

                processed += 1
                state.processed = processed

        # Create tasks for all posts
        tasks = []
        for idx, post_url in enumerate(all_posts):
            if not state.running:
                break
            tasks.append(asyncio.create_task(limited_process(post_url)))
            # Check pause/stop between submissions
            while state.paused and state.running:
                await asyncio.sleep(1)
            if not state.running:
                break

        # Wait for all tasks to complete
        await asyncio.gather(*tasks)

        # Stop the upload client
        await client.stop()
        logger.info("Crawler finished.")
        state.running = False
        state.status = "stopped"

# ---------- Inline Keyboard Definitions ----------
def get_main_menu() -> InlineKeyboardMarkup:
    """Return the main control menu with inline buttons."""
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
        ],
    ]
    return InlineKeyboardMarkup(buttons)

def get_history_pagination(page: int, total_pages: int) -> InlineKeyboardMarkup:
    """Return pagination buttons for history."""
    buttons = []
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"history_{page-1}"))
    nav_buttons.append(InlineKeyboardButton(f"{page}/{total_pages}", callback_data="history_nop"))
    if page < total_pages:
        nav_buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"history_{page+1}"))
    if nav_buttons:
        buttons.append(nav_buttons)
    buttons.append([InlineKeyboardButton("🔙 Back to Menu", callback_data="menu")])
    return InlineKeyboardMarkup(buttons)

def get_single_cancel() -> InlineKeyboardMarkup:
    """Cancel button for single download."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("❌ Cancel", callback_data="menu")]
    ])

# ---------- Bot Command & Callback Handlers ----------
async def start_cmd(client: Client, message: Message):
    """Send the main menu when /start is issued."""
    await message.reply(
        "🤖 *MastiRaja Crawler Bot*\n"
        "Use the buttons below to control the crawler.",
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def start_crawler_callback(client: Client, callback: CallbackQuery):
    """Start the crawler from inline button."""
    if state.running:
        await callback.answer("⚠️ Crawler is already running.", show_alert=True)
        return
    state.task = asyncio.create_task(crawl_and_process())
    await callback.answer("✅ Crawler started.", show_alert=True)
    await callback.message.edit_text(
        "🔄 Crawler started. Use /status or the Status button to monitor progress.",
        reply_markup=get_main_menu()
    )

async def pause_crawler_callback(client: Client, callback: CallbackQuery):
    """Pause the crawler from inline button."""
    if not state.running:
        await callback.answer("⚠️ Crawler is not running.", show_alert=True)
        return
    if state.paused:
        await callback.answer("⚠️ Crawler is already paused.", show_alert=True)
        return
    state.paused = True
    state.status = "paused"
    await callback.answer("⏸ Crawler paused.", show_alert=True)
    await callback.message.edit_text(
        "⏸ Crawler paused. Use Resume to continue.",
        reply_markup=get_main_menu()
    )

async def resume_crawler_callback(client: Client, callback: CallbackQuery):
    """Resume the crawler from inline button."""
    if not state.running:
        await callback.answer("⚠️ Crawler is not running.", show_alert=True)
        return
    if not state.paused:
        await callback.answer("⚠️ Crawler is not paused.", show_alert=True)
        return
    state.paused = False
    state.status = "running"
    await callback.answer("▶️ Crawler resumed.", show_alert=True)
    await callback.message.edit_text(
        "▶️ Crawler resumed.",
        reply_markup=get_main_menu()
    )

async def stop_crawler_callback(client: Client, callback: CallbackQuery):
    """Stop the crawler from inline button."""
    if not state.running:
        await callback.answer("⚠️ Crawler is not running.", show_alert=True)
        return
    state.running = False
    state.paused = False
    state.status = "stopped"
    if state.task and not state.task.done():
        state.task.cancel()
        try:
            await state.task
        except asyncio.CancelledError:
            pass
    await callback.answer("⏹ Crawler stopped.", show_alert=True)
    await callback.message.edit_text(
        "⏹ Crawler stopped.",
        reply_markup=get_main_menu()
    )

async def status_callback(client: Client, callback: CallbackQuery):
    """Show current crawler status."""
    pending = await get_pending_count()
    uploaded = await get_total_uploaded_count()
    text = (
        f"📊 *Crawler Status*\n"
        f"• State: `{state.status}`\n"
        f"• Running: {state.running}\n"
        f"• Paused: {state.paused}\n"
        f"• Total Posts Found: {state.total_posts}\n"
        f"• Processed: {state.processed}\n"
        f"• Pending (in DB): {pending}\n"
        f"• Total Uploaded: {uploaded}\n"
        f"• Queue Size: {len(state.posts_queue)}"
    )
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def logs_callback(client: Client, callback: CallbackQuery):
    """Show the last 200 lines of the log file."""
    try:
        with open(LOG_FILE, 'r') as f:
            lines = f.readlines()
            tail = lines[-200:] if len(lines) > 200 else lines
            if not tail:
                text = "📄 No logs yet."
            else:
                txt = ''.join(tail)
                if len(txt) > 4000:
                    # Send as file
                    await callback.message.reply_document(
                        document=LOG_FILE,
                        caption="📄 Last 200 lines of logs",
                        reply_markup=get_main_menu()
                    )
                    await callback.answer("Logs sent as file.")
                    # Keep the original message intact
                    return
                else:
                    text = f"📄 *Logs (last 200 lines)*\n```\n{txt}```"
    except Exception as e:
        text = f"❌ Error reading logs: {e}"
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def history_callback(client: Client, callback: CallbackQuery, page: int = 1):
    """Show uploaded videos with pagination."""
    per_page = 10
    offset = (page - 1) * per_page
    rows = await get_all_uploaded(limit=per_page, offset=offset)
    total_uploaded = await get_total_uploaded_count()
    total_pages = (total_uploaded + per_page - 1) // per_page if total_uploaded > 0 else 1

    if not rows:
        text = "📭 No uploaded videos yet."
        reply_markup = get_main_menu()
    else:
        text = f"📜 *Uploaded Videos (Page {page}/{total_pages})*\n\n"
        for row in rows:
            text += f"• {row['title'][:50]} | {row['category']} | {row['duration']}\n"
        reply_markup = get_history_pagination(page, total_pages)

    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=reply_markup,
        parse_mode=ParseMode.MARKDOWN
    )

async def single_download_prompt_callback(client: Client, callback: CallbackQuery):
    """Prompt the user to enter a URL for single download."""
    await callback.answer()
    await callback.message.edit_text(
        "🔗 Please send the full URL of the video post you want to download and upload.\n"
        "Example: `https://mastiraja.com/some-video/`\n\n"
        "You can cancel by pressing the button below.",
        reply_markup=get_single_cancel(),
        parse_mode=ParseMode.MARKDOWN
    )
    # Store a flag in the state to indicate we're waiting for a URL
    # We'll use a dictionary for simplicity.
    if not hasattr(state, 'waiting_for_single'):
        state.waiting_for_single = False
    state.waiting_for_single = True

async def handle_single_url(client: Client, message: Message):
    """Process the user-supplied URL for single download."""
    if not hasattr(state, 'waiting_for_single') or not state.waiting_for_single:
        return
    state.waiting_for_single = False

    url = message.text.strip()
    if not url.startswith('http'):
        await message.reply("❌ Invalid URL. Please provide a full http(s) URL.")
        return

    progress_msg = await message.reply("⏳ Processing single video...")

    async with aiohttp.ClientSession() as session:
        info = await extract_video_info(session, url)
        if not info:
            await progress_msg.edit_text("❌ Could not extract video info from that URL.")
            return

        if await is_video_uploaded(info['post_id']):
            await progress_msg.edit_text(f"ℹ️ Video post {info['post_id']} already uploaded.")
            return

        # Download
        os.makedirs(DOWNLOAD_DIR, exist_ok=True)
        filename = os.path.join(DOWNLOAD_DIR, f"{info['post_id']}_{os.path.basename(info['video_url'])}")
        filepath = await download_video(session, info['video_url'], filename)
        if not filepath:
            await progress_msg.edit_text("❌ Download failed.")
            return

        # Upload with a fresh client
        upload_client = Client(
            "single_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True
        )
        await upload_client.start()
        success = await upload_video(upload_client, filepath, info)
        await upload_client.stop()

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
            await progress_msg.edit_text(f"✅ Successfully uploaded: {info['title']}")
        else:
            await progress_msg.edit_text("❌ Upload failed. Check logs.")

    # Return to main menu
    await progress_msg.reply("Return to main menu:", reply_markup=get_main_menu())

async def help_callback(client: Client, callback: CallbackQuery):
    """Show help message."""
    text = (
        "🤖 *MastiRaja Crawler Bot*\n\n"
        "This bot manages a crawler that scrapes videos from MastiRaja and uploads them to your channel.\n\n"
        "**Commands** (or use buttons):\n"
        "• /start – Show main menu\n"
        "• Start – Begin the full crawl\n"
        "• Pause – Temporarily pause\n"
        "• Resume – Continue after pause\n"
        "• Stop – Halt the crawler\n"
        "• Status – View current stats\n"
        "• Logs – View latest log entries\n"
        "• History – Show uploaded videos\n"
        "• Single Download – Upload one specific video\n"
        "\nMade with 🥔 by Potato."
    )
    await callback.answer()
    await callback.message.edit_text(
        text,
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

async def back_to_menu_callback(client: Client, callback: CallbackQuery):
    """Return to main menu."""
    await callback.answer()
    await callback.message.edit_text(
        "🤖 *MastiRaja Crawler Bot*\nUse the buttons below to control the crawler.",
        reply_markup=get_main_menu(),
        parse_mode=ParseMode.MARKDOWN
    )

# ---------- Main Bot Class ----------
class MastiRajaBot:
    """The main bot class that sets up and runs the pyrogram client."""

    def __init__(self):
        self.app = Client(
            "mastiraja_bot",
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            in_memory=True,
            workers=20,
            parse_mode=ParseMode.MARKDOWN
        )

        # Register handlers
        self.app.on_message()(self.handle_message)
        self.app.on_callback_query()(self.handle_callback)

    async def handle_message(self, client: Client, message: Message):
        """Handle incoming text messages (commands and single URL input)."""
        if not message.text:
            return

        # If waiting for single URL, process it
        if hasattr(state, 'waiting_for_single') and state.waiting_for_single:
            await handle_single_url(client, message)
            return

        # Command handling
        if message.text.startswith('/'):
            cmd = message.text.split()[0].lower()
            if cmd == '/start':
                await start_cmd(client, message)
            elif cmd == '/pause':
                await pause_crawler_callback(client, message)  # Not a callback, but we can reuse
                # Actually, we should implement separate functions for command-based pause, etc.
                # For simplicity, we'll treat commands as text and reply with the same actions.
                # But to keep it clean, we'll implement command handlers here.
                if cmd == '/start':
                    await start_cmd(client, message)
                elif cmd == '/pause':
                    # Reuse callback logic but adapt for message
                    if not state.running:
                        await message.reply("⚠️ Crawler is not running.")
                        return
                    if state.paused:
                        await message.reply("⚠️ Already paused.")
                        return
                    state.paused = True
                    state.status = "paused"
                    await message.reply("⏸ Paused.")
                elif cmd == '/resume':
                    if not state.running:
                        await message.reply("⚠️ Not running.")
                        return
                    if not state.paused:
                        await message.reply("⚠️ Not paused.")
                        return
                    state.paused = False
                    state.status = "running"
                    await message.reply("▶️ Resumed.")
                elif cmd == '/stop':
                    if not state.running:
                        await message.reply("⚠️ Not running.")
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
                    await message.reply("⏹ Stopped.")
                elif cmd == '/status':
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
                    await message.reply(text, parse_mode=ParseMode.MARKDOWN)
                elif cmd == '/logs':
                    # Reuse logs callback logic
                    try:
                        with open(LOG_FILE, 'r') as f:
                            lines = f.readlines()
                            tail = lines[-200:] if len(lines) > 200 else lines
                            if not tail:
                                await message.reply("📄 No logs yet.")
                            else:
                                txt = ''.join(tail)
                                if len(txt) > 4000:
                                    await message.reply_document(document=LOG_FILE, caption="📄 Last 200 lines")
                                else:
                                    await message.reply(f"📄 *Logs*\n```\n{txt}```", parse_mode=ParseMode.MARKDOWN)
                    except Exception as e:
                        await message.reply(f"❌ Error: {e}")
                elif cmd == '/history':
                    # Show first page of history
                    rows = await get_all_uploaded(limit=10, offset=0)
                    total = await get_total_uploaded_count()
                    total_pages = (total + 9) // 10 if total > 0 else 1
                    if not rows:
                        await message.reply("📭 No uploaded videos yet.")
                    else:
                        text = f"📜 *Uploaded Videos (Page 1/{total_pages})*\n\n"
                        for row in rows:
                            text += f"• {row['title'][:50]} | {row['category']} | {row['duration']}\n"
                        await message.reply(text, reply_markup=get_history_pagination(1, total_pages), parse_mode=ParseMode.MARKDOWN)
                elif cmd == '/single':
                    # Prompt for URL
                    await message.reply(
                        "🔗 Please send the full URL of the video post you want to download and upload.\n"
                        "Example: `https://mastiraja.com/some-video/`",
                        reply_markup=get_single_cancel(),
                        parse_mode=ParseMode.MARKDOWN
                    )
                    state.waiting_for_single = True
                elif cmd == '/help':
                    text = (
                        "🤖 *MastiRaja Crawler Bot*\n\n"
                        "Commands:\n"
                        "/start – Show main menu\n"
                        "/pause – Pause\n"
                        "/resume – Resume\n"
                        "/stop – Stop\n"
                        "/status – Show status\n"
                        "/single <url> – Download single\n"
                        "/logs – Show logs\n"
                        "/history – Show history\n"
                        "/help – This message"
                    )
                    await message.reply(text, parse_mode=ParseMode.MARKDOWN)
                else:
                    await message.reply("❌ Unknown command. Use /help.")
            else:
                await message.reply("❌ Unknown command. Use /help.")

    async def handle_callback(self, client: Client, callback: CallbackQuery):
        """Handle callback queries from inline keyboards."""
        data = callback.data

        if data == "start":
            await start_crawler_callback(client, callback)
        elif data == "pause":
            await pause_crawler_callback(client, callback)
        elif data == "resume":
            await resume_crawler_callback(client, callback)
        elif data == "stop":
            await stop_crawler_callback(client, callback)
        elif data == "status":
            await status_callback(client, callback)
        elif data == "logs":
            await logs_callback(client, callback)
        elif data == "history":
            await history_callback(client, callback, page=1)
        elif data == "single":
            await single_download_prompt_callback(client, callback)
        elif data == "help":
            await help_callback(client, callback)
        elif data == "menu":
            await back_to_menu_callback(client, callback)
        elif data.startswith("history_"):
            parts = data.split("_")
            if len(parts) == 2 and parts[1].isdigit():
                page = int(parts[1])
                await history_callback(client, callback, page=page)
            else:
                await callback.answer("Invalid page.")
        elif data == "history_nop":
            await callback.answer("This is the current page.")
        else:
            await callback.answer("Unknown action.")

    async def run(self):
        """Start the bot and keep it running."""
        await self.app.start()
        logger.info("Bot started.")
        await asyncio.Event().wait()

# ---------- Entry Point ----------
async def main():
    """Initialize database and start the bot."""
    await init_db()
    bot = MastiRajaBot()
    await bot.run()

if __name__ == "__main__":
    asyncio.run(main())
