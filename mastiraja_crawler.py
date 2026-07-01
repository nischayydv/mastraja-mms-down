#!/usr/bin/env python3
"""
MastiRaja Crawler – Pyroblack edition for Koyeb / Render
Hyper‑fast, highest quality, fully concurrent, ephemeral storage friendly.
Author: Potato
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
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from pyroblack import Client
from pyroblack.errors import RPCError

# ========= CONFIG from environment ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
CHANNEL_ID = os.getenv("CHANNEL_ID", "")       # e.g., @mychannel
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/downloads")  # use /tmp for ephemeral
DB_FILE = os.getenv("DB_FILE", "/tmp/videos.db")
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

# Concurrency limits
MAX_PAGE_FETCH = 5
MAX_VIDEO_EXTRACT = 10
MAX_DOWNLOADS = 5
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3
# ============================

if not all([API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID]):
    raise ValueError("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN, CHANNEL_ID")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Semaphores
page_sem = asyncio.Semaphore(MAX_PAGE_FETCH)
extract_sem = asyncio.Semaphore(MAX_VIDEO_EXTRACT)
download_sem = asyncio.Semaphore(MAX_DOWNLOADS)

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

async def update_last_checked(post_id):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("UPDATE videos SET last_checked = datetime('now') WHERE post_id = ?", (post_id,))
        await db.commit()

# ---------- Web helpers ----------
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

# ---------- Pagination & post links ----------
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

# ---------- Highest quality video source ----------
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
    async with extract_sem:
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
            logger.warning(f"No video URL found for {post_url}")
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

        # views is still extracted but NOT used in caption
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
            'views': views,          # stored but not displayed
            'description': desc,
        }

# ---------- Downloader ----------
async def download_video(session, video_url, filepath):
    if os.path.exists(filepath):
        logger.info(f"File exists: {filepath}")
        return filepath
    async with download_sem:
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
                    logger.info(f"Downloaded {filepath} ({downloaded} bytes)")
                    return filepath
            except Exception as e:
                logger.warning(f"Download attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        logger.error(f"Failed to download {video_url}")
        return None

# ---------- Upload via pyroblack (caption WITHOUT views and original link) ----------
async def upload_video(client, filepath, info):
    caption = f"📹 *{info['title']}*\n"
    if info['category']:
        caption += f"📂 Category: {info['category']}\n"
    if info['tags']:
        caption += f"🏷️ Tags: {info['tags']}\n"
    if info['duration']:
        caption += f"⏱️ Duration: {info['duration']}\n"
    # Removed: Views and Original Post link
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
        logger.info(f"Uploaded {filepath}")
        return True
    except RPCError as e:
        logger.error(f"Upload error for {filepath}: {e}")
        return False

# ---------- Main crawler ----------
async def crawl_all_posts(session):
    to_visit = [BASE_URL]
    visited = set()
    all_posts = set()
    while to_visit:
        url = to_visit.pop()
        if url in visited:
            continue
        visited.add(url)
        async with page_sem:
            soup = await fetch_soup(session, url)
            if not soup:
                continue
            all_posts.update(extract_post_links(soup))
            for p in extract_pagination_links(soup):
                if p not in visited:
                    to_visit.append(p)
            await asyncio.sleep(0.3)
    return list(all_posts)

async def process_post(session, client, post_url):
    info = await extract_video_info(session, post_url)
    if not info or not info['post_id']:
        return

    post_id = info['post_id']
    if await is_video_uploaded(post_id):
        logger.info(f"Post {post_id} already uploaded, skipping.")
        await update_last_checked(post_id)
        return

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = os.path.join(DOWNLOAD_DIR, f"{post_id}_{os.path.basename(info['video_url'])}")

    filepath = await download_video(session, info['video_url'], filename)
    if not filepath:
        return

    success = await upload_video(client, filepath, info)
    if success:
        await mark_uploaded(
            post_id, filepath, info['title'], info['video_url'],
            info['category'], info['tags'], info['duration'],
            info['views'], info['description']
        )
        # Clean up to save disk space on ephemeral storage
        try:
            os.remove(filepath)
            logger.info(f"Deleted local file {filepath} after upload.")
        except OSError:
            pass
    else:
        logger.warning(f"Upload failed for post {post_id}, file kept for retry.")

async def main():
    await init_db()
    logger.info(f"Using database: {DB_FILE}")

    client = Client(
        "mastiraja_bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        in_memory=True,
        workers=10
    )
    await client.start()
    logger.info("pyroblack client started.")

    async with aiohttp.ClientSession() as session:
        logger.info("Collecting all video URLs...")
        post_urls = await crawl_all_posts(session)
        logger.info(f"Found {len(post_urls)} posts.")

        tasks = [asyncio.create_task(process_post(session, client, url)) for url in post_urls]
        await asyncio.gather(*tasks)

    await client.stop()
    logger.info("All done.")

if __name__ == "__main__":
    asyncio.run(main())
