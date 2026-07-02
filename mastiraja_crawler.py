import asyncio
import sys

# 🔥 PYTHON 3.14 & WINDOWS CRASH FIX: Pyrogram import hone se PEHLE loop setup hona zaroori hai
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

# --- BAAKI IMPORTS ---
import os
import re
import base64
import logging
import urllib.parse
from datetime import datetime
from typing import Optional, List, Dict
from urllib.parse import urljoin, urlparse, parse_qs

import aiohttp
import aiofiles
import yt_dlp
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import RPCError, Unauthorized, BadRequest, FloodWait
from pyrogram.enums import ParseMode

load_dotenv()

# ========= CONFIGURATION ==========
BASE_URL = "https://mastiraja.com"
API_ID = int(os.getenv("API_ID", 0))
API_HASH = os.getenv("API_HASH", "")
STRING_SESSION = os.getenv("STRING_SESSION", "").strip()
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
LOG_FILE = os.getenv("LOG_FILE", "crawler.log")
PORT = int(os.getenv("PORT", 8080))  

AUTO_START = os.getenv("AUTO_START", "false").lower() == "true"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_PAGES_TO_SCAN = 15  
REQUEST_TIMEOUT = 30
MAX_RETRIES = 3

# ---------- Channel ID Casting ----------
raw_channel_id = os.getenv("CHANNEL_ID", "").strip()
if raw_channel_id.startswith("-100") or raw_channel_id.isdigit() or (raw_channel_id.startswith("-") and raw_channel_id[1:].isdigit()):
    CHANNEL_ID = int(raw_channel_id)
else:
    CHANNEL_ID = raw_channel_id

# ---------- Logging Setup ----------
logger = logging.getLogger("crawler")
logger.setLevel(logging.INFO)
fh = logging.FileHandler(LOG_FILE, mode='a', encoding='utf-8')
fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(ch)

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
        f"🔄 **MastiRaja Live Monitor**\n"
        f"• System State: `{state.status.upper()}`\n"
        f"• Current Scan Page: `{state.current_page}`\n"
        f"─────────────────────\n"
    )
    
    if state.current_stage == "Scraping":
        text += f"🛰️ **Phase:** Reading Page {state.current_page} links from site..."
    elif state.current_stage in ["Live Checking", "Extracting Info", "Downloading", "Uploading"]:
        text += (
            f"📦 **Phase: Pipeline**\n"
            f"• Queue: `{proc} / {total}` links handled\n"
            f"• Progress: `[{overall_bar}] {overall_pct:.1f}%`\n\n"
            f"🎬 **Active Target:**\n"
            f"• Title: `{cleaned_title[:55]}`\n"
            f"• Sub-Task: `{state.current_stage}`\n"
        )
        if state.current_stage == "Downloading":
            text += f"• **Downloading:** `{state.download_pct:.1f}%`\n"
        elif state.current_stage == "Uploading":
            text += f"• **Uploading:** `{state.upload_pct:.1f}%`\n"
    elif state.current_stage == "Finished":
        text += f"✅ **Run Finished!** Up to page `{state.current_page}`."
    return text

async def live_ui_refresh_loop(client: Client):
    while state.running:
        current_text = build_live_status_text()
        if state.status_msg:
            try:
                await state.status_msg.edit_text(current_text, parse_mode=ParseMode.MARKDOWN)
            except Exception:
                pass
        await asyncio.sleep(4)

async def fetch_soup(session: aiohttp.ClientSession, url: str):
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status != 200:
                    logger.error(f"❌ Site responded with status {resp.status} on {url}")
                    return None
                html = await resp.text()
                return BeautifulSoup(html, 'html.parser')
        except Exception as e:
            logger.warning(f"Fetch failed (Attempt {attempt+1}): {e}")
            await asyncio.sleep(2)
    return None

def extract_post_links(soup: BeautifulSoup) -> List[str]:
    links = []
    articles = soup.find_all('article', class_='thumb-block') or soup.find_all('article')
    logger.info(f"🔍 Page par total {len(articles)} posts mile.")
    
    for article in articles:
        a = article.find('a', href=True)
        if a:
            href = a['href']
            if href.startswith('/'):
                href = urljoin(BASE_URL, href)
            if href not in links and "mastiraja.com" in href:
                links.append(href)
    return links

async def extract_video_info(session: aiohttp.ClientSession, post_url: str) -> Optional[Dict]:
    soup = await fetch_soup(session, post_url)
    if not soup: 
        logger.error(f"❌ Post ka HTML fetch nahi ho paya: {post_url}")
        return None
    
    post_id = extract_slug_id(post_url)
    title_tag = soup.find('h1')
    title = title_tag.get_text(strip=True) if title_tag else "Untitled"
    
    category = "Video"
    cat_tag = soup.find('a', rel='category tag') or soup.find('span', class_='category') or soup.find('div', class_='post-categories')
    if cat_tag:
        category = cat_tag.get_text(strip=True)

    tags_list = []
    for tag in soup.find_all('a', rel='tag') or soup.find_all('span', class_='tag'):
        cleaned_tag = re.sub(r'[^a-zA-Z0-9]', '', tag.get_text(strip=True))
        if cleaned_tag:
            tags_list.append(f"#{cleaned_tag}")
    tags_str = " ".join(tags_list[:5])

    description = ""
    desc_tag = soup.find('div', class_='entry-content') or soup.find('div', class_='description') or soup.find('p', class_='description')
    if desc_tag:
        p_tags = desc_tag.find_all('p')
        description = "\n".join([p.get_text(strip=True) for p in p_tags[:2]]) if p_tags else desc_tag.get_text(strip=True)
        description = description[:200] + "..." if len(description) > 200 else description

    video_url = None
    
    iframe = soup.find('iframe', src=True)
    if iframe:
        src = iframe['src']
        parsed = urlparse(src)
        qs = parse_qs(parsed.query)
        b64_data = qs.get('q', [''])[0]
        
        if b64_data:
            try:
                decoded = base64.b64decode(b64_data).decode('utf-8', errors='ignore')
                unquoted = urllib.parse.unquote(decoded)
                match = re.search(r'src=["\'](https?://[^\s"\']+\.(?:mp4|m3u8|webm)[^\s"\']*)["\']', unquoted)
                if not match:
                    match = re.search(r'src=["\'](https?://[^"\']+)["\']', unquoted)
                    
                if match:
                    video_url = match.group(1).replace('&amp;', '&')
            except Exception as e:
                logger.error(f"❌ Base64/Unquote Decoder Failure: {e}")

    if not video_url:
        video_tag = soup.find('video')
        if video_tag:
            video_url = video_tag.get('src')
            if not video_url:
                source = video_tag.find('source')
                if source: video_url = source.get('src')

    if not video_url: 
        logger.warning(f"❌ Is link par koi playable video source nahi mila: {post_url}")
        return None
        
    logger.info(f"🎯 Success! Asli Video URL mil gaya: {video_url}")
    return {
        'post_id': post_id, 'title': title, 'video_url': video_url,
        'category': category, 'tags': tags_str, 'duration': 0, 'views': '', 'description': description
    }

# ---------- Core Downloader with Auto Metadata ----------
async def download_video(post_id: str, video_url: str, download_dir: str) -> Optional[Dict]:
    os.makedirs(download_dir, exist_ok=True)
    safe_post_id = re.sub(r'[^a-zA-Z0-9_-]', '', post_id)[:50]
    outtmpl = os.path.join(download_dir, f"{safe_post_id}_%(title)s.%(ext)s")
    
    media_data = {
        "filepath": None,
        "duration": 0,
        "thumbnail": None
    }

    def progress_hook(d):
        if d['status'] == 'finished':
            media_data["filepath"] = d['filename']
            state.download_pct = 100.0
        elif d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)
            if total > 0:
                state.download_pct = (downloaded / total) * 100

    ydl_opts = {
        'outtmpl': outtmpl, 
        'quiet': True, 
        'no_warnings': True,
        'progress_hooks': [progress_hook],
        'writethumbnail': True, 
        'http_headers': {'User-Agent': USER_AGENT, 'Referer': BASE_URL}
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info_dict = await asyncio.to_thread(lambda: ydl.extract_info(video_url, download=True))
            if info_dict:
                media_data["duration"] = int(info_dict.get("duration", 0))
        
        if not media_data["filepath"] or not os.path.exists(media_data["filepath"]):
            for file in os.listdir(download_dir):
                if file.startswith(f"{safe_post_id}_") and not file.endswith(('.jpg', '.jpeg', '.webp', '.png')):
                    media_data["filepath"] = os.path.join(download_dir, file)
                    break

        for file in os.listdir(download_dir):
            if file.startswith(f"{safe_post_id}_") and file.endswith(('.jpg', '.jpeg', '.webp', '.png')):
                media_data["thumbnail"] = os.path.join(download_dir, file)
                break

        if media_data["filepath"]: 
            return media_data
    except Exception as e:
        logger.error(f"❌ yt-dlp Download Error: {e}")
    return None

# ---------- Telegram Broadcaster with Video Properties ----------
async def upload_video(client: Client, media_info: Dict, info: Dict) -> bool:
    category_line = f"📂 **Category:** {clean_for_tg(info['category'])}"
    if info['tags']:
        category_line += f" {clean_for_tg(info['tags'])}"
        
    description_line = f"\n\n📝 **Description:**\n{clean_for_tg(info['description'])}" if info['description'] else ""

    caption = (
        f"📹 **Title:** {clean_for_tg(info['title'])}\n"
        f"{category_line}\n"
        f"🆔 **ID:** `{info['post_id']}`"
        f"{description_line}"
    )
    
    filepath = media_info.get("filepath")
    duration = media_info.get("duration", 0)
    thumb_path = media_info.get("thumbnail")
    thumb_to_pass = None

    if thumb_path and os.path.exists(thumb_path):
        if thumb_path.endswith(('.webp', '.png')):
            try:
                from PIL import Image
                img = Image.open(thumb_path)
                img = img.convert('RGB')
                img.thumbnail((320, 320)) 
                jpg_thumb_path = thumb_path.rsplit('.', 1)[0] + "_tg.jpg"
                img.save(jpg_thumb_path, 'JPEG', quality=85)
                thumb_to_pass = jpg_thumb_path
            except ImportError:
                thumb_to_pass = thumb_path 
        else:
            thumb_to_pass = thumb_path

    try:
        def upload_progress(current, total):
            if total > 0: state.upload_pct = (current / total) * 100

        send_kwargs = {
            "chat_id": CHANNEL_ID, 
            "video": filepath, 
            "caption": caption,
            "parse_mode": ParseMode.MARKDOWN, 
            "supports_streaming": True, 
            "progress": upload_progress
        }
        
        if duration and int(duration) > 0:
            send_kwargs["duration"] = int(duration)
            
        if thumb_to_pass and os.path.exists(thumb_to_pass):
            send_kwargs["thumb"] = thumb_to_pass

        await client.send_video(**send_kwargs)
        
        if thumb_to_pass and thumb_to_pass != thumb_path and os.path.exists(thumb_to_pass):
            try: os.remove(thumb_to_pass)
            except Exception: pass
            
        return True
    except Exception as e:
        logger.error(f"❌ Telegram Upload Error: {e}")
        return False

# ---------- Dynamic Processing Engine ----------
async def crawl_and_process(user_client: Client):
    async with state.lock:
        if state.running: return
        state.running = True
        state.paused = False
        state.status = "running"
    
    ui_task = asyncio.create_task(live_ui_refresh_loop(user_client))
    
    async with aiohttp.ClientSession() as session:
        
        while state.current_page <= MAX_PAGES_TO_SCAN and state.running:
            url = BASE_URL if state.current_page == 1 else f"{BASE_URL}/page/{state.current_page}/"
            logger.info(f"Scanning page {state.current_page}... Link: {url}")
            state.current_stage = "Scraping"
            
            soup = await fetch_soup(session, url)
            if not soup: 
                logger.error(f"❌ Page {state.current_page} fetch nahi ho saki.")
                break
                
            links = extract_post_links(soup)
            if not links: 
                logger.warning(f"⚠️ Page {state.current_page} par koi posts nahi mile.")
                break
                
            state.total_posts = len(links)
            state.processed = 0
            
            for target_url in links:
                if not state.running: break
                    
                slug_id = extract_slug_id(target_url)
                state.current_stage = "Live Checking"
                
                # 🔍 EVERY TIME LIVE PYROGRAM SEARCH ENGINE
                already_uploaded = False
                if slug_id:
                    try:
                        # Chat me direct slug_id search kar rahe hain (limit=1 fast result ke liye)
                        async for _ in user_client.search_messages(chat_id=CHANNEL_ID, query=slug_id, limit=1):
                            already_uploaded = True
                            break
                    except FloodWait as f:
                        logger.warning(f"⚠️ Telegram Rate Limit hit! Sleeping for {f.value}s before retrying search...")
                        await asyncio.sleep(f.value)
                        # Retry search once after sleep
                        async for _ in user_client.search_messages(chat_id=CHANNEL_ID, query=slug_id, limit=1):
                            already_uploaded = True
                            break
                    except Exception as e:
                        logger.error(f"❌ Live search error for ID {slug_id}: {e}")

                # Check dynamic search response
                if already_uploaded:
                    logger.info(f"⏭️ Live Match Found! Already Uploaded (Skipping Link): {slug_id}")
                    state.processed += 1
                    continue 
                
                state.current_stage = "Extracting Info"
                logger.info(f"🔗 Processing Link: {target_url}")
                
                info = await extract_video_info(session, target_url)
                if not info:
                    state.processed += 1
                    continue
                    
                state.current_title = info['title']
                state.download_pct = 0.0
                state.upload_pct = 0.0
                
                state.current_stage = "Downloading"
                media_info = await download_video(info['post_id'], info['video_url'], DOWNLOAD_DIR)
                if not media_info or not media_info.get("filepath"):
                    state.processed += 1
                    continue
                    
                state.current_stage = "Uploading"
                success = await upload_video(user_client, media_info, info)
                if success:
                    try: os.remove(media_info["filepath"])
                    except Exception: pass
                    if media_info.get("thumbnail") and os.path.exists(media_info["thumbnail"]):
                        try: os.remove(media_info["thumbnail"])
                        except Exception: pass
                
                state.processed += 1
                await asyncio.sleep(2) 
                
            if state.running:
                state.current_page += 1
            
    if state.current_page > MAX_PAGES_TO_SCAN:
        state.current_page = 1  
        
    state.current_stage = "Finished"
    state.running = False
    state.status = "stopped"
    ui_task.cancel()

# ---------- Command Routines ----------
async def start_task_cmd(client: Client, message: Message):
    if state.running:
        await message.reply("⚠️ Crawler already running.")
        return
        
    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        state.current_page = int(args[1])

    state.status_msg = await message.reply(f"🚀 **Initializing Userbot Scanner from Page {state.current_page} with Realtime Live Search...**")
    state.task = asyncio.create_task(crawl_and_process(client))

async def stop_cmd(client: Client, message: Message):
    state.running = False
    state.status = "stopped"
    await message.reply(f"⏹ Engine stopped manually at Page `{state.current_page}`.")

async def ping_cmd(client: Client, message: Message):
    await message.reply("🏓 Pong! Live.")

async def http_server():
    from aiohttp import web
    app = web.Application()
    app.router.add_get('/', lambda r: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, host='0.0.0.0', port=PORT).start()

async def main():
    asyncio.create_task(http_server())
    if not STRING_SESSION:
        print("❌ STRING_SESSION missing!")
        return

    app = Client(
        "mastiraja_userbot",
        api_id=API_ID, 
        api_hash=API_HASH,
        session_string=STRING_SESSION,
        in_memory=True
    )

    @app.on_message(filters.me & filters.command(["starttask", "stop", "ping"]))
    async def handle_msg(client, message):
        cmd = message.text.split()[0].lower()
        if cmd == '/starttask': await start_task_cmd(client, message)
        elif cmd == '/stop': await stop_cmd(client, message)
        elif cmd == '/ping': await ping_cmd(client, message)

    await app.start()
    logger.info("✨ Userbot Online! Send /starttask to run.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    loop.run_until_complete(main())
