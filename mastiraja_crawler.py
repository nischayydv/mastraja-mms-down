import asyncio
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import os
import re
import json
import math
import time
import base64
import shutil
import random
import signal
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Set, Tuple
from urllib.parse import urljoin, urlparse, parse_qs

import aiohttp
import yt_dlp
from aiohttp import web, ClientTimeout, TCPConnector
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import FloodWait, RPCError
from pyrogram.enums import ParseMode

load_dotenv()

# =========================================================
# CONFIG
# =========================================================

@dataclass
class AppConfig:
    BASE_URL: str = os.getenv("BASE_URL", "https://mastiraja.com").rstrip("/")
    API_ID: int = int(os.getenv("API_ID", "0") or 0)
    API_HASH: str = os.getenv("API_HASH", "").strip()
    STRING_SESSION: str = os.getenv("STRING_SESSION", "").strip()

    DOWNLOAD_DIR: str = os.getenv("DOWNLOAD_DIR", "./downloads")
    DATA_DIR: str = os.getenv("DATA_DIR", "./data")
    TMP_DIR: str = os.getenv("TMP_DIR", "./tmp")
    LOG_FILE: str = os.getenv("LOG_FILE", "crawler.log")

    PORT: int = int(os.getenv("PORT", "8080"))
    AUTO_START: bool = os.getenv("AUTO_START", "false").lower() == "true"

    USER_AGENT: str = os.getenv(
        "USER_AGENT",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )

    MAX_PAGES_TO_SCAN: int = int(os.getenv("MAX_PAGES_TO_SCAN", "15"))
    REQUEST_TIMEOUT: int = int(os.getenv("REQUEST_TIMEOUT", "30"))
    CONNECT_TIMEOUT: int = int(os.getenv("CONNECT_TIMEOUT", "15"))
    SOCK_READ_TIMEOUT: int = int(os.getenv("SOCK_READ_TIMEOUT", "30"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    ITEM_DELAY: float = float(os.getenv("ITEM_DELAY", "2"))
    PAGE_DELAY: float = float(os.getenv("PAGE_DELAY", "1.5"))
    UI_REFRESH_SECONDS: int = int(os.getenv("UI_REFRESH_SECONDS", "4"))
    CONNECTOR_LIMIT: int = int(os.getenv("CONNECTOR_LIMIT", "20"))
    CONNECTOR_LIMIT_PER_HOST: int = int(os.getenv("CONNECTOR_LIMIT_PER_HOST", "8"))
    SAVE_DB_EVERY: int = int(os.getenv("SAVE_DB_EVERY", "5"))
    SEARCH_LIMIT: int = int(os.getenv("SEARCH_LIMIT", "1"))
    MAX_CAPTION_LENGTH: int = int(os.getenv("MAX_CAPTION_LENGTH", "1024"))
    KEEP_FILES_ON_FAIL: bool = os.getenv("KEEP_FILES_ON_FAIL", "false").lower() == "true"

    CHANNEL_ID_RAW: str = os.getenv("CHANNEL_ID", "").strip()

    def channel_id(self):
        raw = self.CHANNEL_ID_RAW
        if raw.startswith("-100") or raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit()):
            return int(raw)
        return raw

    def validate(self):
        issues = []
        if not self.API_ID:
            issues.append("API_ID missing")
        if not self.API_HASH:
            issues.append("API_HASH missing")
        if not self.STRING_SESSION:
            issues.append("STRING_SESSION missing")
        if not self.CHANNEL_ID_RAW:
            issues.append("CHANNEL_ID missing")
        if issues:
            raise RuntimeError("Config error(s): " + ", ".join(issues))


CFG = AppConfig()

Path(CFG.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
Path(CFG.DATA_DIR).mkdir(parents=True, exist_ok=True)
Path(CFG.TMP_DIR).mkdir(parents=True, exist_ok=True)

# =========================================================
# LOGGING
# =========================================================

logger = logging.getLogger("mastiraja_advanced")
logger.setLevel(logging.INFO)

if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(CFG.LOG_FILE, mode="a", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# =========================================================
# PATHS / DB FILES
# =========================================================

PROCESSED_DB = Path(CFG.DATA_DIR) / "processed_ids.json"
FAILED_DB = Path(CFG.DATA_DIR) / "failed_items.json"
SETTINGS_DB = Path(CFG.DATA_DIR) / "settings.json"
RUNTIME_DB = Path(CFG.DATA_DIR) / "runtime_state.json"

# =========================================================
# UTILS
# =========================================================

def now_ts() -> float:
    return time.time()

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def sanitize_text(text: str) -> str:
    if text is None:
        return ""
    return str(text).strip()

def clean_for_tg(text: str) -> str:
    text = sanitize_text(text)
    if not text:
        return ""
    text = text.replace("`", "'").replace("*", "").replace("_", "")
    text = text.replace("<", "[").replace(">", "]")
    return text

def safe_filename(text: str, limit: int = 80) -> str:
    text = sanitize_text(text)
    text = re.sub(r"[^\w\-\. ]+", "", text)
    text = re.sub(r"\s+", "_", text).strip("._ ")
    return text[:limit] or "file"

def extract_slug_id(url: str) -> str:
    try:
        parsed = urlparse(url)
        slug = parsed.path.strip("/")
        if "/" in slug:
            slug = slug.split("/")[-1]
        return slug or "unknown_target"
    except Exception:
        return "unknown_target"

def clamp(v: float, a: float, b: float) -> float:
    return max(a, min(b, v))

def make_progress_bar(percentage: float, length: int = 12) -> str:
    percentage = clamp(percentage, 0.0, 100.0)
    filled = int(round((percentage / 100) * length))
    return "█" * filled + "░" * (length - filled)

def human_bytes(size: int) -> str:
    if size <= 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB"]
    power = min(int(math.log(size, 1024)), len(units) - 1)
    value = size / (1024 ** power)
    return f"{value:.2f} {units[power]}"

def human_seconds(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"

def sleep_jitter(base: float, spread: float = 0.35) -> float:
    return max(0.0, base + random.uniform(-spread, spread))

def truncate(text: str, limit: int) -> str:
    text = sanitize_text(text)
    return text if len(text) <= limit else text[:limit - 3] + "..."

def read_json(path: Path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning(f"JSON read failed [{path.name}]: {e}")
    return default

def write_json(path: Path, data) -> bool:
    try:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        return True
    except Exception as e:
        logger.warning(f"JSON write failed [{path.name}]: {e}")
        return False

def ensure_dirs():
    Path(CFG.DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
    Path(CFG.DATA_DIR).mkdir(parents=True, exist_ok=True)
    Path(CFG.TMP_DIR).mkdir(parents=True, exist_ok=True)

# =========================================================
# STORAGE
# =========================================================

class JsonStore:
    def __init__(self):
        ensure_dirs()
        self.processed_ids: Set[str] = set(read_json(PROCESSED_DB, []))
        self.failed_items: List[Dict[str, Any]] = read_json(FAILED_DB, [])
        self.settings: Dict[str, Any] = read_json(SETTINGS_DB, {})
        self.runtime_state: Dict[str, Any] = read_json(RUNTIME_DB, {})

    def has_processed(self, slug_id: str) -> bool:
        return slug_id in self.processed_ids

    def add_processed(self, slug_id: str):
        self.processed_ids.add(slug_id)

    def save_processed(self):
        write_json(PROCESSED_DB, sorted(self.processed_ids))

    def add_failed(self, item: Dict[str, Any]):
        self.failed_items.append(item)

    def save_failed(self):
        write_json(FAILED_DB, self.failed_items[-5000:])

    def save_runtime(self, data: Dict[str, Any]):
        self.runtime_state = data
        write_json(RUNTIME_DB, data)

    def get_setting(self, key: str, default=None):
        return self.settings.get(key, default)

    def set_setting(self, key: str, value: Any):
        self.settings[key] = value
        write_json(SETTINGS_DB, self.settings)

STORE = JsonStore()

# =========================================================
# METRICS / STATE
# =========================================================

@dataclass
class Metrics:
    started_at: float = 0.0
    ended_at: float = 0.0

    pages_scanned: int = 0
    links_found: int = 0
    processed_items: int = 0

    uploaded: int = 0
    skipped_local_db: int = 0
    skipped_live_search: int = 0

    failed_extract: int = 0
    failed_download: int = 0
    failed_upload: int = 0

    bytes_downloaded_total: int = 0
    upload_attempts: int = 0

@dataclass
class EngineState:
    running: bool = False
    paused: bool = False
    stopping: bool = False
    status: str = "idle"

    current_page: int = 1
    current_page_total_posts: int = 0
    current_page_processed: int = 0

    current_stage: str = "Idle"
    current_title: str = "None"
    current_slug: str = "-"
    current_url: str = "-"
    current_error: str = "None"

    download_pct: float = 0.0
    upload_pct: float = 0.0
    current_downloaded_bytes: int = 0
    current_upload_bytes: int = 0

    status_msg: Optional[Message] = None
    worker_task: Optional[asyncio.Task] = None
    ui_task: Optional[asyncio.Task] = None
    autosave_task: Optional[asyncio.Task] = None

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    pause_event: asyncio.Event = field(default_factory=asyncio.Event)

    recent_ids: Set[str] = field(default_factory=set)
    metrics: Metrics = field(default_factory=Metrics)

    last_status_text: str = ""
    last_status_update_at: float = 0.0

    def reset_progress_fields(self):
        self.current_stage = "Idle"
        self.current_title = "None"
        self.current_slug = "-"
        self.current_url = "-"
        self.current_error = "None"
        self.download_pct = 0.0
        self.upload_pct = 0.0
        self.current_downloaded_bytes = 0
        self.current_upload_bytes = 0
        self.current_page_total_posts = 0
        self.current_page_processed = 0

    def reset_metrics(self):
        self.metrics = Metrics(started_at=now_ts())

    def to_runtime_dict(self) -> Dict[str, Any]:
        return {
            "running": self.running,
            "paused": self.paused,
            "stopping": self.stopping,
            "status": self.status,
            "current_page": self.current_page,
            "current_page_total_posts": self.current_page_total_posts,
            "current_page_processed": self.current_page_processed,
            "current_stage": self.current_stage,
            "current_title": self.current_title,
            "current_slug": self.current_slug,
            "current_url": self.current_url,
            "current_error": self.current_error,
            "download_pct": self.download_pct,
            "upload_pct": self.upload_pct,
            "updated_at": now_str(),
        }

STATE = EngineState()
STATE.pause_event.set()
STATE.recent_ids |= STORE.processed_ids

# =========================================================
# UI
# =========================================================

def calculate_uptime() -> str:
    if not STATE.metrics.started_at:
        return "0s"
    end = STATE.metrics.ended_at or now_ts()
    return human_seconds(end - STATE.metrics.started_at)

def calculate_eta() -> str:
    m = STATE.metrics
    if m.processed_items <= 0 or STATE.current_page_total_posts <= 0:
        return "--"
    elapsed = max(1, now_ts() - m.started_at)
    avg = elapsed / max(1, m.processed_items)
    remain = max(0, STATE.current_page_total_posts - STATE.current_page_processed)
    return human_seconds(avg * remain)

def overall_page_pct() -> float:
    if STATE.current_page_total_posts <= 0:
        return 0.0
    return (STATE.current_page_processed / STATE.current_page_total_posts) * 100

def build_status_text() -> str:
    m = STATE.metrics
    title = truncate(clean_for_tg(STATE.current_title), 54)
    slug = truncate(clean_for_tg(STATE.current_slug), 40)
    stage = truncate(clean_for_tg(STATE.current_stage), 26)
    err = truncate(clean_for_tg(STATE.current_error), 65)

    text = (
        f"╭── 🎬 **MastiRaja Ultra Control Panel**\n"
        f"├ **Status:** `{STATE.status.upper()}`\n"
        f"├ **Stage:** `{stage}`\n"
        f"├ **Page:** `{STATE.current_page} / {CFG.MAX_PAGES_TO_SCAN}`\n"
        f"├ **Page Queue:** `{STATE.current_page_processed} / {STATE.current_page_total_posts}`\n"
        f"├ **Slug:** `{slug}`\n"
        f"├ **Title:** `{title}`\n"
        f"├ **Overall:** `[{make_progress_bar(overall_page_pct())}] {overall_page_pct():.1f}%`\n"
        f"├ **Download:** `[{make_progress_bar(STATE.download_pct)}] {STATE.download_pct:.1f}%`\n"
        f"├ **Upload:** `[{make_progress_bar(STATE.upload_pct)}] {STATE.upload_pct:.1f}%`\n"
        f"├ **Downloaded:** `{human_bytes(STATE.current_downloaded_bytes)}`\n"
        f"├ **Uploaded:** `{m.uploaded}`\n"
        f"├ **Skipped DB/Search:** `{m.skipped_local_db}/{m.skipped_live_search}`\n"
        f"├ **Fail X/D/U:** `{m.failed_extract}/{m.failed_download}/{m.failed_upload}`\n"
        f"├ **Links Found:** `{m.links_found}`\n"
        f"├ **Total Processed:** `{m.processed_items}`\n"
        f"├ **Uptime:** `{calculate_uptime()}`\n"
        f"├ **ETA:** `{calculate_eta()}`\n"
        f"├ **Last Error:** `{err}`\n"
        f"╰ **Updated:** `{now_str()}`"
    )
    return text

async def update_status_message(force: bool = False):
    if not STATE.status_msg:
        return
    text = build_status_text()
    if not force and text == STATE.last_status_text:
        return
    try:
        await STATE.status_msg.edit_text(text, parse_mode=ParseMode.MARKDOWN)
        STATE.last_status_text = text
        STATE.last_status_update_at = now_ts()
    except Exception:
        pass

async def ui_loop():
    while STATE.running or STATE.paused:
        await update_status_message()
        await asyncio.sleep(CFG.UI_REFRESH_SECONDS)

async def autosave_loop():
    while STATE.running or STATE.paused:
        STORE.save_runtime(STATE.to_runtime_dict())
        STORE.save_failed()
        STORE.save_processed()
        await asyncio.sleep(10)

# =========================================================
# HTTP CLIENT
# =========================================================

class SiteHttpClient:
    def __init__(self):
        self.timeout = ClientTimeout(
            total=CFG.REQUEST_TIMEOUT,
            connect=CFG.CONNECT_TIMEOUT,
            sock_read=CFG.SOCK_READ_TIMEOUT
        )
        self.connector = TCPConnector(
            limit=CFG.CONNECTOR_LIMIT,
            limit_per_host=CFG.CONNECTOR_LIMIT_PER_HOST,
            ssl=False
        )
        self.session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                timeout=self.timeout,
                connector=self.connector,
                headers={"User-Agent": CFG.USER_AGENT}
            )

    async def close(self):
        if self.session and not self.session.closed:
            await self.session.close()

    async def fetch_text(self, url: str) -> Optional[str]:
        await self.start()

        for attempt in range(1, CFG.MAX_RETRIES + 1):
            try:
                async with self.session.get(url) as resp:
                    if resp.status != 200:
                        logger.warning(f"HTTP {resp.status} on {url}")
                        if resp.status in (403, 404):
                            return None
                    return await resp.text()
            except asyncio.TimeoutError:
                logger.warning(f"Timeout [{attempt}/{CFG.MAX_RETRIES}] {url}")
            except Exception as e:
                logger.warning(f"Fetch failed [{attempt}/{CFG.MAX_RETRIES}] {url} -> {e}")
            await asyncio.sleep(sleep_jitter(1.5 * attempt, 0.5))
        return None

    async def fetch_soup(self, url: str) -> Optional[BeautifulSoup]:
        html = await self.fetch_text(url)
        if not html:
            return None
        return BeautifulSoup(html, "html.parser")

HTTP = SiteHttpClient()

# =========================================================
# SCRAPER
# =========================================================

def extract_post_links(soup: BeautifulSoup) -> List[str]:
    links = []
    seen = set()

    articles = soup.find_all("article", class_="thumb-block") or soup.find_all("article")
    logger.info(f"Page par total {len(articles)} posts mile.")

    for article in articles:
        a = article.find("a", href=True)
        if not a:
            continue

        href = a["href"]
        if href.startswith("/"):
            href = urljoin(CFG.BASE_URL, href)

        if "mastiraja.com" in href and href not in seen:
            links.append(href)
            seen.add(href)

    return links

async def extract_video_info(post_url: str) -> Optional[Dict[str, Any]]:
    soup = await HTTP.fetch_soup(post_url)
    if not soup:
        STATE.current_error = "Post HTML fetch failed"
        return None

    post_id = extract_slug_id(post_url)

    title_tag = soup.find("h1")
    title = title_tag.get_text(strip=True) if title_tag else "Untitled"

    category = "Video"
    cat_tag = soup.find("a", rel="category tag") or soup.find("span", class_="category") or soup.find("div", class_="post-categories")
    if cat_tag:
        category = cat_tag.get_text(strip=True)

    tags = []
    found_tags = soup.find_all("a", rel="tag") or soup.find_all("span", class_="tag")
    for tag in found_tags:
        cleaned = re.sub(r"[^a-zA-Z0-9]", "", tag.get_text(strip=True))
        if cleaned:
            tags.append(f"#{cleaned}")
    tags_str = " ".join(tags[:6])

    description = ""
    desc_tag = soup.find("div", class_="entry-content") or soup.find("div", class_="description") or soup.find("p", class_="description")
    if desc_tag:
        p_tags = desc_tag.find_all("p")
        description = "\n".join([p.get_text(strip=True) for p in p_tags[:3]]) if p_tags else desc_tag.get_text(strip=True)
        description = truncate(description, 400)

    video_url = None

    iframe = soup.find("iframe", src=True)
    if iframe:
        src = iframe["src"]
        parsed = urlparse(src)
        qs = parse_qs(parsed.query)
        b64_data = qs.get("q", [""])[0]

        if b64_data:
            try:
                decoded = base64.b64decode(b64_data).decode("utf-8", errors="ignore")
                unquoted = urllib.parse.unquote(decoded)

                match = re.search(r'src=["\'](https?://[^\s"\']+\.(?:mp4|m3u8|webm)[^\s"\']*)["\']', unquoted)
                if not match:
                    match = re.search(r'src=["\'](https://https?://[^"\']+)["\']', unquoted)

                if match:
                    video_url = match.group(1).replace("&amp;", "&")
            except Exception as e:
                logger.warning(f"Video decode failed: {e}")

    if not video_url:
        video_tag = soup.find("video")
        if video_tag:
            video_url = video_tag.get("src")
            if not video_url:
                source = video_tag.find("source")
                if source:
                    video_url = source.get("src")

    if not video_url:
        STATE.current_error = "Playable video source not found"
        return None

    return {
        "post_id": post_id,
        "title": title,
        "video_url": video_url,
        "category": category,
        "tags": tags_str,
        "description": description,
        "source_url": post_url
    }

# =========================================================
# DUPLICATE CHECK
# =========================================================

async def is_already_uploaded(client: Client, slug_id: str) -> bool:
    if not slug_id:
        return False

    if slug_id in STATE.recent_ids or STORE.has_processed(slug_id):
        return True

    for attempt in range(1, 3):
        try:
            async for _ in client.search_messages(
                chat_id=CFG.channel_id(),
                query=slug_id,
                limit=CFG.SEARCH_LIMIT
            ):
                STATE.recent_ids.add(slug_id)
                return True
            return False
        except FloodWait as fw:
            logger.warning(f"FloodWait in duplicate check: {fw.value}s")
            await asyncio.sleep(fw.value)
        except Exception as e:
            logger.warning(f"Duplicate check failed [{attempt}/2] for {slug_id}: {e}")
            await asyncio.sleep(1.5)
    return False

# =========================================================
# DOWNLOADER
# =========================================================

def find_downloaded_media(download_dir: str, prefix: str) -> Tuple[Optional[str], Optional[str]]:
    filepath = None
    thumb = None
    for file in os.listdir(download_dir):
        full = os.path.join(download_dir, file)
        if not file.startswith(prefix):
            continue
        if file.lower().endswith((".jpg", ".jpeg", ".png", ".webp")):
            thumb = full
        else:
            filepath = full
    return filepath, thumb

async def download_video(post_id: str, video_url: str, download_dir: str) -> Optional[Dict[str, Any]]:
    Path(download_dir).mkdir(parents=True, exist_ok=True)

    safe_post_id = safe_filename(post_id, 50)
    outtmpl = os.path.join(download_dir, f"{safe_post_id}_%(title).90s.%(ext)s")

    media = {
        "filepath": None,
        "thumbnail": None,
        "duration": 0,
        "size_bytes": 0
    }

    def progress_hook(d):
        try:
            if d["status"] == "downloading":
                total = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
                downloaded = d.get("downloaded_bytes", 0)
                STATE.current_downloaded_bytes = int(downloaded)
                if total > 0:
                    STATE.download_pct = (downloaded / total) * 100
            elif d["status"] == "finished":
                STATE.download_pct = 100.0
                media["filepath"] = d.get("filename")
        except Exception:
            pass

    ydl_opts = {
        "outtmpl": outtmpl,
        "quiet": True,
        "no_warnings": True,
        "progress_hooks": [progress_hook],
        "writethumbnail": True,
        "noplaylist": True,
        "retries": 3,
        "fragment_retries": 3,
        "continuedl": True,
        "socket_timeout": CFG.REQUEST_TIMEOUT,
        "http_headers": {
            "User-Agent": CFG.USER_AGENT,
            "Referer": CFG.BASE_URL
        }
    }

    try:
        def _download():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                return ydl.extract_info(video_url, download=True)

        info = await asyncio.to_thread(_download)

        if info:
            media["duration"] = int(info.get("duration", 0) or 0)

        filepath, thumb = find_downloaded_media(download_dir, f"{safe_post_id}_")
        media["filepath"] = media["filepath"] or filepath
        media["thumbnail"] = thumb

        if media["filepath"] and os.path.exists(media["filepath"]):
            media["size_bytes"] = os.path.getsize(media["filepath"])
            STATE.metrics.bytes_downloaded_total += media["size_bytes"]
            return media

    except Exception as e:
        logger.error(f"yt-dlp download error for {post_id}: {e}")
        STATE.current_error = truncate(f"Download failed: {e}", 80)

    return None

# =========================================================
# THUMB UTILS
# =========================================================

def prepare_telegram_thumb(path: Optional[str]) -> Optional[str]:
    if not path or not os.path.exists(path):
        return None

    try:
        if path.lower().endswith(".jpg") or path.lower().endswith(".jpeg"):
            if os.path.getsize(path) <= 200 * 1024:
                return path

        from PIL import Image

        img = Image.open(path).convert("RGB")
        img.thumbnail((320, 320))

        out = os.path.join(CFG.TMP_DIR, safe_filename(Path(path).stem, 50) + "_tg.jpg")
        quality = 88
        while quality >= 50:
            img.save(out, "JPEG", quality=quality, optimize=True)
            if os.path.getsize(out) <= 200 * 1024:
                return out
            quality -= 8
    except Exception as e:
        logger.warning(f"Thumb prepare failed: {e}")
    return None

# =========================================================
# CAPTION
# =========================================================

def build_caption(info: Dict[str, Any]) -> str:
    title = clean_for_tg(info.get("title", "Untitled"))
    category = clean_for_tg(info.get("category", "Video"))
    tags = clean_for_tg(info.get("tags", ""))
    post_id = clean_for_tg(info.get("post_id", "-"))
    desc = clean_for_tg(info.get("description", ""))
    source = clean_for_tg(info.get("source_url", ""))

    category_line = f"📂 **Category:** {category}"
    if tags:
        category_line += f" {tags}"

    desc_line = f"\n\n📝 **Description:**\n{desc}" if desc else ""
    source_line = f"\n\n🔗 **Source:** {source}" if source else ""

    caption = (
        f"📹 **Title:** {title}\n"
        f"{category_line}\n"
        f"🆔 **ID:** `{post_id}`"
        f"{desc_line}"
        f"{source_line}"
    )
    return truncate(caption, CFG.MAX_CAPTION_LENGTH)

# =========================================================
# UPLOADER
# =========================================================

async def upload_video(client: Client, media_info: Dict[str, Any], info: Dict[str, Any]) -> bool:
    filepath = media_info.get("filepath")
    duration = int(media_info.get("duration", 0) or 0)
    thumb_path = media_info.get("thumbnail")

    if not filepath or not os.path.exists(filepath):
        STATE.current_error = "Upload file missing"
        return False

    thumb_to_pass = prepare_telegram_thumb(thumb_path)
    caption = build_caption(info)

    async def progress(current, total):
        if total > 0:
            STATE.current_upload_bytes = int(current)
            STATE.upload_pct = (current / total) * 100

    send_kwargs = {
        "chat_id": CFG.channel_id(),
        "video": filepath,
        "caption": caption,
        "parse_mode": ParseMode.MARKDOWN,
        "supports_streaming": True,
        "progress": progress
    }

    if duration > 0:
        send_kwargs["duration"] = duration

    if thumb_to_pass and os.path.exists(thumb_to_pass):
        send_kwargs["thumb"] = thumb_to_pass

    STATE.metrics.upload_attempts += 1

    try:
        await client.send_video(**send_kwargs)
        STATE.upload_pct = 100.0
        return True
    except FloodWait as fw:
        logger.warning(f"FloodWait upload: sleeping {fw.value}s")
        await asyncio.sleep(fw.value)
        try:
            await client.send_video(**send_kwargs)
            STATE.upload_pct = 100.0
            return True
        except Exception as e:
            logger.error(f"Upload retry failed: {e}")
            STATE.current_error = truncate(f"Upload retry failed: {e}", 80)
            return False
    except RPCError as e:
        logger.error(f"Telegram RPC upload error: {e}")
        STATE.current_error = truncate(f"RPC upload error: {e}", 80)
        return False
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        STATE.current_error = truncate(f"Upload failed: {e}", 80)
        return False
    finally:
        if thumb_to_pass and thumb_to_pass != thumb_path and os.path.exists(thumb_to_pass):
            try:
                os.remove(thumb_to_pass)
            except Exception:
                pass

# =========================================================
# CLEANUP
# =========================================================

def cleanup_media_files(media_info: Optional[Dict[str, Any]], keep: bool = False):
    if not media_info or keep:
        return
    for key in ("filepath", "thumbnail"):
        fp = media_info.get(key)
        if fp and os.path.exists(fp):
            try:
                os.remove(fp)
            except Exception:
                pass

def cleanup_tmp_dir():
    tmp = Path(CFG.TMP_DIR)
    if not tmp.exists():
        return
    for item in tmp.iterdir():
        try:
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item, ignore_errors=True)
        except Exception:
            pass

# =========================================================
# PROCESSOR
# =========================================================

async def process_target(client: Client, target_url: str):
    await STATE.pause_event.wait()
    if STATE.stopping:
        return

    slug_id = extract_slug_id(target_url)
    STATE.current_slug = slug_id
    STATE.current_url = target_url
    STATE.current_title = "Checking..."
    STATE.current_error = "None"
    STATE.download_pct = 0.0
    STATE.upload_pct = 0.0
    STATE.current_downloaded_bytes = 0
    STATE.current_upload_bytes = 0

    STATE.current_stage = "Live Duplicate Check"
    await update_status_message()

    if STORE.has_processed(slug_id):
        STATE.metrics.skipped_local_db += 1
        logger.info(f"Skipped from local DB: {slug_id}")
        return

    if await is_already_uploaded(client, slug_id):
        STATE.metrics.skipped_live_search += 1
        STORE.add_processed(slug_id)
        logger.info(f"Skipped from Telegram search: {slug_id}")
        return

    STATE.current_stage = "Extracting Post Info"
    await update_status_message()

    info = await extract_video_info(target_url)
    if not info:
        STATE.metrics.failed_extract += 1
        STORE.add_failed({
            "type": "extract",
            "slug_id": slug_id,
            "url": target_url,
            "time": now_str(),
            "error": STATE.current_error
        })
        return

    STATE.current_title = info.get("title", "Untitled")

    STATE.current_stage = "Downloading Media"
    await update_status_message()

    media_info = await download_video(info["post_id"], info["video_url"], CFG.DOWNLOAD_DIR)
    if not media_info or not media_info.get("filepath"):
        STATE.metrics.failed_download += 1
        STORE.add_failed({
            "type": "download",
            "slug_id": slug_id,
            "url": target_url,
            "time": now_str(),
            "error": STATE.current_error
        })
        cleanup_media_files(media_info, keep=CFG.KEEP_FILES_ON_FAIL)
        return

    STATE.current_stage = "Uploading to Telegram"
    await update_status_message()

    success = await upload_video(client, media_info, info)
    if success:
        STATE.metrics.uploaded += 1
        STORE.add_processed(info["post_id"])
        STATE.recent_ids.add(info["post_id"])
        logger.info(f"Uploaded: {info['post_id']}")
    else:
        STATE.metrics.failed_upload += 1
        STORE.add_failed({
            "type": "upload",
            "slug_id": slug_id,
            "url": target_url,
            "time": now_str(),
            "error": STATE.current_error
        })

    cleanup_media_files(media_info, keep=(CFG.KEEP_FILES_ON_FAIL and not success))

# =========================================================
# ENGINE
# =========================================================

async def crawl_and_process(client: Client):
    async with STATE.lock:
        if STATE.running:
            return
        STATE.running = True
        STATE.paused = False
        STATE.stopping = False
        STATE.status = "running"
        STATE.pause_event.set()
        STATE.reset_progress_fields()
        STATE.reset_metrics()

    STATE.ui_task = asyncio.create_task(ui_loop())
    STATE.autosave_task = asyncio.create_task(autosave_loop())

    try:
        await HTTP.start()

        while STATE.current_page <= CFG.MAX_PAGES_TO_SCAN and STATE.running and not STATE.stopping:
            await STATE.pause_event.wait()

            page_url = CFG.BASE_URL if STATE.current_page == 1 else f"{CFG.BASE_URL}/page/{STATE.current_page}/"
            STATE.current_stage = "Scraping Page"
            STATE.current_title = f"Page {STATE.current_page}"
            STATE.current_slug = "-"
            STATE.current_error = "None"
            STATE.current_page_total_posts = 0
            STATE.current_page_processed = 0

            logger.info(f"Scanning page {STATE.current_page}: {page_url}")
            await update_status_message()

            soup = await HTTP.fetch_soup(page_url)
            if not soup:
                STATE.current_error = f"Page fetch failed for page {STATE.current_page}"
                logger.warning(STATE.current_error)
                break

            links = extract_post_links(soup)
            if not links:
                STATE.current_error = f"No posts found on page {STATE.current_page}"
                logger.warning(STATE.current_error)
                break

            STATE.metrics.pages_scanned += 1
            STATE.metrics.links_found += len(links)
            STATE.current_page_total_posts = len(links)

            for i, target_url in enumerate(links, start=1):
                await STATE.pause_event.wait()
                if not STATE.running or STATE.stopping:
                    break

                STATE.current_page_processed = i
                STATE.metrics.processed_items += 1

                try:
                    await process_target(client, target_url)
                except Exception as e:
                    logger.exception(f"Unexpected processing error: {e}")
                    STATE.current_error = truncate(f"Unexpected: {e}", 80)
                    STORE.add_failed({
                        "type": "unexpected",
                        "slug_id": extract_slug_id(target_url),
                        "url": target_url,
                        "time": now_str(),
                        "error": STATE.current_error
                    })

                if STATE.metrics.processed_items % CFG.SAVE_DB_EVERY == 0:
                    STORE.save_processed()
                    STORE.save_failed()
                    STORE.save_runtime(STATE.to_runtime_dict())

                await update_status_message()
                await asyncio.sleep(sleep_jitter(CFG.ITEM_DELAY, 0.35))

            if not STATE.running or STATE.stopping:
                break

            STATE.current_page += 1
            await asyncio.sleep(sleep_jitter(CFG.PAGE_DELAY, 0.4))

        STATE.current_stage = "Finished"
        STATE.status = "stopped"
        STATE.running = False
        STATE.paused = False
        STATE.stopping = False
        STATE.metrics.ended_at = now_ts()
        await update_status_message(force=True)

    finally:
        STORE.save_processed()
        STORE.save_failed()
        STORE.save_runtime(STATE.to_runtime_dict())
        cleanup_tmp_dir()
        await HTTP.close()

        if STATE.ui_task:
            STATE.ui_task.cancel()
        if STATE.autosave_task:
            STATE.autosave_task.cancel()

# =========================================================
# COMMANDS
# =========================================================

async def cmd_starttask(client: Client, message: Message):
    if STATE.running:
        await message.reply("⚠️ **Crawler already running.**", parse_mode=ParseMode.MARKDOWN)
        return

    args = message.text.split()
    if len(args) > 1 and args[1].isdigit():
        STATE.current_page = max(1, int(args[1]))
    else:
        saved_page = STORE.get_setting("last_start_page", 1)
        STATE.current_page = max(1, int(saved_page))

    STORE.set_setting("last_start_page", STATE.current_page)

    STATE.status_msg = await message.reply(
        f"🚀 **Ultra crawler initialized from page {STATE.current_page}.**\n"
        f"`/pause` `/resume` `/status` `/stats` `/stop` `/help`",
        parse_mode=ParseMode.MARKDOWN
    )

    STATE.worker_task = asyncio.create_task(crawl_and_process(client))

async def cmd_stop(client: Client, message: Message):
    if not STATE.running and not STATE.paused:
        await message.reply("⏹️ **Crawler is not active.**", parse_mode=ParseMode.MARKDOWN)
        return

    STATE.stopping = True
    STATE.running = False
    STATE.paused = False
    STATE.status = "stopping"
    STATE.pause_event.set()

    await message.reply(
        f"🛑 **Stopping crawler...**\nCurrent page: `{STATE.current_page}`",
        parse_mode=ParseMode.MARKDOWN
    )

async def cmd_pause(client: Client, message: Message):
    if not STATE.running:
        await message.reply("⚠️ **Crawler is not running.**", parse_mode=ParseMode.MARKDOWN)
        return
    if STATE.paused:
        await message.reply("⏸️ **Crawler already paused.**", parse_mode=ParseMode.MARKDOWN)
        return

    STATE.paused = True
    STATE.running = False
    STATE.status = "paused"
    STATE.pause_event.clear()

    await update_status_message(force=True)
    await message.reply("⏸️ **Crawler paused successfully.**", parse_mode=ParseMode.MARKDOWN)

async def cmd_resume(client: Client, message: Message):
    if not STATE.paused:
        await message.reply("▶️ **Crawler is not paused.**", parse_mode=ParseMode.MARKDOWN)
        return

    STATE.paused = False
    STATE.running = True
    STATE.status = "running"
    STATE.pause_event.set()

    await update_status_message(force=True)
    await message.reply("▶️ **Crawler resumed.**", parse_mode=ParseMode.MARKDOWN)

async def cmd_ping(client: Client, message: Message):
    await message.reply("🏓 **Pong! Ultra bot online.**", parse_mode=ParseMode.MARKDOWN)

async def cmd_status(client: Client, message: Message):
    await message.reply(build_status_text(), parse_mode=ParseMode.MARKDOWN)

async def cmd_stats(client: Client, message: Message):
    m = STATE.metrics
    txt = (
        f"📊 **Crawler Metrics**\n"
        f"• Status: `{STATE.status}`\n"
        f"• Pages scanned: `{m.pages_scanned}`\n"
        f"• Links found: `{m.links_found}`\n"
        f"• Processed items: `{m.processed_items}`\n"
        f"• Uploaded: `{m.uploaded}`\n"
        f"• Skipped local DB: `{m.skipped_local_db}`\n"
        f"• Skipped live search: `{m.skipped_live_search}`\n"
        f"• Failed extract: `{m.failed_extract}`\n"
        f"• Failed download: `{m.failed_download}`\n"
        f"• Failed upload: `{m.failed_upload}`\n"
        f"• Upload attempts: `{m.upload_attempts}`\n"
        f"• Downloaded total: `{human_bytes(m.bytes_downloaded_total)}`\n"
        f"• Current page: `{STATE.current_page}`\n"
        f"• Current stage: `{clean_for_tg(STATE.current_stage)}`\n"
        f"• Uptime: `{calculate_uptime()}`"
    )
    await message.reply(txt, parse_mode=ParseMode.MARKDOWN)

async def cmd_setpage(client: Client, message: Message):
    args = message.text.split()
    if len(args) < 2 or not args[1].isdigit():
        await message.reply("Usage: `/setpage 4`", parse_mode=ParseMode.MARKDOWN)
        return
    if STATE.running or STATE.paused:
        await message.reply("⚠️ Stop crawler before changing page.", parse_mode=ParseMode.MARKDOWN)
        return

    STATE.current_page = max(1, int(args[1]))
    STORE.set_setting("last_start_page", STATE.current_page)
    await message.reply(f"✅ Next start page set to `{STATE.current_page}`", parse_mode=ParseMode.MARKDOWN)

async def cmd_failed(client: Client, message: Message):
    items = STORE.failed_items[-10:]
    if not items:
        await message.reply("✅ No failed items logged.", parse_mode=ParseMode.MARKDOWN)
        return

    lines = ["⚠️ **Last Failed Items**"]
    for item in items:
        lines.append(
            f"• `{clean_for_tg(item.get('type', '?'))}` | "
            f"`{clean_for_tg(item.get('slug_id', '-'))}` | "
            f"`{truncate(clean_for_tg(item.get('error', '-')), 35)}`"
        )
    await message.reply("\n".join(lines), parse_mode=ParseMode.MARKDOWN)

async def cmd_clearfailed(client: Client, message: Message):
    STORE.failed_items = []
    STORE.save_failed()
    await message.reply("🧹 **Failed log cleared.**", parse_mode=ParseMode.MARKDOWN)

async def cmd_help(client: Client, message: Message):
    txt = (
        f"🤖 **Ultra Command Menu**\n"
        f"• `/starttask` - Start from saved page\n"
        f"• `/starttask 3` - Start from page 3\n"
        f"• `/pause` - Pause crawler\n"
        f"• `/resume` - Resume crawler\n"
        f"• `/stop` - Stop crawler\n"
        f"• `/status` - Live status snapshot\n"
        f"• `/stats` - Full metrics\n"
        f"• `/setpage 5` - Set next start page\n"
        f"• `/failed` - Show recent failed items\n"
        f"• `/clearfailed` - Clear failed log\n"
        f"• `/ping` - Health check\n"
        f"• `/help` - Show this help"
    )
    await message.reply(txt, parse_mode=ParseMode.MARKDOWN)

# =========================================================
# HTTP SERVER
# =========================================================

async def start_http_server():
    app = web.Application()

    async def root(_):
        return web.Response(text="OK")

    async def health(_):
        return web.json_response({
            "status": STATE.status,
            "running": STATE.running,
            "paused": STATE.paused,
            "stopping": STATE.stopping,
            "current_page": STATE.current_page,
            "current_stage": STATE.current_stage,
            "updated_at": now_str(),
        })

    async def metrics(_):
        m = STATE.metrics
        return web.json_response({
            "pages_scanned": m.pages_scanned,
            "links_found": m.links_found,
            "processed_items": m.processed_items,
            "uploaded": m.uploaded,
            "skipped_local_db": m.skipped_local_db,
            "skipped_live_search": m.skipped_live_search,
            "failed_extract": m.failed_extract,
            "failed_download": m.failed_download,
            "failed_upload": m.failed_upload,
            "uptime": calculate_uptime(),
        })

    app.router.add_get("/", root)
    app.router.add_get("/health", health)
    app.router.add_get("/metrics", metrics)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CFG.PORT)
    await site.start()
    logger.info(f"HTTP server started on port {CFG.PORT}")

# =========================================================
# SIGNALS / SHUTDOWN
# =========================================================

def setup_signal_handlers():
    try:
        loop = asyncio.get_running_loop()

        def _handle():
            logger.info("Shutdown signal received")
            STATE.stopping = True
            STATE.running = False
            STATE.pause_event.set()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _handle)
            except NotImplementedError:
                pass
    except Exception:
        pass

# =========================================================
# MAIN
# =========================================================

async def main():
    CFG.validate()
    setup_signal_handlers()

    await start_http_server()

    app = Client(
        "mastiraja_userbot_ultra",
        api_id=CFG.API_ID,
        api_hash=CFG.API_HASH,
        session_string=CFG.STRING_SESSION,
        in_memory=True
    )

    @app.on_message(filters.me & filters.command([
        "starttask", "pause", "resume", "stop", "status",
        "stats", "setpage", "failed", "clearfailed", "ping", "help"
    ]))
    async def command_router(client: Client, message: Message):
        text = message.text or ""
        cmd = text.split()[0].split("@")[0].lower()

        if cmd == "/starttask":
            await cmd_starttask(client, message)
        elif cmd == "/pause":
            await cmd_pause(client, message)
        elif cmd == "/resume":
            await cmd_resume(client, message)
        elif cmd == "/stop":
            await cmd_stop(client, message)
        elif cmd == "/status":
            await cmd_status(client, message)
        elif cmd == "/stats":
            await cmd_stats(client, message)
        elif cmd == "/setpage":
            await cmd_setpage(client, message)
        elif cmd == "/failed":
            await cmd_failed(client, message)
        elif cmd == "/clearfailed":
            await cmd_clearfailed(client, message)
        elif cmd == "/ping":
            await cmd_ping(client, message)
        elif cmd == "/help":
            await cmd_help(client, message)

    await app.start()
    logger.info("✨ Ultra userbot online. Use /help")
    logger.info(f"Target channel: {CFG.channel_id()}")

    if CFG.AUTO_START:
        logger.info("AUTO_START enabled")
        STATE.current_page = max(1, int(STORE.get_setting("last_start_page", 1)))
        STATE.worker_task = asyncio.create_task(crawl_and_process(app))

    await asyncio.Event().wait()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Stopped by KeyboardInterrupt")
