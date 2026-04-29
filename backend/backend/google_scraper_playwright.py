"""
Google Shorts Scraper - Playwright 버전
Headless Chromium으로 JS 렌더링 후 HTML 추출 (구글 봇 감지 우회)
fetch 부분만 다르고 나머지는 google_scraper.py와 동일
"""

from __future__ import annotations

import os
import sys
import time
import random
import logging
import urllib.parse
from pathlib import Path
from dataclasses import dataclass, asdict
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from supabase import create_client, Client
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, LOG_LEVEL

logging.basicConfig(
    level=getattr(logging, str(LOG_LEVEL).upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(message)s"
)

# ------------------------------------------------------
# Constants
# ------------------------------------------------------
KST = timezone(timedelta(hours=9))

SOURCE_FILTERS = {
    "youtube":   "H4sIAAAAAAAAAB2KQQrAMAjAftNLoX-ynXTirGCVsd-veEtI6qfh0bENlcKiaybR2g7TQNKc2JUTL8Bxx7nUGluxeOjFnukHCnw2uEsAAAA",
    "instagram": "H4sIAAAAAAAAANPOzCsuSUwvSszVS87PVavMLy0pTUoFs7Nz8_1PSwaySzOyS_1GwwswJMFqWmpGSWgJkAUMF7WEEAAAA",
    "tiktok":    "H4sIAAAAAAAAANMuycwuyc_1WS87PVcvMKy5JTC9KzAXzKvNLS0qTUsHsotSUlMwSMDMtMTk1KR-qowJMAgAtsZu8RAAAAA",
    "all":       None,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

GOOGLE_SEARCH = "https://www.google.com/search"


@dataclass
class ShortItem:
    platform: str
    platform_id: str
    region: str
    title: str
    nickname: Optional[str] = None
    avatar: Optional[str] = None
    thumbnail: Optional[str] = None
    video_url: Optional[str] = None
    description: Optional[str] = None
    likes: Optional[int] = None
    views: Optional[int] = None
    comments: Optional[int] = None
    published_at: Optional[str] = None
    source: str = "google"
    category: str = "all"
    keyword: Optional[str] = None


def utc_iso(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ------------------------------------------------------
# Schedule / Keyword (기존과 동일)
# ------------------------------------------------------
def get_today_schedule(sb: Client, account: str) -> Optional[Dict]:
    weekday = datetime.now(KST).weekday()
    res = (
        sb.table("scraper_schedule")
        .select("*")
        .eq("account", account)
        .eq("weekday", weekday)
        .limit(1)
        .execute()
    )
    if not res.data:
        return None
    return res.data[0]


def get_active_keywords(sb: Client, region: str) -> Dict[str, List[str]]:
    res = (
        sb.table("keywords")
        .select("keyword, is_fixed")
        .eq("region", region)
        .eq("is_active", True)
        .execute()
    )
    fixed = [r["keyword"] for r in res.data if r["is_fixed"]]
    normal = [r["keyword"] for r in res.data if not r["is_fixed"]]
    return {"fixed": fixed, "normal": normal}


def get_recent_keywords(sb: Client, region: str, days: int = 3) -> set[str]:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    res = (
        sb.table("shorts_items")
        .select("keyword")
        .eq("region", region)
        .gte("crawled_at", since)
        .execute()
    )
    return {r["keyword"] for r in res.data if r.get("keyword")}


def pick_keywords(mode, fixed, normal, recent):
    available = [k for k in normal if k not in recent]
    if not available:
        available = normal
        logging.warning("All normal keywords used recently, ignoring filter")

    result = []
    if mode == "full":
        if len(available) < 4:
            picks = available[:]
            random.shuffle(picks)
        else:
            picks = random.sample(available, 4)
        sources = ["youtube", "instagram", "tiktok", "all"]
        random.shuffle(sources)
        for kw, src in zip(picks, sources):
            result.append({"keyword": kw, "source_filter": src})
        for fk in fixed:
            result.append({"keyword": fk, "source_filter": "all"})
    elif mode == "light":
        if available:
            picks = random.sample(available, 1)
            src = random.choice(["youtube", "instagram", "tiktok"])
            result.append({"keyword": picks[0], "source_filter": src})
        for fk in fixed:
            result.append({"keyword": fk, "source_filter": "all"})
    return result


def build_search_url(keyword: str, lang: str, source: str) -> str:
    params = {
        "q": keyword,
        "num": 12,
        "udm": 39,
        "hl": lang,
    }
    srcf = SOURCE_FILTERS.get(source)
    if srcf:
        params["tbs"] = f"qdr:w,srcf:{srcf}"
    else:
        params["tbs"] = "qdr:w"
    return f"{GOOGLE_SEARCH}?{urllib.parse.urlencode(params)}"


# ------------------------------------------------------
# Playwright Browser (재사용)
# ------------------------------------------------------
class PlaywrightFetcher:
    def __init__(self):
        self.pw = None
        self.browser = None
        self.context = None

    def __enter__(self):
        self.pw = sync_playwright().start()
        self.browser = self.pw.chromium.launch(
            headless=True,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
        self.context = self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            locale="ko-KR",
            timezone_id="Asia/Seoul",
            viewport={"width": 1280, "height": 800},
        )
        # webdriver 흔적 제거
        self.context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        return self

    def __exit__(self, *args):
        try:
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.pw:
                self.pw.stop()
        except Exception:
            pass

    def fetch(self, url: str) -> str:
        page = self.context.new_page()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            # 결과 카드 또는 본문 로드 대기 (10초)
            try:
                page.wait_for_selector("div.MYHjcd, div#search, div#main", timeout=10000)
            except PlaywrightTimeoutError:
                logging.warning("selector wait timeout for %s", url)
            # 동적 컨텐츠 추가 로드 대기
            page.wait_for_timeout(2000)
            html = page.content()
            return html
        finally:
            page.close()


# ------------------------------------------------------
# Parse / Detect / Extract (기존과 동일)
# ------------------------------------------------------
def detect_platform(url: str) -> str:
    if "youtube.com" in url or "youtu.be" in url:
        return "youtube"
    if "instagram.com" in url:
        return "instagram"
    if "tiktok.com" in url:
        return "tiktok"
    return "other"


def extract_platform_id(url: str, platform: str) -> Optional[str]:
    try:
        if platform == "youtube":
            if "/shorts/" in url:
                return url.split("/shorts/")[1].split("?")[0].split("/")[0]
            if "watch?v=" in url:
                return urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("v", [None])[0]
            if "youtu.be/" in url:
                return url.split("youtu.be/")[1].split("?")[0]
        elif platform == "instagram":
            parts = urllib.parse.urlparse(url).path.strip("/").split("/")
            if len(parts) >= 2:
                return parts[1]
        elif platform == "tiktok":
            if "/video/" in url:
                return url.split("/video/")[1].split("?")[0].split("/")[0]
    except Exception as e:
        logging.warning("extract_platform_id failed url=%s err=%s", url, e)
    return None


def parse_results(html: str, keyword: str, region: str) -> List[ShortItem]:
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_ids = set()

    for a in soup.select("a[href]"):
        href = a.get("href", "")
        if href.startswith("/url?"):
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(href).query)
            href = qs.get("q", [""])[0] or qs.get("url", [""])[0]
        if not href.startswith("http"):
            continue

        platform = detect_platform(href)
        if platform == "other":
            continue

        pid = extract_platform_id(href, platform)
        if not pid:
            continue

        key = (platform, pid)
        if key in seen_ids:
            continue
        seen_ids.add(key)

        title = a.get_text(strip=True) or ""
        if not title:
            parent = a.find_parent()
            if parent:
                title = parent.get_text(strip=True)[:200]

        thumb = None
        img = a.find("img")
        if img:
            thumb = img.get("src") or img.get("data-src")
        if platform == "youtube" and not thumb and pid:
            thumb = f"https://i.ytimg.com/vi/{pid}/hqdefault.jpg"

        items.append(ShortItem(
            platform=platform,
            platform_id=pid,
            region=region,
            title=title or keyword,
            thumbnail=thumb,
            video_url=href,
            keyword=keyword,
            source="google",
            category="all",
        ))

        if len(items) >= 12:
            break

    return items


# ------------------------------------------------------
# Supabase Writer
# ------------------------------------------------------
class SupabaseWriter:
    def __init__(self, sb: Client):
        self.sb = sb

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
    def upsert_batch(self, items: List[ShortItem]) -> int:
        if not items:
            return 0
        rows = []
        for it in items:
            d = asdict(it)
            d["crawled_at"] = utc_iso()
            rows.append(d)
        self.sb.table("shorts_items").upsert(
            rows, on_conflict="platform,platform_id"
        ).execute()
        logging.info("Upserted %d rows", len(rows))
        return len(rows)


# ------------------------------------------------------
# Main
# ------------------------------------------------------
def main():
    account = os.getenv("ACCOUNT_NAME")
    extra_keyword = os.getenv("EXTRA_KEYWORD", "").strip()

    if not account:
        logging.error("ACCOUNT_NAME env var not set")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    writer = SupabaseWriter(sb)

    sched = get_today_schedule(sb, account)
    if not sched:
        logging.warning("No schedule for account=%s today", account)
        return

    mode = sched["mode"]
    region = sched["region"]
    lang = sched["lang"]
    sleep_min = float(sched["sleep_min"])
    sleep_max = float(sched["sleep_max"])

    logging.info("[PLAYWRIGHT] account=%s mode=%s region=%s lang=%s sleep=%.3f~%.3f",
                 account, mode, region, lang, sleep_min, sleep_max)

    if mode == "off":
        logging.info("Mode is off today. Exiting.")
        return

    pool = get_active_keywords(sb, region)
    recent = get_recent_keywords(sb, region, days=3)
    tasks = pick_keywords(mode, pool["fixed"], pool["normal"], recent)

    if extra_keyword:
        tasks.append({"keyword": extra_keyword, "source_filter": "all"})
        logging.info("Extra keyword added: %s", extra_keyword)

    if not tasks:
        logging.warning("No keywords picked. Exiting.")
        return

    total = 0
    with PlaywrightFetcher() as fetcher:
        for i, task in enumerate(tasks):
            kw = task["keyword"]
            src = task["source_filter"]

            sleep_sec = random.uniform(sleep_min, sleep_max)
            logging.info("[%d/%d] sleeping %.3fs before kw=%s src=%s",
                         i + 1, len(tasks), sleep_sec, kw, src)
            time.sleep(sleep_sec)

            url = build_search_url(kw, lang, src)
            try:
                html = fetcher.fetch(url)
                logging.info("DEBUG kw=%s html_len=%d yt=%d ig=%d tt=%d MYHjcd=%d",
                             kw, len(html),
                             html.count('youtube.com'),
                             html.count('instagram.com'),
                             html.count('tiktok.com'),
                             html.count('MYHjcd'))
                items = parse_results(html, kw, region)

                if not items and src != "all":
                    logging.warning("Empty result with src=%s, falling back to all", src)
                    fb_sleep = random.uniform(sleep_min, sleep_max)
                    time.sleep(fb_sleep)
                    fb_url = build_search_url(kw, lang, "all")
                    fb_html = fetcher.fetch(fb_url)
                    items = parse_results(fb_html, kw, region)
                    logging.info("fallback parsed=%d", len(items))

                count = writer.upsert_batch(items)
                total += count
                logging.info("kw=%s src=%s parsed=%d upserted=%d",
                             kw, src, len(items), count)
            except Exception as e:
                logging.error("Failed kw=%s src=%s err=%s", kw, src, e)

    logging.info("Done. Total upserted: %d", total)


if __name__ == "__main__":
    main()
