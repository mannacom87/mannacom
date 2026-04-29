"""
Google Shorts Scraper
- Supabase scraper_schedule 테이블에서 오늘 모드 조회
- 활성 키워드 풀에서 사용 키워드 선정 (최근 3일 제외)
- 구글 숏폼 탭(udm=39) 검색 결과 파싱
- shorts_items 테이블에 적재
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

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from supabase import create_client, Client

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

# 출처 필터 (구글 udm=39 숏폼 탭의 srcf 파라미터)
SOURCE_FILTERS = {
    "youtube":   "H4sIAAAAAAAAAB2KQQrAMAjAftNLoX-ynXTirGCVsd-veEtI6qfh0bENlcKiaybR2g7TQNKc2JUTL8Bxx7nUGluxeOjFnukHCnw2uEsAAAA",
    "instagram": "H4sIAAAAAAAAANPOzCsuSUwvSszVS87PVavMLy0pTUoFs7Nz8_1PSwaySzOyS_1GwwswJMFqWmpGSWgJkAUMF7WEEAAAA",
    "tiktok":    "H4sIAAAAAAAAANMuycwuyc_1WS87PVcvMKy5JTC9KzAXzKvNLS0qTUsHsotSUlMwSMDMtMTk1KR-qowJMAgAtsZu8RAAAAA",
    "all":       None,  # 출처 필터 없음
}

# 일반적인 데스크톱 User-Agent 풀
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
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
# Schedule Loader
# ------------------------------------------------------
def get_today_schedule(sb: Client, account: str) -> Optional[Dict]:
    """오늘(KST) 요일 기준 스케줄 조회"""
    weekday = datetime.now(KST).weekday()  # 0=월 ~ 6=일
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


# ------------------------------------------------------
# Keyword Picker
# ------------------------------------------------------
def get_active_keywords(sb: Client, region: str) -> Dict[str, List[str]]:
    """활성 키워드를 fixed/normal로 분리 반환"""
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
    """최근 N일간 사용된 키워드 집합 반환"""
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    res = (
        sb.table("shorts_items")
        .select("keyword")
        .eq("region", region)
        .gte("crawled_at", since)
        .execute()
    )
    return {r["keyword"] for r in res.data if r.get("keyword")}


def pick_keywords(
    mode: str,
    fixed: List[str],
    normal: List[str],
    recent: set[str],
) -> List[Dict]:
    """
    모드에 따라 (keyword, source_filter) 매핑 리스트 반환
    - full: 일반 4 + 챌린지 1 (출처: YT/IG/TT/all + all)
    - light: 일반 1 + 챌린지 1 (출처: 랜덤 1 + all)
    """
    available = [k for k in normal if k not in recent]
    if not available:
        # 최근 3일 안에 다 썼으면 그냥 풀에서 다시 뽑음
        available = normal
        logging.warning("All normal keywords used in recent days, ignoring filter")

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

        # 챌린지
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


# ------------------------------------------------------
# URL Builder
# ------------------------------------------------------
def build_search_url(keyword: str, lang: str, source: str) -> str:
    params = {
        "q": keyword,
        "num": 12,
        "udm": 39,        # 숏폼 비디오 탭
        "hl": lang,
    }
    srcf = SOURCE_FILTERS.get(source)
    if srcf:
        params["tbs"] = f"qdr:w,srcf:{srcf}"
    else:
        params["tbs"] = "qdr:w"

    return f"{GOOGLE_SEARCH}?{urllib.parse.urlencode(params)}"


# ------------------------------------------------------
# Scraper
# ------------------------------------------------------
@retry(stop=stop_after_attempt(2), wait=wait_exponential(min=2, max=10), reraise=True)
def fetch_html(url: str) -> str:
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "ko,en;q=0.9,zh-CN;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    r = httpx.get(url, headers=headers, timeout=20.0, follow_redirects=True)
    r.raise_for_status()
    return r.text


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
            # /reel/ABC123/ 또는 /p/ABC123/
            parts = urllib.parse.urlparse(url).path.strip("/").split("/")
            if len(parts) >= 2:
                return parts[1]
        elif platform == "tiktok":
            # /@user/video/12345
            if "/video/" in url:
                return url.split("/video/")[1].split("?")[0].split("/")[0]
    except Exception as e:
        logging.warning("extract_platform_id failed url=%s err=%s", url, e)
    return None


def parse_results(html: str, keyword: str, region: str) -> List[ShortItem]:
    """구글 숏폼 검색 결과 파싱 → ShortItem 리스트"""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    seen_ids = set()

    # 카드 컨테이너 단위로 순회
    cards = soup.select("div.MYHjcd, div.Z1YvVd")
    if not cards:
        # fallback: 전체 페이지에서 a[href] 훑기
        cards = [soup]

    for card in cards:
        # URL 추출 (카드 안의 첫 a[href])
        a = card.find("a", href=True)
        if not a:
            continue
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

        # 제목
        title_el = card.select_one("span.Yt787")
        title = title_el.get_text(strip=True) if title_el else ""

        # 계정명 (span.E51IV 안의 마지막 span.jSLaVc)
        nickname = None
        ev = card.select_one("span.E51IV")
        if ev:
            ns = ev.select("span.jSLaVc")
            if ns:
                nickname = ns[-1].get_text(strip=True)

        # 썸네일
        thumb = None
        img = card.select_one("div.kSFuOd img[src]")
        if img:
            src = img.get("src", "")
            if src.startswith("http"):
                thumb = src
        if platform == "youtube" and not thumb and pid:
            thumb = f"https://i.ytimg.com/vi/{pid}/hqdefault.jpg"

        # 제목이 비어있으면 aria-label에서 추출 시도
        if not title:
            aria = a.get("aria-label", "") or ""
            if aria:
                title = aria[:200]

        items.append(ShortItem(
            platform=platform,
            platform_id=pid,
            region=region,
            title=title or keyword,
            nickname=nickname,
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
            rows, on_conflict="platform,platform_id,source"
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

    # 1. 오늘 스케줄 조회
    sched = get_today_schedule(sb, account)
    if not sched:
        logging.warning("No schedule for account=%s today", account)
        return

    mode = sched["mode"]
    region = sched["region"]
    lang = sched["lang"]
    sleep_min = float(sched["sleep_min"])
    sleep_max = float(sched["sleep_max"])

    logging.info("account=%s mode=%s region=%s lang=%s sleep=%.3f~%.3f",
                 account, mode, region, lang, sleep_min, sleep_max)

    if mode == "off":
        logging.info("Mode is off today. Exiting.")
        return

    # 2. 키워드 선정
    pool = get_active_keywords(sb, region)
    recent = get_recent_keywords(sb, region, days=3)
    tasks = pick_keywords(mode, pool["fixed"], pool["normal"], recent)

    # 3. 추가 키워드 (수동 입력)
    if extra_keyword:
        tasks.append({"keyword": extra_keyword, "source_filter": "all"})
        logging.info("Extra keyword added: %s", extra_keyword)

    if not tasks:
        logging.warning("No keywords picked. Exiting.")
        return

    # 4. 검색 실행
    total = 0
    for i, task in enumerate(tasks):
        kw = task["keyword"]
        src = task["source_filter"]

        # 첫 검색 전에도 슬립
        sleep_sec = random.uniform(sleep_min, sleep_max)
        logging.info("[%d/%d] sleeping %.3fs before kw=%s src=%s",
                     i + 1, len(tasks), sleep_sec, kw, src)
        time.sleep(sleep_sec)

        url = build_search_url(kw, lang, src)
        try:
            html = fetch_html(url)
            items = parse_results(html, kw, region)

            # Fallback: srcf 적용했는데 결과 0개면 srcf 제거하고 재시도
            if not items and src != "all":
                logging.warning("Empty result with src=%s, falling back to all", src)
                fb_sleep = random.uniform(sleep_min, sleep_max)
                logging.info("fallback sleeping %.3fs", fb_sleep)
                time.sleep(fb_sleep)
                fb_url = build_search_url(kw, lang, "all")
                fb_html = fetch_html(fb_url)
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
