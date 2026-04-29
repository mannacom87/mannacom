"""
YouTube View Updater
- google_scraper로 수집된 영상 중 조회수가 비어있는 것을 채움
- 본인 region 한정으로 동작
- scraper_schedule의 mode/sleep 설정을 따름
"""

from __future__ import annotations

import os
import re
import sys
import time
import json
import random
import logging
from pathlib import Path
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

KST = timezone(timedelta(hours=9))

# 모드별 처리 개수
COUNT_BY_MODE = {
    "full": 20,
    "light": 10,
    "off": 0,
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Safari/605.1.15",
]


# ------------------------------------------------------
# Schedule Loader (google_scraper.py와 동일 로직)
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


# ------------------------------------------------------
# Target Loader
# ------------------------------------------------------
def get_targets(sb: Client, region: str, limit: int) -> List[Dict]:
    """본인 region + youtube + views NULL인 행 조회"""
    res = (
        sb.table("shorts_items")
        .select("id, platform, platform_id, video_url")
        .eq("region", region)
        .eq("platform", "youtube")
        .is_("views", "null")
        .order("crawled_at", desc=True)
        .limit(limit)
        .execute()
    )
    return res.data or []


# ------------------------------------------------------
# YouTube Page Parser
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


def safe_int(s) -> Optional[int]:
    if s is None:
        return None
    try:
        if isinstance(s, (int, float)):
            return int(s)
        # "1,234,567" 형태 처리
        return int(re.sub(r"[^\d]", "", str(s)))
    except Exception:
        return None


def parse_youtube_stats(html: str) -> Dict[str, Optional[int]]:
    """
    YouTube HTML에서 views/likes/comments 추출
    - ytInitialData JSON 에서 추출 시도
    - 실패 시 meta 태그 fallback
    """
    result = {"views": None, "likes": None, "comments": None}

    # 1. ytInitialData에서 추출
    m = re.search(r"var ytInitialData = (\{.*?\});", html, re.DOTALL)
    if m:
        try:
            data = json.loads(m.group(1))
            # viewCount는 여러 위치에 있을 수 있어 재귀 탐색
            views = _find_view_count(data)
            if views:
                result["views"] = safe_int(views)

            likes = _find_like_count(data)
            if likes:
                result["likes"] = safe_int(likes)

            comments = _find_comment_count(data)
            if comments:
                result["comments"] = safe_int(comments)
        except Exception as e:
            logging.warning("ytInitialData parse failed: %s", e)

    # 2. meta 태그 fallback (views만 가능)
    if result["views"] is None:
        soup = BeautifulSoup(html, "html.parser")
        meta = soup.find("meta", itemprop="interactionCount")
        if meta:
            result["views"] = safe_int(meta.get("content"))

    return result


def _find_view_count(obj):
    """재귀로 viewCount 키 찾기"""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == "viewCount" and isinstance(v, (str, int)):
                return v
            if k == "videoViewCountRenderer" and isinstance(v, dict):
                vc = v.get("viewCount", {})
                if isinstance(vc, dict):
                    text = vc.get("simpleText") or _extract_runs_text(vc.get("runs"))
                    if text:
                        return text
            r = _find_view_count(v)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_view_count(item)
            if r:
                return r
    return None


def _find_like_count(obj):
    """재귀로 likeButton/likeCount 찾기"""
    if isinstance(obj, dict):
        # 신형 구조
        if "likeButtonViewModel" in obj:
            try:
                tooltip = obj["likeButtonViewModel"].get("likeButtonViewModel", {}) \
                    .get("toggleButtonViewModel", {}).get("toggleButtonViewModel", {}) \
                    .get("defaultButtonViewModel", {}).get("buttonViewModel", {}) \
                    .get("title")
                if tooltip:
                    return tooltip
            except Exception:
                pass

        # 구형 구조
        if "toggleButtonRenderer" in obj:
            tbr = obj["toggleButtonRenderer"]
            if "defaultText" in tbr:
                dt = tbr["defaultText"]
                if isinstance(dt, dict):
                    text = dt.get("simpleText") or _extract_runs_text(dt.get("runs"))
                    if text:
                        return text

        for v in obj.values():
            r = _find_like_count(v)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_like_count(item)
            if r:
                return r
    return None


def _find_comment_count(obj):
    """재귀로 commentCount 찾기"""
    if isinstance(obj, dict):
        if "commentsEntryPointHeaderRenderer" in obj:
            ce = obj["commentsEntryPointHeaderRenderer"]
            cc = ce.get("commentCount", {})
            if isinstance(cc, dict):
                text = cc.get("simpleText") or _extract_runs_text(cc.get("runs"))
                if text:
                    return text

        for v in obj.values():
            r = _find_comment_count(v)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _find_comment_count(item)
            if r:
                return r
    return None


def _extract_runs_text(runs):
    if not runs or not isinstance(runs, list):
        return None
    return "".join(r.get("text", "") for r in runs if isinstance(r, dict))


# ------------------------------------------------------
# Updater
# ------------------------------------------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=8), reraise=True)
def update_row(sb: Client, row_id: int, stats: Dict[str, Optional[int]]) -> None:
    payload = {k: v for k, v in stats.items() if v is not None}
    if not payload:
        return
    sb.table("shorts_items").update(payload).eq("id", row_id).execute()


# ------------------------------------------------------
# Main
# ------------------------------------------------------
def main():
    account = os.getenv("ACCOUNT_NAME")
    if not account:
        logging.error("ACCOUNT_NAME env var not set")
        sys.exit(1)

    sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

    # 1. 오늘 스케줄
    sched = get_today_schedule(sb, account)
    if not sched:
        logging.warning("No schedule for account=%s today", account)
        return

    mode = sched["mode"]
    region = sched["region"]
    sleep_min = float(sched["sleep_min"])
    sleep_max = float(sched["sleep_max"])

    if mode == "off":
        logging.info("Mode is off today. Exiting.")
        return

    target_count = COUNT_BY_MODE.get(mode, 0)
    if target_count == 0:
        logging.info("No targets for mode=%s. Exiting.", mode)
        return

    # 2. 갱신 대상 조회
    targets = get_targets(sb, region, target_count)
    if not targets:
        logging.info("No targets to update for region=%s", region)
        return

    logging.info("account=%s mode=%s region=%s targets=%d sleep=%.3f~%.3f",
                 account, mode, region, len(targets), sleep_min, sleep_max)

    # 3. 순차 처리
    success = 0
    failed = 0
    for i, row in enumerate(targets):
        sleep_sec = random.uniform(sleep_min, sleep_max)
        logging.info("[%d/%d] sleeping %.3fs id=%s url=%s",
                     i + 1, len(targets), sleep_sec, row["id"], row["video_url"])
        time.sleep(sleep_sec)

        try:
            html = fetch_html(row["video_url"])
            stats = parse_youtube_stats(html)
            if any(v is not None for v in stats.values()):
                update_row(sb, row["id"], stats)
                success += 1
                logging.info("updated id=%s views=%s likes=%s comments=%s",
                             row["id"], stats["views"], stats["likes"], stats["comments"])
            else:
                failed += 1
                logging.warning("no stats parsed id=%s url=%s",
                                row["id"], row["video_url"])
        except Exception as e:
            failed += 1
            logging.error("update failed id=%s err=%s", row["id"], e)

    logging.info("Done. success=%d failed=%d", success, failed)


if __name__ == "__main__":
    main()
