# DEBUG / DIAGNOSTIC SCRIPT
# Do not delete.
# Used to validate GDELT API behavior and query correctness.

from __future__ import annotations
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import time
from typing import Dict, Any

import pandas as pd
import requests

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# GDELT: ~1 request per 5 seconds
MIN_SECONDS_BETWEEN_REQUESTS = 5.2

OUT_DIR = Path("data/interim")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = OUT_DIR / "gdelt_ukraine_test.csv"
CACHE_JSON = OUT_DIR / "gdelt_cache_ukraine_test.json"

# Parentheses ONLY around OR groups
TOPICS = {
    "violence": '(attack OR bombing OR airstrike OR shelling OR clashes)',
    "protest": '(protest OR demonstration OR rally OR riot OR strike OR unrest)',
    "rebellion": '(insurgency OR militia OR separatist OR "armed group" OR rebellion)',
}

_last_request_time = 0.0


# ---------------------------
# JSON helpers
# ---------------------------
def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def save_json(path: Path, obj: Dict[str, Any]) -> None:
    path.write_text(json.dumps(obj, indent=2, sort_keys=True))


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


# ---------------------------
# Rate limiting
# ---------------------------
def rate_limit_sleep() -> None:
    global _last_request_time
    now = time.time()
    wait = MIN_SECONDS_BETWEEN_REQUESTS - (now - _last_request_time)
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.time()


# ---------------------------
# Query + parsing
# ---------------------------
def build_country_query(country: str, topic_query: str) -> str:
    country = country.strip()
    # No parentheses around the country
    return f'"{country}" AND {topic_query}'


def extract_timeline_total(payload: Dict[str, Any]) -> int:
    """
    timelinevolraw returns:
      payload["timeline"] = [{"series": "...", "data": [{"date":..., "value":...}, ...]}]
    We must sum timeline[*]["data"][*]["value"].
    """
    timeline = payload.get("timeline", [])
    if not timeline:
        return 0

    # Normal shape: list of series dicts with "data"
    if isinstance(timeline, list) and isinstance(timeline[0], dict) and "data" in timeline[0]:
        total = 0
        for series in timeline:
            for p in series.get("data", []):
                try:
                    total += int(p.get("value", 0))
                except Exception:
                    total += 0
        return total

    # Fallback: already points
    total = 0
    for p in timeline:
        try:
            total += int(p.get("value", p.get("count", 0)))
        except Exception:
            total += 0
    return total


def fetch_count(
    session: requests.Session,
    query: str,
    start: datetime,
    end: datetime,
    max_retries: int = 6,
    debug: bool = False,
) -> int:
    params = {
        "query": query,
        "mode": "timelinevolraw",
        "format": "json",
        "startdatetime": fmt(start),
        "enddatetime": fmt(end),
    }

    backoff = 6.0

    for attempt in range(1, max_retries + 1):
        rate_limit_sleep()

        try:
            r = session.get(GDELT_DOC_URL, params=params, timeout=30)
        except Exception as e:
            if debug:
                print(f"  WARN request exception: {e}")
            time.sleep(backoff)
            backoff *= 1.8
            continue

        # Rate limit
        if r.status_code == 429:
            if debug:
                print(f"  429 rate limit. backoff {backoff:.1f}s (attempt {attempt}/{max_retries})")
            time.sleep(backoff)
            backoff *= 1.8
            continue

        if not r.ok:
            if debug:
                body = (r.text or "")[:200].replace("\n", " ")
                print(f"  WARN HTTP {r.status_code}: {body}")
                print(f"  URL: {r.url}")
            return 0

        ctype = (r.headers.get("Content-Type") or "").lower()
        if "json" not in ctype:
            if debug:
                body = (r.text or "")[:200].replace("\n", " ")
                print(f"  WARN non-JSON content-type={ctype}: {body}")
                print(f"  URL: {r.url}")
            time.sleep(backoff)
            backoff *= 1.8
            continue

        try:
            payload = r.json()
        except Exception:
            if debug:
                body = (r.text or "")[:200].replace("\n", " ")
                print(f"  WARN JSON parse failed: {body}")
                print(f"  URL: {r.url}")
            time.sleep(backoff)
            backoff *= 1.8
            continue

        total = extract_timeline_total(payload)

        if debug:
            print(f"  OK total={total}")
            # Uncomment if you want to see the final URL every time:
            # print(f"  URL: {r.url}")

        return total

    return 0


# ---------------------------
# MAIN (Ukraine only)
# ---------------------------
def main():
    # Test parameters
    COUNTRY = "Ukraine"
    WEEK_END = datetime(2024, 10, 1, tzinfo=timezone.utc)

    # For testing, use a bigger window so you *see* signal
    WINDOW_DAYS = 30  # change back to 7 later
    end = WEEK_END
    start = end - timedelta(days=WINDOW_DAYS)

    print("=== GDELT UKRAINE TEST ===")
    print(f"Country: {COUNTRY}")
    print(f"Window: {start.date()} -> {end.date()} ({WINDOW_DAYS} days)")
    print(f"Rate limit: {MIN_SECONDS_BETWEEN_REQUESTS}s/request")

    cache = load_json(CACHE_JSON)

    session = requests.Session()
    session.headers.update({"User-Agent": "BlueLanceGDELT/1.0 (local test)"})

    row: Dict[str, Any] = {
        "country": COUNTRY,
        "week_end": end.date().isoformat(),
        "window_days": WINDOW_DAYS,
    }

    for topic_name, topic_query in TOPICS.items():
        cache_key = f"{COUNTRY}|{end.date().isoformat()}|{WINDOW_DAYS}d|{topic_name}"

        if cache_key in cache:
            count = int(cache[cache_key])
            print(f"{topic_name}: {count} (cache)")
        else:
            q = build_country_query(COUNTRY, topic_query)
            print(f"\nTOPIC: {topic_name}")
            print(f"QUERY: {q}")
            count = fetch_count(session, q, start, end, debug=True)
            cache[cache_key] = int(count)
            save_json(CACHE_JSON, cache)
            print(f"{topic_name}: {count}")

        row[f"gdelt_{topic_name}_count_{WINDOW_DAYS}d"] = int(count)

    # Save result
    df = pd.DataFrame([row])
    df.to_csv(OUT_CSV, index=False)

    print(f"\nSaved -> {OUT_CSV}")
    print(df)


if __name__ == "__main__":
    main()