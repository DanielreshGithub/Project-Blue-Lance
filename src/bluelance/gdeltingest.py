from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import json
import os
import time

import pandas as pd
import requests

# ============================================================
# GDELT Country-Week Ingest (Resumable + Cached + Rate-limited)
# ============================================================
#
# Goals:
# - One clean request path (no duplicated fetch logic)
# - Resumable across runs (progress + done)
# - Per-topic cache to avoid re-hitting GDELT
# - Handles "phrase too short" countries via candidate query expressions
# - Atomic JSON writes to avoid corrupted JSON files on Ctrl+C
#

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

PROCESSED = Path("data/processed")
INTERIM = Path("data/interim")
INTERIM.mkdir(parents=True, exist_ok=True)

OUT_CSV = INTERIM / "gdelt_country_week_features.csv"
PROGRESS_JSON = INTERIM / "gdelt_country_progress.json"
DONE_JSON = INTERIM / "gdelt_country_done.json"
CACHE_JSON = INTERIM / "gdelt_country_cache.json"

# ---- Tuning knobs ----
MIN_SECONDS_BETWEEN_REQUESTS = 5.2   # bump to 6.0 if you still hit 429
CHUNK_SIZE = 25                      # countries per run
WINDOW_DAYS = 30                     # 30d window you validated
MAX_RETRIES = 6

# IMPORTANT: parentheses only around OR groups (GDELT rule)
TOPICS: Dict[str, str] = {
    "violence": '(attack OR bombing OR airstrike OR shelling OR clashes OR fighting OR gunfire)',
    "protest": '(protest OR demonstration OR rally OR riot OR strike OR unrest)',
    "rebellion": '(insurgency OR militia OR separatist OR "armed group" OR rebellion)',
}

# Countries that can fail as short phrases. We keep an ordered list to try.
# NOTE: If you use OR at the top level, it MUST be wrapped in parentheses.
COUNTRY_QUERY_CANDIDATES: Dict[str, List[str]] = {
    "Iran": [
        '"Islamic Republic of Iran"',
        '"Iran (Islamic Republic of)"',
    ],
    "Iraq": [
        '("Republic of Iraq" OR Iraqi OR Baghdad)',
        '(Iraqi OR Baghdad)',
    ],
    "Mali": [
        '("Republic of Mali" OR Malian OR Bamako)',
        '(Malian OR Bamako)',
    ],
    "Peru": [
        '("Republic of Peru" OR Peruvian OR Lima)',
        '(Peruvian OR Lima)',
    ],
}


# --------------------
# JSON helpers (atomic)
# --------------------

def _atomic_write_text(path: Path, text: str) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text)
    os.replace(tmp, path)


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception:
        # If the file got corrupted mid-write, back it up and start fresh.
        bak = path.with_suffix(path.suffix + ".corrupt")
        try:
            os.replace(path, bak)
        except Exception:
            pass
        return {}


def save_json(path: Path, obj: Dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(obj, indent=2, sort_keys=True))


def fmt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")


def normalize_country(country: str) -> str:
    return " ".join(str(country).strip().split())


# --------------------
# Rate limiting
# --------------------

_last_request_time = 0.0


def rate_limit_sleep() -> None:
    """Guarantee minimum spacing between requests."""
    global _last_request_time
    now = time.time()
    wait = MIN_SECONDS_BETWEEN_REQUESTS - (now - _last_request_time)
    if wait > 0:
        time.sleep(wait)
    _last_request_time = time.time()


# --------------------
# GDELT parsing + request
# --------------------

def extract_timeline_total(payload: Dict[str, Any]) -> int:
    """Sum timeline values for timelinevolraw responses."""
    timeline = payload.get("timeline")
    if not timeline:
        return 0

    total = 0

    # Common shape: [{"series":..., "data":[{"date":..., "value":...}, ...]}]
    if isinstance(timeline, list) and timeline and isinstance(timeline[0], dict) and "data" in timeline[0]:
        for series in timeline:
            for p in series.get("data", []):
                try:
                    total += int(p.get("value", 0))
                except Exception:
                    pass
        return total

    # Fallback: list of points
    if isinstance(timeline, list):
        for p in timeline:
            try:
                total += int(p.get("value", 0))
            except Exception:
                pass

    return total


def is_semantic_error(msg: str) -> bool:
    """Non-transient errors; retrying won't help."""
    m = msg.lower()
    return (
        "the specified phrase is too short" in m
        or "boolean or's may only appear inside of a () clause" in m
        or "parentheses may only be used around or'd statements" in m
        or "query is invalid" in m
    )


def _wrap_or(expr: str) -> str:
    """If expr contains top-level OR, ensure it's wrapped in parentheses."""
    s = expr.strip()
    if " OR " in s and not (s.startswith("(") and s.endswith(")")):
        return f"({s})"
    return s


def build_query(country_expr: str, topic_query: str) -> str:
    country_expr = _wrap_or(country_expr)
    return f"{country_expr} AND {topic_query}"


def request_timeline_total(
    session: requests.Session,
    query: str,
    start: datetime,
    end: datetime,
) -> Tuple[Optional[int], Optional[str]]:
    """Return (count, semantic_error_message)."""
    params = {
        "query": query,
        "mode": "timelinevolraw",
        "format": "json",
        "startdatetime": fmt(start),
        "enddatetime": fmt(end),
    }

    backoff = 6.0

    for attempt in range(1, MAX_RETRIES + 1):
        rate_limit_sleep()

        try:
            r = session.get(GDELT_DOC_URL, params=params, timeout=30)
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 1.8
                continue
            return 0, None

        if r.status_code == 429:
            time.sleep(backoff)
            backoff *= 1.8
            continue

        if not r.ok:
            return 0, None

        ctype = (r.headers.get("Content-Type") or "").lower()
        if "json" not in ctype:
            preview = (r.text or "")[:240].replace("\n", " ").strip()
            if is_semantic_error(preview):
                return None, preview
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 1.8
                continue
            return 0, None

        try:
            payload = r.json()
        except Exception:
            if attempt < MAX_RETRIES:
                time.sleep(backoff)
                backoff *= 1.8
                continue
            return 0, None

        return extract_timeline_total(payload), None

    return 0, None


def fetch_country_topic_count(
    session: requests.Session,
    country: str,
    topic_query: str,
    start: datetime,
    end: datetime,
) -> int:
    """Try candidate country expressions until one works."""
    country = normalize_country(country)
    candidates = COUNTRY_QUERY_CANDIDATES.get(country, [f'"{country}"'])

    last_semantic: Optional[str] = None

    for expr in candidates:
        q = build_query(expr, topic_query)
        count, semantic = request_timeline_total(session, q, start, end)
        if semantic is not None:
            last_semantic = semantic
            continue
        return int(count or 0)

    if last_semantic:
        print(f"  WARN semantic error (no viable expression): {last_semantic}")
        print(f"  DEBUG country={country} candidates={candidates}")

    return 0


# --------------------
# Persistence helpers
# --------------------

def done_key(country: str, week_str: str, window_days: int) -> str:
    return f"{normalize_country(country)}|{week_str}|{window_days}d"


def cache_key(country: str, week_str: str, window_days: int, topic: str) -> str:
    return f"{normalize_country(country)}|{week_str}|{window_days}d|{topic}"


def append_row(row: Dict[str, Any]) -> None:
    df = pd.DataFrame([row])
    header = not OUT_CSV.exists()
    df.to_csv(OUT_CSV, mode="a", header=header, index=False)


# --------------------
# Main
# --------------------

def main() -> None:
    infile = PROCESSED / "acled_global_weekly_features.csv"
    if not infile.exists():
        raise FileNotFoundError(f"Missing input file: {infile}")

    df = pd.read_csv(infile, parse_dates=["week"])
    if "country" not in df.columns or "week" not in df.columns:
        raise ValueError("Expected columns ['week','country'] in ACLED features file")

    latest_week = df["week"].max()
    week_str = latest_week.date().isoformat()

    countries: List[str] = sorted(
        normalize_country(c)
        for c in df.loc[df["week"] == latest_week, "country"].dropna().unique().tolist()
    )

    print(f"Latest ACLED week: {week_str}")
    print(f"Countries: {len(countries):,}")
    print(f"Window: last {WINDOW_DAYS} days ending at week_end")
    print(f"Rate limit: {MIN_SECONDS_BETWEEN_REQUESTS}s/request")
    print(f"Chunk size: {CHUNK_SIZE}")
    print(f"Output CSV: {OUT_CSV}\n")

    # Anchor window to ACLED week end (midnight UTC)
    end = datetime(latest_week.year, latest_week.month, latest_week.day, tzinfo=timezone.utc)
    start = end - timedelta(days=WINDOW_DAYS)

    progress = load_json(PROGRESS_JSON)
    start_idx = int(progress.get("country_index", 0))

    done = load_json(DONE_JSON)   # {"Canada|2025-12-27|30d": true, ...}
    cache = load_json(CACHE_JSON) # {"Canada|2025-12-27|30d|violence": 123, ...}

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Project-Blue-Lance/1.0 (GDELT ingest; local dev)"
    })

    processed_this_run = 0

    for i in range(start_idx, len(countries)):
        country = countries[i]
        dkey = done_key(country, week_str, WINDOW_DAYS)

        if done.get(dkey) is True:
            print(f"SKIP {country} (already done)")
        else:
            print(f"FETCH {country}")

            row: Dict[str, Any] = {
                "country": country,
                "week": week_str,
                "window_days": WINDOW_DAYS,
            }

            for topic_name, topic_query in TOPICS.items():
                ckey = cache_key(country, week_str, WINDOW_DAYS, topic_name)

                if ckey in cache:
                    count = int(cache[ckey])
                else:
                    count = fetch_country_topic_count(session, country, topic_query, start, end)
                    cache[ckey] = int(count)
                    save_json(CACHE_JSON, cache)  # Ctrl+C safe

                row[f"gdelt_{topic_name}_count_{WINDOW_DAYS}d"] = int(count)
                print(f"  {topic_name}: {count}")

            append_row(row)

            done[dkey] = True
            save_json(DONE_JSON, done)

        processed_this_run += 1

        # checkpoint after each country
        save_json(PROGRESS_JSON, {"country_index": i + 1, "week": week_str})

        if processed_this_run >= CHUNK_SIZE:
            print(f"\nChunk complete: {processed_this_run} countries. Run again to resume.")
            return

    print("\nAll countries done.")


if __name__ == "__main__":
    main()