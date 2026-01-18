# DEBUG / DIAGNOSTIC SCRIPT
# Do not delete.
# Used to validate GDELT API behavior and query correctness.

from __future__ import annotations

from datetime import datetime, timezone
import requests

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

def fmt(dt: datetime) -> str:
    return dt.strftime("%Y%m%d%H%M%S")

def extract_timeline_total(payload: dict) -> int:
    timeline = payload.get("timeline", [])
    if not timeline:
        return 0

    # Normal structure: timeline[0]["data"] is the list of points with "value"
    if isinstance(timeline, list) and isinstance(timeline[0], dict) and "data" in timeline[0]:
        total = 0
        for series in timeline:
            for p in series.get("data", []):
                total += int(p.get("value", 0))
        return total

    # Fallback: already list of points
    return sum(int(p.get("value", 0)) for p in timeline)

def main():
    country = "Ukraine"
    topic = "(attack OR bombing OR airstrike OR shelling OR clashes)"

    # 30-day window (more signal than 7 days)
    start = datetime(2024, 9, 1, tzinfo=timezone.utc)
    end   = datetime(2024, 10, 1, tzinfo=timezone.utc)

    query = f'"{country}" AND {topic}'

    params = {
        "query": query,
        "mode": "timelinevolraw",
        "format": "json",
        "startdatetime": fmt(start),
        "enddatetime": fmt(end),
    }

    r = requests.get(GDELT_DOC_URL, params=params, timeout=30)
    print("STATUS:", r.status_code)
    print("CONTENT-TYPE:", r.headers.get("Content-Type"))
    print("URL:", r.url)

    if r.status_code != 200:
        print("BODY:", (r.text or "")[:300])
        return

    payload = r.json()
    total = extract_timeline_total(payload)

    # print first few points so you SEE if values exist
    timeline = payload.get("timeline", [])
    if timeline and isinstance(timeline[0], dict) and "data" in timeline[0]:
        print("FIRST 5 POINTS:", timeline[0]["data"][:5])

    print("TOTAL:", total)

if __name__ == "__main__":
    main()