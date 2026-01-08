import os
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

import requests


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _iso_z(dt: datetime) -> str:
    dt = _to_utc(dt)
    # Snowflake/dbt friendly + consistent with many APIs
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def fetch_loyalty_events(since: datetime, until: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """
    Call an external HTTP API to fetch loyalty events updated since `since`.
    Optional `until` bounds the window (best for backfills).

    Env vars used:
      - EXTERNAL_API_BASE_URL
      - EXTERNAL_API_KEY
    """
    base_url = os.environ.get("EXTERNAL_API_BASE_URL", "https://httpbin.org")
    api_key = os.environ.get("EXTERNAL_API_KEY", "dummy-key")

    url = f"{base_url.rstrip('/')}/anything/loyalty-events"

    params = {
        "updated_since": _iso_z(since),
    }
    # If your real API supports an upper bound, this is where it goes:
    if until is not None:
        params["updated_until"] = _iso_z(until)

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    echoed = resp.json()

    # Synthetic payload for now; swap this with real resp.json()["events"] later.
    # We generate timestamps within [since, until) if until is provided.
    since_utc = _to_utc(since)
    if until is not None:
        until_utc = _to_utc(until)
        # pick a time inside the window: end minus 1 second (but not before since)
        inside_end = max(since_utc, until_utc - timedelta(seconds=1))
        created_at_2 = _iso_z(inside_end)
    else:
        created_at_2 = _iso_z(since_utc)

    events = [
        {
            "event_id": "EVT-1001",
            "member_id": "M001",
            "event_type": "POINT_EARNED",
            "points": 500,
            "created_at": _iso_z(since_utc),
            "payload": echoed,
        },
        {
            "event_id": "EVT-1002",
            "member_id": "M002",
            "event_type": "POINT_REDEEMED",
            "points": -200,
            "created_at": created_at_2,
            "payload": echoed,
        },
    ]

    return events
