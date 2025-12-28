import os
from datetime import datetime
from typing import List, Dict, Any

import requests


def fetch_loyalty_events(since: datetime) -> List[Dict[str, Any]]:
    """
    Call an external HTTP API to fetch loyalty events updated since `since`.

    Env vars used:
      - EXTERNAL_API_BASE_URL
      - EXTERNAL_API_KEY
    """
    base_url = os.environ.get("EXTERNAL_API_BASE_URL", "https://httpbin.org")
    api_key = os.environ.get("EXTERNAL_API_KEY", "dummy-key")

    url = f"{base_url.rstrip('/')}/anything/loyalty-events"

    params = {
        "updated_since": since.isoformat(timespec="seconds") + "Z",
    }

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }

    resp = requests.get(url, params=params, headers=headers, timeout=30)
    resp.raise_for_status()

    echoed = resp.json()

    # Synthetic payload for now; swap this with real resp.json()["events"] later.
    events = [
        {
            "event_id": "EVT-1001",
            "member_id": "M001",
            "event_type": "POINT_EARNED",
            "points": 500,
            "created_at": since.isoformat(timespec="seconds") + "Z",
            "payload": echoed,
        },
        {
            "event_id": "EVT-1002",
            "member_id": "M002",
            "event_type": "POINT_REDEEMED",
            "points": -200,
            "created_at": since.isoformat(timespec="seconds") + "Z",
            "payload": echoed,
        },
    ]

    return events
