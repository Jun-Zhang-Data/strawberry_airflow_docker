from typing import List, Dict, Any


def normalize_loyalty_events(raw_events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Flatten / normalize raw JSON events into a simple tabular structure.
    """
    normalized: List[Dict[str, Any]] = []

    for evt in raw_events:
        normalized.append(
            {
                "event_id": evt.get("event_id"),
                "member_id": evt.get("member_id"),
                "event_type": evt.get("event_type"),
                "points": evt.get("points", 0),
                "created_at": evt.get("created_at"),
                "raw_payload": str(evt.get("payload")),
            }
        )

    return normalized
