"""Event schema helpers and JSONL emission utilities."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path


EVENT_TYPES = {
    "ENTRY",
    "EXIT",
    "ZONE_ENTER",
    "ZONE_EXIT",
    "ZONE_DWELL",
    "BILLING_QUEUE_JOIN",
    "BILLING_QUEUE_ABANDON",
    "REENTRY",
}


def build_event(
    store_id,
    camera_id,
    visitor_id,
    event_type,
    zone_id=None,
    dwell_ms=0,
    is_staff=False,
    confidence=0.9,
    queue_depth=None,
    sku_zone=None,
    session_seq=1,
    timestamp=None,
):
    if event_type not in EVENT_TYPES:
        raise ValueError(f"unsupported event_type: {event_type}")

    return {
        "event_id": str(uuid.uuid4()),
        "store_id": store_id,
        "camera_id": camera_id,
        "visitor_id": visitor_id if visitor_id.startswith("VIS_") else f"VIS_{visitor_id}",
        "event_type": event_type,
        "timestamp": timestamp or datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": float(confidence),
        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": sku_zone,
            "session_seq": session_seq,
        },
    }


def write_jsonl(events, output_path):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(event) + "\n")


def stream_line(event):
    return json.dumps(event) + "\n"
