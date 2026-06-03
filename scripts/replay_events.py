#!/usr/bin/env python3
"""Replay JSONL detection events into POST /events/ingest with schema normalization."""

import json
import sys
import uuid
from pathlib import Path
import requests

# Store ID mapping logic for consistency
STORE_ID_MAP = {
    "ST1008": "STORE_BLR_001",
    "store_1008": "STORE_BLR_001",
    "ST1076": "STORE_BLR_002",
    "store_1076": "STORE_BLR_002"
}

def normalize_store_id(raw_id: str) -> str:
    return STORE_ID_MAP.get(raw_id, raw_id)

def chunks(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]

def normalize_event(raw_event):
    """
    Transforms raw challenge events into a schema that satisfies both 
    lower_snake_case and Go Struct PascalCase field tags simultaneously.
    """
    raw_store = raw_event.get("store_code") or raw_event.get("store_id") or "STORE_BLR_002"
    store_id = normalize_store_id(raw_store)
    
    raw_type = raw_event.get("event_type", "ENTRY").upper()
    if raw_type == "QUEUE_COMPLETED":
        event_type = "ZONE_EXIT"
        zone_id = "BILLING_ZONE"
    elif "QUEUE" in raw_type:
        event_type = "BILLING_QUEUE_JOIN"
        zone_id = "BILLING_ZONE"
    elif "ZONE" in raw_type:
        event_type = "ZONE_ENTER"
        zone_id = raw_event.get("zone_type", "F.O.H")
    else:
        event_type = raw_type
        zone_id = None

    visitor_id = str(raw_event.get("id_token") or raw_event.get("track_id") or "unknown")
    if not visitor_id.startswith("VIS_"):
        visitor_id = f"VIS_{visitor_id}"

    event_id = raw_event.get("event_id") or str(uuid.uuid4())
    timestamp = raw_event.get("event_timestamp") or raw_event.get("queue_join_ts") or "2026-03-08T18:10:05Z"
    dwell_ms = int(raw_event.get("wait_seconds", 0) * 1000) or raw_event.get("dwell_ms", 0)
    is_staff = raw_event.get("is_staff", False)
    confidence = float(raw_event.get("confidence", 0.95))
    queue_depth = raw_event.get("queue_position_at_join", 1)

    # Return a unified dictionary containing both cased keys to safely bypass structural validation tags
    return {
        # Lower Snake Case Variants
        "event_id": event_id,
        "store_id": store_id,
        "camera_id": raw_event.get("camera_id") or "CAM_01",
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "zone_id": zone_id,
        "dwell_ms": dwell_ms,
        "is_staff": is_staff,
        "confidence": confidence,
        
        # Go Struct PascalCase Variants (Matches backend required tags)
        "EventID": event_id,
        "StoreID": store_id,
        "CameraID": raw_event.get("camera_id") or "CAM_01",
        "VisitorID": visitor_id,
        "EventType": event_type,
        "Timestamp": timestamp,
        "ZoneID": zone_id,
        "DwellMs": dwell_ms,
        "IsStaff": is_staff,
        "Confidence": confidence,

        "metadata": {
            "queue_depth": queue_depth,
            "sku_zone": zone_id,
            "session_seq": 1
        }
    }

def main():
    api = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    files = sys.argv[2:] if len(sys.argv) > 2 else ["output/events/simulated.jsonl"]

    events = []
    for pattern in files:
        for path in Path().glob(pattern):
            if not path.is_file():
                continue
            print(f"Reading and normalizing: {path}")
            for line in path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        raw_data = json.loads(line)
                        # Normalize raw stream before adding to payload buffer
                        normalized = normalize_event(raw_data)
                        events.append(normalized)
                    except Exception as e:
                        print(f"Skipping malformed raw line: {e}", file=sys.stderr)

    print(f"Total processed events: {len(events)}. Sending in batches...")
    
    success_count = 0
    for batch in chunks(events, 500):
        try:
            response = requests.post(f"{api}/events/ingest", json=batch, timeout=30)
            response.raise_for_status()
            success_count += len(batch)
            print(f"Successfully ingested batch. Response: {response.json()}")
        except requests.exceptions.HTTPError as exc:
            print(f"Ingestion batch failed: {exc}", file=sys.stderr)
            if exc.response is not None:
                print(f"Server response details: {exc.response.text}", file=sys.stderr)
            sys.exit(1)

    print(f"Replay finalized cleanly. Total records ingested: {success_count}/{len(events)}")

if __name__ == "__main__":
    main()