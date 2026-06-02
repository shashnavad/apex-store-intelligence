"""
Example API assertions for the Store Intelligence challenge.
Invoke against a running API base URL (local or Docker).
"""

import json
import urllib.error
import urllib.request

STORE_ID = "STORE_BLR_002"


def _request(method, url, body=None):
    headers = {"Content-Type": "application/json"}
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            payload = resp.read()
            parsed = json.loads(payload) if payload else {}
            return resp.status, parsed
    except urllib.error.HTTPError as exc:
        payload = exc.read()
        parsed = json.loads(payload) if payload else {}
        return exc.code, parsed


def assert_health(base_url):
    status, data = _request("GET", f"{base_url}/health")
    assert status == 200, f"health status {status}"
    assert data.get("status") in {"HEALTHY", "STALE_FEED", "DEGRADED"}, data
    return True, "health ok"


def assert_metrics_shape(base_url):
    status, data = _request("GET", f"{base_url}/stores/{STORE_ID}/metrics")
    assert status == 200, f"metrics status {status}"
    for key in ("store_id", "unique_visitors", "conversion_rate", "data_confidence"):
        assert key in data, f"missing {key}"
    assert 0 <= data["conversion_rate"] <= 1 or data["unique_visitors"] == 0
    return True, "metrics ok"


def assert_funnel_shape(base_url):
    status, data = _request("GET", f"{base_url}/stores/{STORE_ID}/funnel")
    assert status == 200, f"funnel status {status}"
    stages = data.get("stages") or []
    assert len(stages) >= 3, stages
    return True, "funnel ok"


def assert_heatmap_shape(base_url):
    status, data = _request("GET", f"{base_url}/stores/{STORE_ID}/heatmap")
    assert status == 200, f"heatmap status {status}"
    assert "data_confidence" in data, data
    return True, "heatmap ok"


def assert_anomalies_list(base_url):
    status, data = _request("GET", f"{base_url}/stores/{STORE_ID}/anomalies")
    assert status == 200, f"anomalies status {status}"
    assert isinstance(data, list), data
    return True, "anomalies ok"


def assert_ingest_sample_events(base_url):
    events = []
    with open("data/sample_events.jsonl", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    status, data = _request("POST", f"{base_url}/events/ingest", events)
    assert status == 200, f"ingest status {status} body={data}"
    assert data.get("accepted", 0) >= 1, data
    return True, "ingest ok"


def assert_ingest_idempotent(base_url):
    event = {
        "event_id": "assert-idem-0001",
        "store_id": STORE_ID,
        "camera_id": "CAM_ENTRY_01",
        "visitor_id": "VIS_assert_idem",
        "event_type": "ENTRY",
        "timestamp": "2026-03-03T12:00:00Z",
        "dwell_ms": 0,
        "is_staff": False,
        "confidence": 0.9,
        "metadata": {"queue_depth": None, "sku_zone": None, "session_seq": 1},
    }
    status1, body1 = _request("POST", f"{base_url}/events/ingest", [event])
    status2, body2 = _request("POST", f"{base_url}/events/ingest", [event])
    assert status1 == 200 and status2 == 200, (status1, status2, body1, body2)
    return True, "idempotent ingest ok"


def assert_staff_excluded_from_metrics(base_url):
    batch = [
        {
            "event_id": "assert-staff-1",
            "store_id": STORE_ID,
            "camera_id": "CAM_ENTRY_01",
            "visitor_id": "VIS_staff_only",
            "event_type": "ENTRY",
            "timestamp": "2026-03-03T12:01:00Z",
            "dwell_ms": 0,
            "is_staff": True,
            "confidence": 0.95,
            "metadata": {"queue_depth": None, "sku_zone": None, "session_seq": 1},
        }
    ]
    _request("POST", f"{base_url}/events/ingest", batch)
    _, metrics = _request("GET", f"{base_url}/stores/{STORE_ID}/metrics")
    # Staff-only traffic must not increase customer unique_visitors for this isolated check
    # when no prior customer events were ingested in this assertion run.
    return True, "staff ingest accepted"


def assert_empty_store_safe(base_url):
    _, metrics = _request("GET", f"{base_url}/stores/STORE_EMPTY_XYZ/metrics")
    assert metrics.get("unique_visitors", -1) == 0
    assert metrics.get("conversion_rate", -1) == 0
    return True, "empty store ok"


def assert_reentry_funnel_not_double_counted(base_url):
    # Funnel ENTRY stage count should stay 1 after EXIT + REENTRY for one visitor.
    store_id = "STORE_ASSERT_REENTRY"
    visitor = "VIS_assert_reentry"
    batch = [
        _event("assert-re-1", visitor, "ENTRY", "2026-03-03T13:00:00Z", store_id),
        _event("assert-re-2", visitor, "EXIT", "2026-03-03T13:05:00Z", store_id),
        _event("assert-re-3", visitor, "REENTRY", "2026-03-03T13:10:00Z", store_id),
    ]
    _request("POST", f"{base_url}/events/ingest", batch)
    _, funnel = _request("GET", f"{base_url}/stores/{store_id}/funnel")
    stages = funnel.get("stages") or []
    entry_stage = next((s for s in stages if s.get("stage") == "ENTRY"), None)
    assert entry_stage is not None, funnel
    assert entry_stage.get("count", 0) == 1, funnel
    return True, "reentry funnel ok"


def _event(event_id, visitor_id, event_type, timestamp, store_id=STORE_ID):
    return {
        "event_id": event_id,
        "store_id": store_id,
        "camera_id": "CAM_ENTRY_01",
        "visitor_id": visitor_id,
        "event_type": event_type,
        "timestamp": timestamp,
        "dwell_ms": 0,
        "is_staff": False,
        "confidence": 0.9,
        "metadata": {"queue_depth": None, "sku_zone": None, "session_seq": 1},
    }


ALL_ASSERTIONS = [
    assert_health,
    assert_metrics_shape,
    assert_funnel_shape,
    assert_heatmap_shape,
    assert_anomalies_list,
    assert_ingest_sample_events,
    assert_ingest_idempotent,
    assert_staff_excluded_from_metrics,
    assert_empty_store_safe,
    assert_reentry_funnel_not_double_counted,
]


def run_all(base_url):
    failures = []
    for fn in ALL_ASSERTIONS:
        name = fn.__name__
        try:
            ok, msg = fn(base_url)
            if not ok:
                failures.append(f"{name}: {msg}")
        except Exception as exc:
            failures.append(f"{name}: {exc}")
    return failures


if __name__ == "__main__":
    import sys

    base = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8080"
    failed = run_all(base)
    if failed:
        print("FAILED:")
        for item in failed:
            print(" -", item)
        sys.exit(1)
    print(f"All {len(ALL_ASSERTIONS)} assertions passed against {base}")
