# PROMPT: Write pytest tests for pipeline emit schema, point-in-polygon zone checks, and short clip processing.
# CHANGES MADE: Added schema validation, PIP geometry cases, emit JSONL format checks, and parametrized MP4 smoke tests with max_frames cap.

import json
import sys
import uuid
from pathlib import Path

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_DIR = ROOT / "pipeline"
sys.path.insert(0, str(PIPELINE_DIR))

from detect import DetectionPipeline, load_zones, point_in_polygon  # noqa: E402
from emit import EVENT_TYPES, build_event, stream_line, write_jsonl  # noqa: E402

STORE_ID = "STORE_BLR_002"
LAYOUT = ROOT / "data" / "store_layout.json"
CLIPS_DIR = ROOT / "data" / "clips"


def test_build_event_schema_fields():
    event = build_event(STORE_ID, "CAM_ENTRY_01", "abc", "ENTRY")
    required = {
        "event_id",
        "store_id",
        "camera_id",
        "visitor_id",
        "event_type",
        "timestamp",
        "zone_id",
        "dwell_ms",
        "is_staff",
        "confidence",
        "metadata",
    }
    assert required.issubset(event.keys())
    uuid.UUID(event["event_id"])
    assert event["visitor_id"].startswith("VIS_")
    assert event["event_type"] in EVENT_TYPES


def test_build_event_rejects_unknown_type():
    with pytest.raises(ValueError):
        build_event(STORE_ID, "CAM_01", "VIS_x", "NOT_A_REAL_TYPE")


def test_stream_line_is_single_json_object_per_line():
    event = build_event(STORE_ID, "CAM_01", "VIS_line", "ZONE_ENTER", zone_id="SKINCARE")
    line = stream_line(event)
    parsed = json.loads(line.strip())
    assert parsed["event_type"] == "ZONE_ENTER"


def test_write_jsonl_roundtrip(tmp_path):
    events = [
        build_event(STORE_ID, "CAM_01", "VIS_a", "ENTRY"),
        build_event(STORE_ID, "CAM_01", "VIS_a", "EXIT"),
    ]
    out = tmp_path / "events.jsonl"
    write_jsonl(events, out)
    lines = [json.loads(row) for row in out.read_text(encoding="utf-8").splitlines() if row.strip()]
    assert len(lines) == 2
    assert lines[0]["event_id"] != lines[1]["event_id"]


def test_point_in_polygon_skincare_zone():
    zones = load_zones(str(LAYOUT), STORE_ID)
    poly = zones["SKINCARE"]
    assert point_in_polygon((0.3, 0.4), poly) is True
    assert point_in_polygon((0.9, 0.9), poly) is False


def test_point_in_polygon_billing_zone():
    zones = load_zones(str(LAYOUT), STORE_ID)
    poly = zones["BILLING_ZONE"]
    assert point_in_polygon((0.8, 0.7), poly) is True
    assert point_in_polygon((0.2, 0.2), poly) is False


def test_sample_events_jsonl_matches_schema():
    sample = ROOT / "data" / "sample_events.jsonl"
    if not sample.exists():
        pytest.skip("sample_events.jsonl missing")
    for line in sample.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        assert row["event_type"] in EVENT_TYPES
        assert row["store_id"] == STORE_ID


@pytest.mark.parametrize(
    "clip_path",
    sorted(CLIPS_DIR.glob("*.mp4")) if CLIPS_DIR.exists() else [],
    ids=lambda p: p.name,
)
def test_process_mp4_clip_short(clip_path, tmp_path):
    if not clip_path.exists():
        pytest.skip(f"missing clip {clip_path}")

    out = tmp_path / f"{clip_path.stem.replace(' ', '_')}.jsonl"
    pipeline = DetectionPipeline(
        video_path=str(clip_path),
        store_id=STORE_ID,
        camera_id="CAM_TEST_01",
        layout_path=str(LAYOUT),
        output_jsonl=str(out),
        simulate=False,
    )
    pipeline.process_stream(max_frames=15)

    assert out.exists(), f"no output for {clip_path.name}"
    lines = [ln for ln in out.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(lines) >= 1, f"expected events from {clip_path.name}"

    for line in lines[:5]:
        row = json.loads(line)
        assert row["event_type"] in EVENT_TYPES
        assert row["store_id"] == STORE_ID


def test_detection_pipeline_simulate_mode(tmp_path):
    out = tmp_path / "sim.jsonl"
    pipeline = DetectionPipeline(
        video_path=str(ROOT / "data" / "clips" / "missing.mp4"),
        store_id=STORE_ID,
        camera_id="CAM_SIM",
        layout_path=str(LAYOUT),
        output_jsonl=str(out),
        simulate=True,
    )
    pipeline.process_stream()
    assert out.exists()
    assert len(out.read_text(encoding="utf-8").splitlines()) >= 1
