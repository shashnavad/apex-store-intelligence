# Design: Apex Store Intelligence

## Overview

Apex Store Intelligence turns anonymised CCTV footage into live retail metrics. The system is split into a Python detection tier and a Go analytics tier connected by a Unix domain socket. The north star metric is offline store conversion rate: purchases divided by unique visitor sessions in a time window.

## Data flow

```text
CCTV clips / simulation
        |
        v
  pipeline/detect.py  (YOLOv8 + ByteTrack when available)
        |
        |  JSON lines (event schema)
        v
  Unix socket  /tmp/store_intelligence.sock
        |
        v
  Go Engine  (validate, dedupe, SQLite WAL, in-memory rollups)
        |
        v
  REST API  (/metrics, /funnel, /heatmap, /anomalies, /health)
        |
        v
  Dashboard / Prometheus / on-call tools
```

## Component responsibilities

### Detection pipeline (Python)

- Reads clips from `data/clips/` or runs simulated tracks when clips are missing.
- Uses point-in-polygon checks against `data/store_layout.json` for zone assignment.
- Emits schema-compliant events: ENTRY, EXIT, ZONE_*, BILLING_QUEUE_*, REENTRY.
- `pipeline/stitching.py` and `pipeline/tracker.py` reduce fragmented track IDs across short occlusions.
- Staff flags are set from heuristics (uniform detection can be extended); staff events are still emitted but excluded downstream.

### Intelligence API (Go)

- `POST /events/ingest` accepts up to 500 events, deduplicates by `event_id`, and returns partial success metadata.
- Per-store `MetricTracker` structs maintain visitors, zone dwell, queue depth, and session funnel state in memory.
- SQLite in WAL mode stores raw events for audit and recovery; reads for GET endpoints come from memory for low latency.
- POS rows in `data/pos_transactions.csv` correlate by store and a five-minute window after billing activity.
- `data_confidence` is false when fewer than 20 active visitor sessions are tracked.

### Observability

- Structured JSON logs per request: trace_id, store_id, endpoint, latency_ms, event_count, status_code.
- `/health` exposes `STALE_FEED` when no event arrived for ten minutes.
- `/metrics` exposes Prometheus counters and histograms for Kubernetes-style scaling.

## Edge case handling

| Edge case | Approach |
|-----------|----------|
| Group entry | One ENTRY per track ID from the detector |
| Staff | `is_staff=true`, excluded from customer aggregates |
| Re-entry | REENTRY after prior EXIT via stitcher history |
| Occlusion | ByteTrack persistence + spatial/temporal stitcher |
| Empty store | Zero visitors, zero conversion, no panic |
| Low traffic | `data_confidence=false` below 20 sessions |
| Camera overlap | Shared visitor stitching keyed by spatial proximity |

## Deployment

`docker compose up` builds two images: API and pipeline. A shared volume mounts `/tmp` for the socket. The pipeline waits for API health before streaming.

## AI-Assisted Decisions

1. **Hybrid Python + Go split**  
   An LLM suggested a single FastAPI service. We kept Python only for CV and moved ingest, analytics, and REST to Go for concurrency and smaller runtime memory. This matched the decision to avoid disk-based JSON handoffs in the hot path.

2. **Unix socket instead of Kafka**  
   AI recommended RabbitMQ for durability. We rejected it for the assessment constraint of `docker compose up` with no external brokers. A socket plus SQLite WAL was chosen; the model agreed after we listed operational cost.

3. **Point-in-polygon zones vs VLM classification**  
   Gemini-style vision prompts were proposed for zone labels. We overrode that for latency: VLMs are reserved for offline calibration, not per-frame inference.

## Future production notes

- Horizontal scale: stateless API pods with shared Redis or event bus for trackers if multi-replica.
- Model serving: ONNX Runtime sidecar for GPU clips.
- Ground truth harness: compare ENTRY/EXIT counts per clip post-submission.
