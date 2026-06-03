# Apex Store Intelligence

End-to-end store analytics system: Python computer vision pipeline, Go intelligence API, Unix socket IPC, and SQLite persistence. Built for the Purplle Store Intelligence challenge.

## What this repo contains

| Path | Purpose |
|------|---------|
| `pipeline/` | Detection, tracking, zone geometry, event emission |
| `app/` | Go REST API, in-memory analytics, ingest, anomalies |
| `data/` | Layout, sample events, POS CSV (place CCTV clips under `data/clips/`) |
| `docs/DESIGN.md` | Architecture and AI-assisted decisions |
| `docs/CHOICES.md` | Model, schema, and infrastructure tradeoffs |
| `scripts/` | Event replay |

## Prerequisites

- Docker Engine 20.10+ and Docker Compose v2
- Port `8080` available on the host
- Optional local dev: Go 1.21+, Python 3.11+

## Quick start (5 commands)

```bash
git clone https://github.com/shashnavad/apex-store-intelligence.git
cd apex-store-intelligence
docker compose up -d --build
curl -s http://localhost:8080/health | python3 -m json.tool
curl -s http://localhost:8080/stores/STORE_BLR_002/metrics | python3 -m json.tool
```

The API and simulated detection pipeline start together. With no video files present, the pipeline runs in `SIMULATE=1` mode and streams sample events over the Unix socket.

Then open `http://localhost:8080/dashboard` in your browser to view live store metrics.

## Dataset placement

Copy challenge assets into `data/`:

```text
data/
  clips/                    # *.mp4 per store and camera
  store_layout.json
  pos_transactions.csv
  sample_events.jsonl
```

Clips are gitignored. The repo ships layout, POS, and sample events so ingest and metrics work out of the box.

## Run with Docker Compose (recommended)

```bash
docker compose up -d --build
docker compose ps
docker compose logs -f intelligence-api
docker compose logs -f detection-pipeline
```

Stop:

```bash
docker compose down
```

### Services

- `intelligence-api` (port 8080): REST API, Prometheus `/metrics`, Unix socket ingest
- `detection-pipeline`: runs `pipeline/run.sh`, writes JSONL to `output/events/`, streams to the socket

## Run locally without Docker

### 1. Start the Go API

```bash
export DATA_DIR=./data
export IPC_SOCKET_PATH=/tmp/store_intelligence.sock
go run ./app
```

### 2. Run the detection pipeline

Simulated events (no video required):

```bash
pip install -r requirements.txt
chmod +x pipeline/run.sh
SIMULATE=1 python3 pipeline/detect.py --simulate --output-jsonl output/events/simulated.jsonl
```

Process real clips when available:

```bash
bash pipeline/run.sh
```

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `IPC_SOCKET_PATH` | `/tmp/store_intelligence.sock` | Unix socket for live streaming |
| `SIMULATE` | `0` | Force simulated detections in `run.sh` |
| `YOLO_WEIGHTS` | `yolov8n.pt` | Ultralytics weights when YOLO is installed |

### 3. Replay JSONL into the API (batch ingest)

```bash
pip install requests
python3 scripts/replay_events.py http://localhost:8080 output/events/*.jsonl
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| POST | `/events/ingest` | Batch ingest (max 500), idempotent by `event_id` |
| GET | `/stores/{id}/metrics` | Visitors, conversion, dwell, queue, `data_confidence` |
| GET | `/stores/{id}/funnel` | Session funnel with drop-off percentages |
| GET | `/stores/{id}/heatmap` | Zone frequency and normalized dwell |
| GET | `/stores/{id}/anomalies` | Queue spike, conversion drop, dead zone |
| GET | `/dashboard` | Live browser dashboard (defaults to STORE_BLR_002) |
| GET | `/dashboard/{id}` | Live browser dashboard for a specific store |
| GET | `/health` | Status, per-store last event, `STALE_FEED` if lag > 10 min |
| GET | `/metrics` | Prometheus metrics |

Example:

```bash
curl -s -X POST http://localhost:8080/events/ingest \
  -H 'Content-Type: application/json' \
  -d @data/sample_events.jsonl
```

Note: ingest expects a JSON array. For JSONL files use `scripts/replay_events.py`.

## Tests

### Go unit and handler tests

```bash
go test ./app/... -cover
```

Target: statement coverage above 70% on the `app` package.

### Python pipeline and clip tests

```bash
pip install -r requirements.txt
pytest tests/test_pipeline.py -v
```

Runs schema, PIP zone, and short (15 frame) processing on each file in `data/clips/`.

### Challenge assertions (integration)

```bash
# API must be running, or pytest will start one on port 18080
pytest tests/test_integration_api.py -v

# Or against an existing server:
python3 data/assertions.py http://localhost:8080
```

### Integration chaos harness (optional)

With the API running:

```bash
pip install requests
python3 pipeline/simulation_harness.py
```

### Manual API smoke test

```bash
curl -f http://localhost:8080/health
curl -f http://localhost:8080/stores/STORE_BLR_002/metrics
curl -f http://localhost:8080/stores/STORE_BLR_002/funnel
curl -f http://localhost:8080/stores/STORE_BLR_002/heatmap
curl -f http://localhost:8080/stores/STORE_BLR_002/anomalies
```

## Architecture summary

- **Python**: frame processing, YOLOv8 + ByteTrack (when installed), point-in-polygon zones, event schema
- **Go**: Ingest, deduplication, in-memory rollups, SQLite WAL persistence, REST and Prometheus
- **Defensive Ingestion Middleware**: Real-time identifier unification mapping (`store_code` ➔ `store_id`) and fallback token normalization logic built right into the ingestion loop to guarantee bulletproof multi-platform runtime compatibility.
- **IPC**: Newline-delimited JSON over Unix domain socket (no Kafka or HTTP between processes)

See `docs/DESIGN.md` and `docs/CHOICES.md` for full rationale.

## Submission checklist

- [ ] `docker compose up` works on a clean machine
- [ ] README pipeline steps verified
- [ ] `docs/DESIGN.md` and `docs/CHOICES.md` reviewed
- [ ] Test files include PROMPT / CHANGES MADE headers
- [ ] Challenge clips added under `data/clips/` for full detection scoring

## License

Challenge dataset and clips are for evaluation only. Do not redistribute footage.
