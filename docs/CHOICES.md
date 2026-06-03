# Architectural Choices: Apex Store Intelligence

This document records the main engineering decisions, options considered, and tradeoffs for the storefront intelligence system.

## 1. Multi-language hybrid: Python CV + Go API

**Options considered:** All-Python FastAPI stack; all-Go with embedded ONNX; Python CV plus Go services.

**What AI suggested:** Single FastAPI monolith for speed of development.

**What we chose:** Python for YOLOv8, ByteTrack, and geometry; Go for ingest, aggregation, REST, and Prometheus.

**Why:** Python has the mature CV ecosystem. Go gives sub-100ms reads via in-memory structures and efficient concurrency. Frames are not written to disk between tiers; events stream over a Unix socket.

**Tradeoff:** Two runtimes in Docker. Mitigated with compose health checks and a single command startup.

---

## 2. IPC: Unix domain socket (not Kafka or HTTP)

**Options considered:** Kafka, RabbitMQ, REST POST per event, Unix socket with newline JSON.

**What AI suggested:** RabbitMQ for guaranteed delivery.

**What we chose:** Unix domain socket with newline-delimited JSON events.

**Why:** Meets turnkey `docker compose up`, minimal footprint, and lowest local latency.

**Tradeoff:** Volatile in-flight buffer if the API crashes. Mitigated with SQLite WAL persistence on accepted events and pipeline retry on connect.

---

## 3. In-memory pre-aggregations

**Options considered:** SQL aggregates on every GET; stream processor (Flink-style); in-memory rollups with SQLite archive.

**What AI suggested:** PostgreSQL materialized views.

**What we chose:** Thread-safe Go maps per store for visitors, zones, queue, and sessions.

**Why:** Funnel and heatmap queries stay in single-digit milliseconds under write load.

**Tradeoff:** RAM grows with active sessions. Mitigated by purging state on EXIT and persisting raw events to SQLite.

---

## 4. SQLite WAL + data_confidence

**Options considered:** PostgreSQL, in-memory only, SQLite WAL embedded.

**What AI suggested:** PostgreSQL for analytics maturity.

**What we chose:** SQLite with `journal_mode=WAL` and `data_confidence=false` when fewer than 20 sessions are active.

**Why:** Zero extra containers for evaluators; WAL handles concurrent writes from ingest.

**Tradeoff:** Write contention at very high QPS. Acceptable for the challenge scale; can swap driver DSN later.

---

## 5. Point-in-polygon fencing (not live VLM)

**Options considered:** GPT-4V / Gemini per frame for zones; manual ROI masks; PIP on layout polygons.

**What AI suggested:** VLM prompts for zone and staff classification.

**What we chose:** YOLOv8 boxes plus PIP against `store_layout.json`.

**Why:** VLMs exceed 500ms per frame and break real-time 15fps processing.

**Tradeoff:** Less semantic context (cannot read signage). Staff detection remains heuristic until a offline labeled set exists.

---

## 6. Stateless API surface + Prometheus

**Options considered:** Stateful sticky sessions; metrics only in logs; Prometheus `/metrics` plus structured logs.

**What AI suggested:** Cloud vendor APM agent.

**What we chose:** Stateless handlers, JSON logs, Prometheus histograms, health staleness per store.

**Why:** Same binary works locally and on Kubernetes HPA in production.

**Tradeoff:** Slightly more bootstrap code. No vendor lock-in.

---

## 7. Static Offline Layout Compilation vs. Dynamic Runtime VLM Parsing

**Options considered:** 1. Building a runtime pipeline using a Vision-Language Model (VLM) API inside the Docker container to dynamically parse the store layout PNGs on startup.
2. Offline pre-processing of the PNG layouts into a unified, static global JSON configuration file (`store_layout.json`).

**What AI suggested:** A dynamic Python script using a Vision API to parse layout PNGs on container startup to ensure the system could "handle any layout format automatically."

**What we chose:** Option 2 (Offline Pre-processing).

**Why:** * **Production Reliability & Gate Compliance:** The evaluation requires a deterministic, zero-manual-step execution via `docker compose up` on a clean machine. Relying on external third-party Vision APIs at runtime introduces critical breaking points (requiring external API keys, internet connectivity dependencies, and handling non-deterministic VLM coordinate outputs).
* **Business Domain Reality:** In physical retail, store floor plans are static operational assets. They do not change dynamically from frame to frame. Treating them as a compiled configuration file reduces system latency, saves significant API costs, and keeps the core focus on the primary business logic: the Detection Layer accuracy and Intelligence API metrics.  

---

## 8. Handling Heterogeneous Upstream Identifier Schemas (Data Cleansing)

**The Problem:** During integration, a structural mismatch was identified between the problem statement specifications (`STORE_BLR_002`), the mock transactional data (`ST1008`), and the validation telemetry stream (`store_1076`).

**What we chose:** Built a defensive Data Normalization Registry inside the ingestion engine. All raw strings are sanitized through an in-memory lookup dictionary upon extraction.

**Why:** This replicates a true production middleware pipeline where downstream business logic shouldn't crash due to upstream naming anomalies from legacy POS vendors versus modern CV hardware tracking layers. Spatial-to-transactional pairing was verified by cross-referencing the `brand_name` column of the transactional database with the physical wall-unit bounding labels on the Store 1 blueprint. The presence of identical brand anchors like 'Faces Canada' and 'Renee' confirmed the structural logic.

## Detection model (summary)

**Model:** Ultralytics YOLOv8n with ByteTrack when weights are available; simulation mode otherwise.

**Why not VLM for live path:** Latency and cost. Documented above; VLMs may be used offline to refine polygons or staff uniforms.
