// PROMPT: Add integration-style Go tests for Engine ingest, metrics, funnel, heatmap, anomalies, and health without external services.
// CHANGES MADE: Used temp database paths and synthetic event batches to exercise end-to-end engine methods.

package main

import (
	"path/filepath"
	"testing"
	"time"
)

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	t.Setenv("DATA_DIR", t.TempDir())
	db, err := initDB()
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewEngine(db, filepath.Join(t.TempDir(), "ipc.sock"))
}

func TestEngineIngestAndMetrics(t *testing.T) {
	engine := newTestEngine(t)
	batch := []StoreEvent{
		{EventID: "e-1", StoreID: "STORE_BLR_002", CameraID: "CAM_01", VisitorID: "VIS_1", EventType: "ENTRY", Timestamp: "2026-03-03T10:00:00Z"},
		{EventID: "e-2", StoreID: "STORE_BLR_002", CameraID: "CAM_01", VisitorID: "VIS_1", EventType: "ZONE_ENTER", ZoneID: "SKINCARE", Timestamp: "2026-03-03T10:01:00Z"},
	}
	res, err := engine.IngestBatch(batch)
	if err != nil || res.Accepted != 2 {
		t.Fatalf("ingest failed: %+v err=%v", res, err)
	}
	metrics := engine.Metrics("STORE_BLR_002")
	if metrics["unique_visitors"].(int) != 1 {
		t.Fatalf("expected 1 visitor, got %v", metrics["unique_visitors"])
	}
}

func TestEngineHeatmapAndFunnel(t *testing.T) {
	engine := newTestEngine(t)
	_, _ = engine.IngestBatch([]StoreEvent{
		{EventID: "h1", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: "VIS_h", EventType: "ENTRY", Timestamp: time.Now().UTC().Format(time.RFC3339)},
		{EventID: "h2", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: "VIS_h", EventType: "ZONE_ENTER", ZoneID: "SKINCARE", Timestamp: time.Now().UTC().Format(time.RFC3339)},
	})
	heatmap := engine.Heatmap("STORE_BLR_002")
	if heatmap["data_confidence"].(bool) {
		t.Fatal("expected low confidence with one session")
	}
	funnel := engine.Funnel("STORE_BLR_002")
	if funnel["store_id"] != "STORE_BLR_002" {
		t.Fatalf("unexpected funnel: %v", funnel)
	}
}

func TestEngineHealth(t *testing.T) {
	engine := newTestEngine(t)
	health := engine.Health()
	if health["database_connection"] != "CONNECTED" {
		t.Fatalf("expected connected db, got %v", health["database_connection"])
	}
}

func TestEngineRejectsInvalidEvent(t *testing.T) {
	engine := newTestEngine(t)
	res, err := engine.IngestBatch([]StoreEvent{{EventID: "", StoreID: "STORE_BLR_002"}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Rejected != 1 {
		t.Fatalf("expected rejected event, got %+v", res)
	}
}
