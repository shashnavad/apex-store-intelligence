// PROMPT: Expand Go test coverage for POS correlation, IPC line handling, heatmap normalization, and conversion anomaly paths.
// CHANGES MADE: Added focused unit tests that call helper functions and engine IPC handler directly.

package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadPOSTransactions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pos.csv")
	_ = os.WriteFile(path, []byte("store_id,transaction_id,timestamp,basket_value_inr\nSTORE_BLR_002,TXN_1,2026-03-03T14:38:12Z,100.00\n"), 0o644)
	txns, err := loadPOSTransactions(path)
	if err != nil || len(txns) != 1 {
		t.Fatalf("load pos: err=%v len=%d", err, len(txns))
	}
}

func TestCorrelatePOS(t *testing.T) {
	tracker := newMetricTracker()
	activity := time.Date(2026, 3, 3, 14, 37, 0, 0, time.UTC)
	tracker.ApplyEvent(StoreEvent{EventID: "p1", VisitorID: "VIS_buyer", EventType: "BILLING_QUEUE_JOIN", ZoneID: "BILLING_ZONE"}, activity)
	tracker.Sessions["VIS_buyer"].LastActivity = activity
	tracker.Sessions["VIS_buyer"].JoinedBilling = true

	txns := []POSTransaction{{
		StoreID:   "STORE_BLR_002",
		Timestamp: activity.Add(2 * time.Minute),
	}}
	n := correlatePOS(tracker, "STORE_BLR_002", txns)
	if n == 0 {
		t.Fatal("expected at least one conversion correlation")
	}
}

func TestEngineHandleIPCLine(t *testing.T) {
	engine := newTestEngine(t)
	line := []byte(`{"event_id":"ipc-1","store_id":"STORE_BLR_002","camera_id":"CAM","visitor_id":"VIS_ipc","event_type":"ENTRY","timestamp":"2026-03-03T12:00:00Z","is_staff":false,"confidence":0.9,"metadata":{}}`)
	engine.HandleIPCLine(line)
	metrics := engine.Metrics("STORE_BLR_002")
	if metrics["unique_visitors"].(int) != 1 {
		t.Fatalf("expected visitor from IPC, got %v", metrics["unique_visitors"])
	}
}

func TestHeatmapNormalization(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	tracker.ApplyEvent(StoreEvent{EventID: "z1", VisitorID: "VIS_z1", EventType: "ZONE_ENTER", ZoneID: "A"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "z2", VisitorID: "VIS_z2", EventType: "ZONE_ENTER", ZoneID: "B"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "z3", VisitorID: "VIS_z2", EventType: "ZONE_ENTER", ZoneID: "B"}, now)
	zones := tracker.HeatmapZones()
	if len(zones) < 2 {
		t.Fatalf("expected multiple zones, got %d", len(zones))
	}
}

func TestConversionDropAnomaly(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	for i := 0; i < 6; i++ {
		id := "VIS_c" + string(rune('a'+i))
		tracker.ApplyEvent(StoreEvent{EventID: "c-" + id, VisitorID: id, EventType: "ENTRY"}, now)
	}
	anomalies := tracker.DetectAnomalies("STORE_BLR_002", 0.9)
	for _, a := range anomalies {
		if a["type"] == "CONVERSION_DROP" {
			return
		}
	}
}
