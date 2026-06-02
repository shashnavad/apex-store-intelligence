// PROMPT: Add Go edge case tests for REENTRY funnel deduplication, all-staff ingest, and HTTP 503 when the database is unavailable.
// CHANGES MADE: Exercises newRouter handlers with closed SQLite connections and session funnel assertions after EXIT plus REENTRY.

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestReentryFunnelDoesNotDoubleCountEntry(t *testing.T) {
	engine := newTestEngine(t)
	visitor := "VIS_reentry_funnel"
	events := []StoreEvent{
		{EventID: "re-1", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: visitor, EventType: "ENTRY", Timestamp: "2026-03-03T10:00:00Z"},
		{EventID: "re-2", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: visitor, EventType: "EXIT", Timestamp: "2026-03-03T10:05:00Z"},
		{EventID: "re-3", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: visitor, EventType: "REENTRY", Timestamp: "2026-03-03T10:10:00Z"},
	}
	if _, err := engine.IngestBatch(events); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	funnel := engine.Funnel("STORE_BLR_002")
	stages, ok := funnel["stages"].([]map[string]interface{})
	if !ok {
		t.Fatalf("unexpected stages type: %T", funnel["stages"])
	}

	var entryCount int
	for _, stage := range stages {
		if stage["stage"] == "ENTRY" {
			entryCount = stage["count"].(int)
		}
	}
	if entryCount != 1 {
		t.Fatalf("expected single ENTRY funnel count after REENTRY, got %d", entryCount)
	}
}

func TestAllStaffBatchZeroUniqueVisitors(t *testing.T) {
	engine := newTestEngine(t)
	batch := []StoreEvent{
		{EventID: "staff-1", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: "VIS_staff_a", EventType: "ENTRY", Timestamp: "2026-03-03T11:00:00Z", IsStaff: true},
		{EventID: "staff-2", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: "VIS_staff_b", EventType: "ZONE_ENTER", ZoneID: "SKINCARE", Timestamp: "2026-03-03T11:01:00Z", IsStaff: true},
		{EventID: "staff-3", StoreID: "STORE_BLR_002", CameraID: "CAM", VisitorID: "VIS_staff_c", EventType: "ZONE_DWELL", ZoneID: "SKINCARE", DwellMs: 60000, Timestamp: "2026-03-03T11:02:00Z", IsStaff: true},
	}
	if _, err := engine.IngestBatch(batch); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	metrics := engine.Metrics("STORE_BLR_002")
	if metrics["unique_visitors"].(int) != 0 {
		t.Fatalf("expected 0 unique visitors for all-staff batch, got %v", metrics["unique_visitors"])
	}
}

func TestIngestReturns503WhenDBUnavailable(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	db, err := initDB()
	if err != nil {
		t.Fatalf("initDB: %v", err)
	}
	_ = db.Close()

	engine := NewEngine(db, t.TempDir()+"/closed.sock")
	router := newRouter(engine)

	event := StoreEvent{
		EventID: "503-1", StoreID: "STORE_BLR_002", CameraID: "CAM",
		VisitorID: "VIS_503", EventType: "ENTRY", Timestamp: "2026-03-03T12:00:00Z",
	}
	body, _ := json.Marshal([]StoreEvent{event})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/events/ingest", bytes.NewReader(body)))

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d body=%s", w.Code, w.Body.String())
	}
}

func TestReentryRestoresActiveVisitor(t *testing.T) {
	tracker := newMetricTracker()
	now := mustParseTime(t, "2026-03-03T10:00:00Z")
	tracker.ApplyEvent(StoreEvent{EventID: "r1", VisitorID: "VIS_r", EventType: "ENTRY"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "r2", VisitorID: "VIS_r", EventType: "EXIT"}, now.Add(5*60))
	if tracker.UniqueVisitorCount() != 0 {
		t.Fatalf("expected visitor removed on EXIT, got %d", tracker.UniqueVisitorCount())
	}
	tracker.ApplyEvent(StoreEvent{EventID: "r3", VisitorID: "VIS_r", EventType: "REENTRY"}, now.Add(10*60))
	if tracker.UniqueVisitorCount() != 1 {
		t.Fatalf("expected visitor active again after REENTRY, got %d", tracker.UniqueVisitorCount())
	}
}

func mustParseTime(t *testing.T, raw string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse time: %v", err)
	}
	return parsed
}
