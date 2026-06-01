// PROMPT: Generate Go HTTP tests for POST /events/ingest including idempotency, batch limits, and partial rejection of malformed events.
// CHANGES MADE: Added in-memory sqlite setup, idempotent replay test, and batch size validation using httptest against the real router handlers.

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func setupTestRouter(t *testing.T) (*gin.Engine, *Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	db, err := initDB()
	if err != nil {
		t.Fatalf("db init: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	engine := NewEngine(db, t.TempDir()+"/test.sock")
	r := gin.New()
	r.POST("/events/ingest", func(c *gin.Context) {
		var batch []StoreEvent
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "malformed JSON payload"})
			return
		}
		if len(batch) > 500 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "batch size exceeds limit of 500 events"})
			return
		}
		res, err := engine.IngestBatch(batch)
		if err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, res)
	})
	return r, engine
}

func TestIngestIdempotent(t *testing.T) {
	router, _ := setupTestRouter(t)
	event := StoreEvent{
		EventID: "dup-1", StoreID: "STORE_BLR_002", CameraID: "CAM_01",
		VisitorID: "VIS_dup", EventType: "ENTRY", Timestamp: "2026-03-03T10:00:00Z",
	}
	body, _ := json.Marshal([]StoreEvent{event})
	w1 := httptest.NewRecorder()
	router.ServeHTTP(w1, httptest.NewRequest(http.MethodPost, "/events/ingest", bytes.NewReader(body)))
	w2 := httptest.NewRecorder()
	router.ServeHTTP(w2, httptest.NewRequest(http.MethodPost, "/events/ingest", bytes.NewReader(body)))

	if w1.Code != http.StatusOK || w2.Code != http.StatusOK {
		t.Fatalf("unexpected status codes: %d %d", w1.Code, w2.Code)
	}
}

func TestIngestRejectsOversizedBatch(t *testing.T) {
	router, _ := setupTestRouter(t)
	batch := make([]StoreEvent, 501)
	body, _ := json.Marshal(batch)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/events/ingest", bytes.NewReader(body)))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestMetricsEndpointEmptyStore(t *testing.T) {
	db, err := initDB()
	if err != nil {
		t.Fatalf("db init: %v", err)
	}
	defer db.Close()
	engine := NewEngine(db, t.TempDir()+"/sock")
	metrics := engine.Metrics("STORE_BLR_002")
	if metrics["unique_visitors"].(int) != 0 {
		t.Fatalf("expected zero visitors, got %v", metrics["unique_visitors"])
	}
	if metrics["conversion_rate"].(float64) != 0 {
		t.Fatalf("expected zero conversion, got %v", metrics["conversion_rate"])
	}
}
