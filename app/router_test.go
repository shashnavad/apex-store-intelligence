// PROMPT: Add httptest coverage for all REST endpoints including metrics, funnel, heatmap, anomalies, health, and prometheus metrics.
// CHANGES MADE: Reused newRouter helper to exercise handler paths without starting the unix socket server.

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func testRouter(t *testing.T) *gin.Engine {
	t.Helper()
	gin.SetMode(gin.TestMode)
	engine := newTestEngine(t)
	return newRouter(engine)
}

func TestRouterEndpoints(t *testing.T) {
	router := testRouter(t)
	event := StoreEvent{
		EventID: "route-1", StoreID: "STORE_BLR_002", CameraID: "CAM",
		VisitorID: "VIS_route", EventType: "ENTRY", Timestamp: "2026-03-03T10:00:00Z",
	}
	body, _ := json.Marshal([]StoreEvent{event})
	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/events/ingest", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("ingest status %d", w.Code)
	}

	for _, path := range []string{
		"/stores/STORE_BLR_002/metrics",
		"/stores/STORE_BLR_002/funnel",
		"/stores/STORE_BLR_002/heatmap",
		"/stores/STORE_BLR_002/anomalies",
		"/dashboard/STORE_BLR_002",
		"/health",
		"/metrics",
	} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("%s returned %d", path, w.Code)
		}
	}
}
