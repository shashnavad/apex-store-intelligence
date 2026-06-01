// PROMPT: Create anomaly detection tests for queue spike, conversion drop, and dead zone scenarios in the Go analytics layer.
// CHANGES MADE: Lowered queue thresholds for unit tests and seeded zone timestamps to trigger DEAD_ZONE without external services.

package main

import (
	"testing"
	"time"
)

func TestQueueSpikeAnomaly(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	for i := 0; i < 12; i++ {
		tracker.ApplyEvent(StoreEvent{
			EventID:   "q-" + string(rune('a'+i)),
			VisitorID: "VIS_q" + string(rune('a'+i)),
			EventType: "BILLING_QUEUE_JOIN",
			ZoneID:    "BILLING_ZONE",
		}, now)
	}
	anomalies := tracker.DetectAnomalies("STORE_BLR_002", 0.25)
	if len(anomalies) == 0 {
		t.Fatal("expected queue spike anomaly")
	}
}

func TestDeadZoneAnomaly(t *testing.T) {
	tracker := newMetricTracker()
	old := time.Now().Add(-45 * time.Minute)
	tracker.ZoneLastVisit["SKINCARE"] = old
	anomalies := tracker.DetectAnomalies("STORE_BLR_002", 0.25)
	found := false
	for _, a := range anomalies {
		if a["type"] == "DEAD_ZONE" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected dead zone anomaly")
	}
}
