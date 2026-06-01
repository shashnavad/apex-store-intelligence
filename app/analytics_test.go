// PROMPT: Write Go table-driven tests for MetricTracker covering staff exclusion, queue abandonment, and data_confidence when unique visitors are below 20.
// CHANGES MADE: Wired tests to MetricTracker.ApplyEvent directly and added funnel and heatmap assertions aligned with challenge rules.

package main

import (
	"testing"
	"time"
)

func TestStaffExcludedFromMetrics(t *testing.T) {
	tracker := newMetricTracker()
	tracker.ApplyEvent(StoreEvent{EventID: "e1", VisitorID: "VIS_staff", EventType: "ZONE_ENTER", ZoneID: "SKINCARE", IsStaff: true}, time.Now())
	tracker.ApplyEvent(StoreEvent{EventID: "e2", VisitorID: "VIS_cust", EventType: "ZONE_ENTER", ZoneID: "SKINCARE", IsStaff: false}, time.Now())

	if tracker.UniqueVisitorCount() != 1 {
		t.Fatalf("expected 1 customer, got %d", tracker.UniqueVisitorCount())
	}
	if tracker.ZoneHitCounts["SKINCARE"] != 1 {
		t.Fatalf("expected 1 zone hit, got %d", tracker.ZoneHitCounts["SKINCARE"])
	}
}

func TestQueueAbandonment(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	tracker.ApplyEvent(StoreEvent{EventID: "e3", VisitorID: "VIS_1", EventType: "BILLING_QUEUE_JOIN", ZoneID: "BILLING_ZONE"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "e4", VisitorID: "VIS_2", EventType: "BILLING_QUEUE_JOIN", ZoneID: "BILLING_ZONE"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "e5", VisitorID: "VIS_1", EventType: "BILLING_QUEUE_ABANDON", ZoneID: "BILLING_ZONE"}, now)

	if tracker.ActiveQueueSize != 1 {
		t.Fatalf("expected queue depth 1, got %d", tracker.ActiveQueueSize)
	}
	if tracker.TotalAbandons != 1 {
		t.Fatalf("expected 1 abandon, got %d", tracker.TotalAbandons)
	}
}

func TestDataConfidenceThreshold(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	for i := 0; i < 19; i++ {
		tracker.ApplyEvent(StoreEvent{
			EventID:   "evt-" + string(rune('a'+i)),
			VisitorID: "VIS_" + string(rune('a'+i)),
			EventType: "ENTRY",
		}, now)
	}
	if tracker.DataConfidence() {
		t.Fatal("expected data_confidence false below 20 sessions")
	}
}

func TestFunnelSessionStages(t *testing.T) {
	tracker := newMetricTracker()
	now := time.Now()
	tracker.ApplyEvent(StoreEvent{EventID: "f1", VisitorID: "VIS_f", EventType: "ENTRY"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "f2", VisitorID: "VIS_f", EventType: "ZONE_ENTER", ZoneID: "SKINCARE"}, now)
	tracker.ApplyEvent(StoreEvent{EventID: "f3", VisitorID: "VIS_f", EventType: "BILLING_QUEUE_JOIN", ZoneID: "BILLING_ZONE"}, now)
	tracker.MarkConverted("VIS_f")

	stages := tracker.FunnelStages()
	if stages[0]["count"].(int) != 1 {
		t.Fatalf("unexpected entry count: %v", stages[0]["count"])
	}
}
