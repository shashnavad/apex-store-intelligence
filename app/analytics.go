package main

import (
	"sync"
	"time"
)

// SessionState tracks one visitor session for funnel and conversion logic.
type SessionState struct {
	Entered       bool
	VisitedZone   bool
	JoinedBilling bool
	Converted     bool
	Closed        bool
	LastZone      string
	LastActivity  time.Time
}

// MetricTracker holds thread-safe in-memory rollups per store.
type MetricTracker struct {
	sync.RWMutex
	UniqueVisitors  map[string]struct{}
	ZoneDwellSums   map[string]int64
	ZoneHitCounts   map[string]int64
	ZoneLastVisit   map[string]time.Time
	ActiveQueueSize int
	TotalAbandons   int
	TotalEntries    int
	TotalExits      int
	LastIngestTime  time.Time
	Sessions        map[string]*SessionState
}

func newMetricTracker() *MetricTracker {
	return &MetricTracker{
		UniqueVisitors: make(map[string]struct{}),
		ZoneDwellSums:  make(map[string]int64),
		ZoneHitCounts:  make(map[string]int64),
		ZoneLastVisit:  make(map[string]time.Time),
		Sessions:       make(map[string]*SessionState),
	}
}

func (m *MetricTracker) sessionFor(visitorID string) *SessionState {
	s, ok := m.Sessions[visitorID]
	if !ok {
		s = &SessionState{}
		m.Sessions[visitorID] = s
	}
	return s
}

// ApplyEvent updates in-memory aggregates. Staff events are persisted but excluded from customer metrics.
func (m *MetricTracker) ApplyEvent(ev StoreEvent, ts time.Time) {
	if ev.IsStaff {
		return
	}

	m.LastIngestTime = ts
	m.UniqueVisitors[ev.VisitorID] = struct{}{}
	sess := m.sessionFor(ev.VisitorID)
	sess.LastActivity = ts

	switch ev.EventType {
	case "ENTRY":
		m.TotalEntries++
		sess.Entered = true
	case "REENTRY":
		sess.Entered = true
	case "EXIT":
		m.TotalExits++
		sess.Closed = true
		delete(m.UniqueVisitors, ev.VisitorID)
	case "ZONE_ENTER", "ZONE_DWELL":
		if ev.ZoneID != "" {
			m.ZoneHitCounts[ev.ZoneID]++
			m.ZoneLastVisit[ev.ZoneID] = ts
			sess.VisitedZone = true
			sess.LastZone = ev.ZoneID
		}
		if ev.EventType == "ZONE_DWELL" && ev.ZoneID != "" {
			m.ZoneDwellSums[ev.ZoneID] += int64(ev.DwellMs)
		}
	case "ZONE_EXIT":
		if ev.ZoneID != "" {
			m.ZoneDwellSums[ev.ZoneID] += int64(ev.DwellMs)
		}
	case "BILLING_QUEUE_JOIN":
		m.ActiveQueueSize++
		sess.JoinedBilling = true
	case "BILLING_QUEUE_ABANDON":
		m.TotalAbandons++
		if m.ActiveQueueSize > 0 {
			m.ActiveQueueSize--
		}
	}
}

func (m *MetricTracker) MarkConverted(visitorID string) {
	sess := m.sessionFor(visitorID)
	sess.Converted = true
}

func (m *MetricTracker) UniqueVisitorCount() int {
	return len(m.UniqueVisitors)
}

func (m *MetricTracker) DataConfidence() bool {
	return m.UniqueVisitorCount() >= 20
}

func (m *MetricTracker) ConversionRate(converted int) float64 {
	total := m.UniqueVisitorCount()
	if total == 0 {
		return 0
	}
	return float64(converted) / float64(total)
}

func (m *MetricTracker) FunnelStages() []map[string]interface{} {
	entries := 0
	zoneVisits := 0
	billing := 0
	purchases := 0

	for _, s := range m.Sessions {
		if s.Entered {
			entries++
		}
		if s.VisitedZone {
			zoneVisits++
		}
		if s.JoinedBilling {
			billing++
		}
		if s.Converted {
			purchases++
		}
	}

	return []map[string]interface{}{
		{"stage": "ENTRY", "count": entries, "dropoff_pct": dropoff(entries, entries)},
		{"stage": "ZONE_VISIT", "count": zoneVisits, "dropoff_pct": dropoff(entries, zoneVisits)},
		{"stage": "BILLING_QUEUE", "count": billing, "dropoff_pct": dropoff(zoneVisits, billing)},
		{"stage": "PURCHASE", "count": purchases, "dropoff_pct": dropoff(billing, purchases)},
	}
}

func dropoff(from, to int) float64 {
	if from == 0 {
		return 0
	}
	if to >= from {
		return 0
	}
	return (float64(from-to) / float64(from)) * 100
}

func (m *MetricTracker) HeatmapZones() []map[string]interface{} {
	maxHits := int64(1)
	for _, hits := range m.ZoneHitCounts {
		if hits > maxHits {
			maxHits = hits
		}
	}

	var zones []map[string]interface{}
	for zone, hits := range m.ZoneHitCounts {
		avgDwell := int64(0)
		if hits > 0 {
			avgDwell = m.ZoneDwellSums[zone] / hits
		}
		normalized := int((float64(hits) / float64(maxHits)) * 100)
		zones = append(zones, map[string]interface{}{
			"zone_id":          zone,
			"visit_frequency":  hits,
			"avg_dwell_ms":     avgDwell,
			"normalized_score": normalized,
		})
	}
	return zones
}

func (m *MetricTracker) DetectAnomalies(storeID string, baselineConversion float64) []map[string]interface{} {
	var out []map[string]interface{}

	if m.ActiveQueueSize > 10 {
		out = append(out, map[string]interface{}{
			"type":             "BILLING_QUEUE_SPIKE",
			"severity":         severityForQueue(m.ActiveQueueSize),
			"suggested_action": "Open an additional billing counter to reduce wait time",
			"store_id":         storeID,
		})
	}

	current := m.ConversionRate(countConverted(m.Sessions))
	if baselineConversion > 0 && current < baselineConversion*0.7 && m.UniqueVisitorCount() >= 5 {
		out = append(out, map[string]interface{}{
			"type":             "CONVERSION_DROP",
			"severity":         "WARN",
			"suggested_action": "Review staffing and in-store promotions for underperforming zones",
			"store_id":         storeID,
		})
	}

	now := time.Now()
	for zone, last := range m.ZoneLastVisit {
		if !last.IsZero() && now.Sub(last) > 30*time.Minute {
			out = append(out, map[string]interface{}{
				"type":             "DEAD_ZONE",
				"severity":         "INFO",
				"suggested_action": "Inspect camera coverage or merchandising for zone " + zone,
				"store_id":         storeID,
				"zone_id":          zone,
			})
		}
	}

	return out
}

func severityForQueue(depth int) string {
	if depth > 15 {
		return "CRITICAL"
	}
	if depth > 10 {
		return "WARN"
	}
	return "INFO"
}

func countConverted(sessions map[string]*SessionState) int {
	n := 0
	for _, s := range sessions {
		if s.Converted {
			n++
		}
	}
	return n
}
