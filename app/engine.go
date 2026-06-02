package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Engine coordinates persistence, idempotency, and per-store trackers.
type Engine struct {
	db             *sql.DB
	trackers       map[string]*MetricTracker
	idempotency    sync.Map
	posTxns        []POSTransaction
	baselineConv   map[string]float64
	mu             sync.RWMutex
	socketPath     string
	eventsIngested int64
}

func NewEngine(db *sql.DB, socketPath string) *Engine {
	e := &Engine{
		db:           db,
		trackers:     make(map[string]*MetricTracker),
		baselineConv: make(map[string]float64),
		socketPath:   socketPath,
	}
	if txns, err := loadPOSTransactions("data/pos_transactions.csv"); err == nil {
		e.posTxns = txns
	}
	e.baselineConv["STORE_BLR_002"] = 0.25
	return e
}

func (e *Engine) tracker(storeID string) *MetricTracker {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.trackers[storeID]
	if !ok {
		t = newMetricTracker()
		e.trackers[storeID] = t
	}
	return t
}

type IngestResult struct {
	Accepted int      `json:"accepted"`
	Rejected int      `json:"rejected"`
	Errors   []string `json:"errors,omitempty"`
}

func (e *Engine) ProcessEvent(ev StoreEvent) error {
	if ev.EventID == "" || ev.StoreID == "" || ev.VisitorID == "" || ev.EventType == "" || ev.Timestamp == "" {
		return fmt.Errorf("missing required fields")
	}

	if _, loaded := e.idempotency.Load(ev.EventID); loaded {
		return nil
	}

	if err := pingDB(e.db); err != nil {
		return fmt.Errorf("database unavailable: %w", err)
	}

	if err := persistEvent(e.db, ev); err != nil {
		return err
	}
	e.idempotency.Store(ev.EventID, true)

	ts, err := time.Parse(time.RFC3339, ev.Timestamp)
	if err != nil {
		ts = time.Now().UTC()
	}

	tracker := e.tracker(ev.StoreID)
	tracker.Lock()
	tracker.ApplyEvent(ev, ts)
	if ev.EventType == "BILLING_QUEUE_JOIN" {
		correlatePOS(tracker, ev.StoreID, e.posTxns)
	}
	tracker.Unlock()

	e.eventsIngested++
	return nil
}

func (e *Engine) IngestBatch(batch []StoreEvent) (IngestResult, error) {
	if err := pingDB(e.db); err != nil {
		return IngestResult{}, fmt.Errorf("database unavailable: %w", err)
	}

	res := IngestResult{}
	for _, ev := range batch {
		if err := e.ProcessEvent(ev); err != nil {
			res.Rejected++
			res.Errors = append(res.Errors, fmt.Sprintf("%s: %s", ev.EventID, err.Error()))
			continue
		}
		res.Accepted++
	}
	return res, nil
}

func (e *Engine) Metrics(storeID string) map[string]interface{} {
	tracker := e.tracker(storeID)
	tracker.RLock()
	defer tracker.RUnlock()

	converted := countConverted(tracker.Sessions)
	abandonRate := 0.0
	if tracker.TotalAbandons+tracker.ActiveQueueSize > 0 {
		abandonRate = float64(tracker.TotalAbandons) / float64(tracker.TotalAbandons+tracker.ActiveQueueSize+1)
	}

	zoneDwell := map[string]float64{}
	for z, sum := range tracker.ZoneDwellSums {
		hits := tracker.ZoneHitCounts[z]
		if hits > 0 {
			zoneDwell[z] = float64(sum) / float64(hits)
		}
	}

	return map[string]interface{}{
		"store_id":           storeID,
		"unique_visitors":    tracker.UniqueVisitorCount(),
		"conversion_rate":    tracker.ConversionRate(converted),
		"avg_dwell_per_zone": zoneDwell,
		"queue_depth":        tracker.ActiveQueueSize,
		"abandonment_rate":   abandonRate,
		"data_confidence":    tracker.DataConfidence(),
	}
}

func (e *Engine) Funnel(storeID string) map[string]interface{} {
	tracker := e.tracker(storeID)
	tracker.RLock()
	defer tracker.RUnlock()
	return map[string]interface{}{
		"store_id": storeID,
		"stages":   tracker.FunnelStages(),
	}
}

func (e *Engine) Heatmap(storeID string) map[string]interface{} {
	tracker := e.tracker(storeID)
	tracker.RLock()
	defer tracker.RUnlock()
	return map[string]interface{}{
		"store_id":        storeID,
		"zones":           tracker.HeatmapZones(),
		"data_confidence": tracker.DataConfidence(),
	}
}

func (e *Engine) Anomalies(storeID string) []map[string]interface{} {
	tracker := e.tracker(storeID)
	tracker.RLock()
	defer tracker.RUnlock()
	baseline := e.baselineConv[storeID]
	if baseline == 0 {
		baseline = 0.2
	}
	return tracker.DetectAnomalies(storeID, baseline)
}

func (e *Engine) Health() map[string]interface{} {
	e.mu.RLock()
	defer e.mu.RUnlock()

	stores := map[string]string{}
	status := "HEALTHY"
	now := time.Now()

	for storeID, tr := range e.trackers {
		tr.RLock()
		last := tr.LastIngestTime
		tr.RUnlock()
		if last.IsZero() {
			stores[storeID] = ""
			continue
		}
		stores[storeID] = last.UTC().Format(time.RFC3339)
		if now.Sub(last) > 10*time.Minute {
			status = "STALE_FEED"
		}
	}

	dbStatus := "CONNECTED"
	if err := pingDB(e.db); err != nil {
		dbStatus = "UNAVAILABLE"
		status = "DEGRADED"
	}

	return map[string]interface{}{
		"status":                status,
		"last_event_by_store":   stores,
		"database_connection":   dbStatus,
		"events_ingested_total": e.eventsIngested,
	}
}

func (e *Engine) HandleIPCLine(line []byte) {
	var ev StoreEvent
	if err := json.Unmarshal(line, &ev); err != nil {
		return
	}
	_ = e.ProcessEvent(ev)
}
