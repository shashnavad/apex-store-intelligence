package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

func dbPath() string {
	if p := os.Getenv("DB_PATH"); p != "" {
		return p
	}
	dataDir := os.Getenv("DATA_DIR")
	if dataDir == "" {
		dataDir = "./data"
	}
	_ = os.MkdirAll(dataDir, 0o755)
	return filepath.Join(dataDir, "store_intelligence.db")
}

func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath()+"?_busy_timeout=5000")
	if err != nil {
		return nil, err
	}

	pragmaSettings := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=NORMAL;",
		"PRAGMA cache_size=-64000;",
		"PRAGMA foreign_keys=ON;",
	}
	for _, pragma := range pragmaSettings {
		if _, err := db.Exec(pragma); err != nil {
			return nil, err
		}
	}

	schema := `
	CREATE TABLE IF NOT EXISTS store_events (
		event_id TEXT PRIMARY KEY,
		store_id TEXT NOT NULL,
		camera_id TEXT NOT NULL,
		visitor_id TEXT NOT NULL,
		event_type TEXT NOT NULL,
		timestamp TEXT NOT NULL,
		zone_id TEXT,
		dwell_ms INTEGER,
		is_staff INTEGER,
		confidence REAL,
		metadata_json TEXT
	);
	CREATE INDEX IF NOT EXISTS idx_store_events_store ON store_events(store_id);
	CREATE INDEX IF NOT EXISTS idx_store_events_ts ON store_events(timestamp);
	`
	if _, err := db.Exec(schema); err != nil {
		return nil, err
	}
	return db, nil
}

func persistEvent(db *sql.DB, ev StoreEvent) error {
	meta, _ := json.Marshal(ev.Metadata)
	_, err := db.Exec(`
		INSERT OR IGNORE INTO store_events
		(event_id, store_id, camera_id, visitor_id, event_type, timestamp, zone_id, dwell_ms, is_staff, confidence, metadata_json)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ev.EventID, ev.StoreID, ev.CameraID, ev.VisitorID, ev.EventType, ev.Timestamp,
		ev.ZoneID, ev.DwellMs, boolToInt(ev.IsStaff), ev.Confidence, string(meta),
	)
	return err
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func pingDB(db *sql.DB) error {
	return db.Ping()
}

func logDBInitFailure(err error) {
	log.Printf("database initialization failed: %v", err)
}
