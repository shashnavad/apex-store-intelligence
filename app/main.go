package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	ingestCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "store_events_ingested_total", Help: "Total events ingested"},
		[]string{"store_id", "status"},
	)
	requestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "http_request_duration_ms", Help: "HTTP latency in ms", Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500}},
		[]string{"endpoint"},
	)
)

const dashboardPage = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Store Intelligence Dashboard</title>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; padding: 1rem; background: #f7fafc; color:#111; }
    header { margin-bottom: 1rem; }
    .card { background:#fff; border:1px solid #e2e8f0; border-radius:12px; padding:1rem; margin-bottom:1rem; box-shadow:0 2px 8px rgba(0,0,0,.04); }
    .grid { display:grid; gap:1rem; grid-template-columns:repeat(auto-fit, minmax(220px,1fr)); }
    pre { background:#edf2f7; padding:1rem; border-radius:8px; overflow:auto; }
  </style>
</head>
<body>
  <header>
    <h1>Store Intelligence</h1>
    <p>Live metrics for <strong id="store-id"></strong></p>
  </header>
  <div class="grid">
    <div class="card"><h2>Metrics</h2><pre id="metrics">Loading...</pre></div>
    <div class="card"><h2>Funnel</h2><pre id="funnel">Loading...</pre></div>
    <div class="card"><h2>Anomalies</h2><pre id="anomalies">Loading...</pre></div>
  </div>
  <script>
    const pathParts = window.location.pathname.split('/').filter(Boolean);
    const storeId = pathParts[pathParts.length - 1] || 'STORE_BLR_002';
    document.getElementById('store-id').textContent = storeId;

    async function fetchJson(path) {
      try {
        const res = await fetch(path);
        if (!res.ok) throw new Error(res.statusText);
        return await res.json();
      } catch (err) {
        return { error: err.message };
      }
    }

    async function refresh() {
      const metrics = await fetchJson('/stores/' + storeId + '/metrics');
      const funnel = await fetchJson('/stores/' + storeId + '/funnel');
      const anomalies = await fetchJson('/stores/' + storeId + '/anomalies');
      document.getElementById('metrics').textContent = JSON.stringify(metrics, null, 2);
      document.getElementById('funnel').textContent = JSON.stringify(funnel, null, 2);
      document.getElementById('anomalies').textContent = JSON.stringify(anomalies, null, 2);
    }
    refresh();
    setInterval(refresh, 2000);
  </script>
</body>
</html>`

func init() {
	prometheus.MustRegister(ingestCounter, requestLatency)
}

// EventMetadata matches the challenge schema metadata object.
type EventMetadata struct {
	QueueDepth *int   `json:"queue_depth"`
	SkuZone    string `json:"sku_zone"`
	SessionSeq int    `json:"session_seq"`
}

// StoreEvent matches the required detection output schema.
type StoreEvent struct {
	EventID    string        `json:"event_id" binding:"required"`
	StoreID    string        `json:"store_id" binding:"required"`
	CameraID   string        `json:"camera_id" binding:"required"`
	VisitorID  string        `json:"visitor_id" binding:"required"`
	EventType  string        `json:"event_type" binding:"required"`
	Timestamp  string        `json:"timestamp" binding:"required"`
	ZoneID     string        `json:"zone_id"`
	DwellMs    int           `json:"dwell_ms"`
	IsStaff    bool          `json:"is_staff"`
	Confidence float64       `json:"confidence"`
	Metadata   EventMetadata `json:"metadata"`
}

func newRouter(engine *Engine) *gin.Engine {
	router := gin.New()
	router.Use(structuredLogging())
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))
	router.GET("/dashboard", func(c *gin.Context) {
		c.Redirect(http.StatusFound, "/dashboard/STORE_BLR_002")
	})
	router.GET("/dashboard/:id", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(dashboardPage))
	})

	router.POST("/events/ingest", func(c *gin.Context) {
		start := time.Now()
		var batch []StoreEvent
		if err := c.ShouldBindJSON(&batch); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "malformed JSON payload", "detail": err.Error()})
			return
		}
		if len(batch) > 500 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "batch size exceeds limit of 500 events"})
			return
		}

		c.Set("store_id", firstStore(batch))
		c.Set("event_count", len(batch))
		res, err := engine.IngestBatch(batch)
		requestLatency.WithLabelValues("/events/ingest").Observe(float64(time.Since(start).Milliseconds()))

		if err != nil && strings.Contains(err.Error(), "database unavailable") {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":    "database unavailable",
				"trace_id": c.GetString("trace_id"),
			})
			return
		}

		status := http.StatusOK
		if res.Rejected > 0 && res.Accepted > 0 {
			status = http.StatusMultiStatus
		} else if res.Accepted == 0 && res.Rejected > 0 {
			status = http.StatusBadRequest
		}

		ingestCounter.WithLabelValues(firstStore(batch), "batch").Add(float64(res.Accepted))

		c.JSON(status, gin.H{
			"status":   "processed",
			"accepted": res.Accepted,
			"rejected": res.Rejected,
			"errors":   res.Errors,
		})
	})

	router.GET("/stores/:id/metrics", func(c *gin.Context) {
		start := time.Now()
		defer requestLatency.WithLabelValues("/stores/:id/metrics").Observe(float64(time.Since(start).Milliseconds()))
		c.JSON(http.StatusOK, engine.Metrics(c.Param("id")))
	})

	router.GET("/stores/:id/funnel", func(c *gin.Context) {
		c.JSON(http.StatusOK, engine.Funnel(c.Param("id")))
	})

	router.GET("/stores/:id/heatmap", func(c *gin.Context) {
		c.JSON(http.StatusOK, engine.Heatmap(c.Param("id")))
	})

	router.GET("/stores/:id/anomalies", func(c *gin.Context) {
		out := engine.Anomalies(c.Param("id"))
		if out == nil {
			out = []map[string]interface{}{}
		}
		c.JSON(http.StatusOK, out)
	})

	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, engine.Health())
	})
	return router
}

func main() {
	gin.SetMode(gin.ReleaseMode)

	db, err := initDB()
	if err != nil {
		logDBInitFailure(err)
		log.Fatalf("failed to open database: %v", err)
	}
	defer db.Close()

	socketPath := os.Getenv("IPC_SOCKET_PATH")
	if socketPath == "" {
		socketPath = "/tmp/store_intelligence.sock"
	}

	engine := NewEngine(db, socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go startUnixSocketServer(ctx, engine, socketPath)

	router := newRouter(engine)
	addr := ":8080"
	if port := os.Getenv("PORT"); port != "" {
		addr = ":" + port
	}
	srv := &http.Server{Addr: addr, Handler: router}
	go func() {
		log.Println("Store Intelligence API listening on :8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
}

func structuredLogging() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		traceID := c.GetHeader("X-Trace-ID")
		if traceID == "" {
			traceID = fmt.Sprintf("%d", start.UnixNano())
		}
		c.Set("trace_id", traceID)
		c.Next()

		eventCount := 0
		if c.Request.Method == http.MethodPost && c.Request.URL.Path == "/events/ingest" {
			if v, ok := c.Get("event_count"); ok {
				eventCount, _ = v.(int)
			}
		}

		storeID := c.Param("id")
		if storeID == "" {
			storeID = c.GetString("store_id")
		}

		log.Printf(`{"trace_id":"%s","store_id":"%s","endpoint":"%s","latency_ms":%d,"event_count":%d,"status_code":%d}`,
			traceID, storeID, c.Request.URL.Path, time.Since(start).Milliseconds(), eventCount, c.Writer.Status())
	}
}

func firstStore(batch []StoreEvent) string {
	if len(batch) == 0 {
		return "unknown"
	}
	return batch[0].StoreID
}
