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

		res, err := engine.IngestBatch(batch)
		requestLatency.WithLabelValues("/events/ingest").Observe(float64(time.Since(start).Milliseconds()))

		if err != nil && strings.Contains(err.Error(), "database unavailable") {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error":   "database unavailable",
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
	srv := &http.Server{Addr: ":8080", Handler: router}
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

		log.Printf(`{"trace_id":"%s","store_id":"%s","endpoint":"%s","latency_ms":%d,"event_count":%d,"status_code":%d}`,
			traceID, c.Param("id"), c.Request.URL.Path, time.Since(start).Milliseconds(), eventCount, c.Writer.Status())
	}
}

func firstStore(batch []StoreEvent) string {
	if len(batch) == 0 {
		return "unknown"
	}
	return batch[0].StoreID
}
