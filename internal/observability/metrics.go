package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Metrics struct {
	HTTPRequests       *prometheus.CounterVec
	HTTPRequestSeconds *prometheus.HistogramVec
	OrdersCreated      prometheus.Counter
	KafkaProcessed     *prometheus.CounterVec
	WorkerQueueDepth   prometheus.Gauge
}

func NewMetrics(registry prometheus.Registerer) *Metrics {
	metrics := &Metrics{
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fulfill_http_requests_total",
			Help: "Total number of HTTP requests.",
		}, []string{"method", "route", "status"}),
		HTTPRequestSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "fulfill_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
		OrdersCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "fulfill_orders_created_total",
			Help: "Total number of newly created orders.",
		}),
		KafkaProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "fulfill_kafka_messages_total",
			Help: "Kafka messages handled by result.",
		}, []string{"result"}),
		WorkerQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "fulfill_worker_queue_depth",
			Help: "Current number of queued Kafka messages.",
		}),
	}
	registry.MustRegister(
		metrics.HTTPRequests,
		metrics.HTTPRequestSeconds,
		metrics.OrdersCreated,
		metrics.KafkaProcessed,
		metrics.WorkerQueueDepth,
	)
	return metrics
}

func (m *Metrics) ObserveHTTP(method, route string, status int, started time.Time) {
	m.HTTPRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.HTTPRequestSeconds.WithLabelValues(method, route).Observe(time.Since(started).Seconds())
}
