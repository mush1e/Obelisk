package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// BrokerMetrics holds all Prometheus metrics for Obelisk
type BrokerMetrics struct {
	// Message throughput
	MessagesReceived prometheus.CounterVec
	MessagesStored   prometheus.CounterVec
	MessagesFailed   prometheus.CounterVec

	// Batching metrics
	BatchSize     prometheus.HistogramVec
	BatchFlushes  prometheus.CounterVec
	FlushDuration prometheus.HistogramVec

	// Buffer metrics
	BufferSize      prometheus.GaugeVec
	BufferOverflows prometheus.CounterVec

	// Consumer metrics
	ConsumerPolls   prometheus.CounterVec
	ConsumerCommits prometheus.CounterVec
	ConsumerLag     prometheus.GaugeVec

	// Storage metrics
	DiskWrites     prometheus.CounterVec
	DiskWriteBytes prometheus.CounterVec
	IndexRebuilds  prometheus.CounterVec

	// Connection metrics
	ActiveConnections prometheus.Gauge
	ConnectionsTotal  prometheus.Counter
	ConnectionErrors  prometheus.CounterVec
}

// NewBrokerMetrics creates and registers all Prometheus metrics
func NewBrokerMetrics() *BrokerMetrics {
	return &BrokerMetrics{
		MessagesReceived: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_messages_received_total",
				Help: "Total number of messages received by topic",
			},
			[]string{"topic"},
		),

		MessagesStored: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_messages_stored_total",
				Help: "Total number of messages successfully stored",
			},
			[]string{"topic"},
		),

		MessagesFailed: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_messages_failed_total",
				Help: "Total number of failed message operations",
			},
			[]string{"topic", "reason"},
		),

		BatchSize: *promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "obelisk_batch_size",
				Help:    "Size of batches flushed to disk",
				Buckets: prometheus.LinearBuckets(10, 10, 10), // 10, 20, 30... 100
			},
			[]string{"topic"},
		),

		BatchFlushes: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_batch_flushes_total",
				Help: "Total number of batch flushes",
			},
			[]string{"topic", "trigger"}, // trigger: size, time
		),

		FlushDuration: *promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "obelisk_flush_duration_seconds",
				Help:    "Time spent flushing batches to disk",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"topic"},
		),

		BufferSize: *promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "obelisk_buffer_messages",
				Help: "Current number of messages in topic buffers",
			},
			[]string{"topic"},
		),

		BufferOverflows: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_buffer_overflows_total",
				Help: "Number of times buffer overflowed (messages lost)",
			},
			[]string{"topic"},
		),

		ConsumerPolls: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_consumer_polls_total",
				Help: "Total number of consumer poll operations",
			},
			[]string{"consumer_id", "topic"},
		),

		ConsumerCommits: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_consumer_commits_total",
				Help: "Total number of consumer commits",
			},
			[]string{"consumer_id", "topic"},
		),

		ConsumerLag: *promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "obelisk_consumer_lag_messages",
				Help: "Number of messages behind the latest offset",
			},
			[]string{"consumer_id", "topic"},
		),

		DiskWrites: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_disk_writes_total",
				Help: "Total number of disk write operations",
			},
			[]string{"topic"},
		),

		DiskWriteBytes: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_disk_write_bytes_total",
				Help: "Total bytes written to disk",
			},
			[]string{"topic"},
		),

		IndexRebuilds: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_index_rebuilds_total",
				Help: "Total number of index rebuild operations",
			},
			[]string{"topic", "reason"}, // reason: corruption, startup
		),

		ActiveConnections: promauto.NewGauge(
			prometheus.GaugeOpts{
				Name: "obelisk_active_connections",
				Help: "Current number of active TCP connections",
			},
		),

		ConnectionsTotal: promauto.NewCounter(
			prometheus.CounterOpts{
				Name: "obelisk_connections_total",
				Help: "Total number of TCP connections established",
			},
		),

		ConnectionErrors: *promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "obelisk_connection_errors_total",
				Help: "Total number of connection errors",
			},
			[]string{"error_type"}, // error_type: timeout, protocol, etc.
		),
	}
}

// Global metrics instance (initialized in main)
var Metrics *BrokerMetrics

// InitMetrics initializes the global metrics instance
func InitMetrics() {
	Metrics = NewBrokerMetrics()
}
