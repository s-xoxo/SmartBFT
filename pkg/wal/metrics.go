package wal

import (
	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
)

var countOfFilesOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "wal_count_of_files",
	Help:         "Count of wal-files.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// latencyAppendOpts measures one Append call: marshaling the record, writing it and the fsync
// that follows every record, and switching files when the current one is full. The time spent
// waiting for the WAL mutex is not included.
var latencyAppendOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "wal_latency_append",
	Help:         "Time it takes to append a record to the WAL, including the fsync.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// Metrics encapsulates wal metrics
type Metrics struct {
	CountOfFiles  metrics.Gauge
	LatencyAppend metrics.Histogram
}

// NewMetrics create new wal metrics
func NewMetrics(p metrics.Provider, labelNames ...string) *Metrics {
	countOfFilesOptsTmp := api.NewGaugeOpts(countOfFilesOpts, labelNames)
	latencyAppendOptsTmp := api.NewHistogramOpts(latencyAppendOpts, labelNames)
	return &Metrics{
		CountOfFiles:  p.NewGauge(countOfFilesOptsTmp),
		LatencyAppend: p.NewHistogram(latencyAppendOptsTmp),
	}
}

func (m *Metrics) With(labelValues ...string) *Metrics {
	return &Metrics{
		CountOfFiles:  m.CountOfFiles.With(labelValues...),
		LatencyAppend: m.LatencyAppend.With(labelValues...),
	}
}

func (m *Metrics) Initialize() {
	m.CountOfFiles.Add(0)
	m.LatencyAppend.Observe(0)
}
