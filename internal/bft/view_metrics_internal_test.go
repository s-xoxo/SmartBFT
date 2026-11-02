// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/stretchr/testify/require"
)

// viewMetricsRecorder records the values set on gauges and observed by histograms, by metric name.
type viewMetricsRecorder struct {
	disabled.Provider
	gauges     map[string]float64
	histograms map[string][]float64
}

func (p *viewMetricsRecorder) NewGauge(opts metrics.GaugeOpts) metrics.Gauge {
	return &viewRecordedGauge{recorder: p, name: opts.Name}
}

func (p *viewMetricsRecorder) NewHistogram(opts metrics.HistogramOpts) metrics.Histogram {
	return &viewRecordedHistogram{recorder: p, name: opts.Name}
}

type viewRecordedGauge struct {
	recorder *viewMetricsRecorder
	name     string
}

func (g *viewRecordedGauge) With(...string) metrics.Gauge { return g }
func (g *viewRecordedGauge) Add(delta float64)            { g.Set(g.recorder.gauges[g.name] + delta) }

func (g *viewRecordedGauge) Set(value float64) {
	if g.recorder.gauges == nil {
		g.recorder.gauges = make(map[string]float64)
	}
	g.recorder.gauges[g.name] = value
}

type viewRecordedHistogram struct {
	recorder *viewMetricsRecorder
	name     string
}

func (h *viewRecordedHistogram) With(...string) metrics.Histogram { return h }
func (h *viewRecordedHistogram) Observe(value float64) {
	if h.recorder.histograms == nil {
		h.recorder.histograms = make(map[string][]float64)
	}
	h.recorder.histograms[h.name] = append(h.recorder.histograms[h.name], value)
}

// TestViewPrePrepareLatencyPerSequence makes sure the view keeps the pre-prepare time of every
// sequence in flight separately: recording a sequence counts it as in flight, observing a sequence
// measures from that sequence's own pre-prepare, forgets only that sequence, and an unknown sequence
// observes nothing.
func TestViewPrePrepareLatencyPerSequence(t *testing.T) {
	recorder := &viewMetricsRecorder{}
	v := &View{MetricsView: api.NewMetricsView(recorder)}
	latencies := func() []float64 { return recorder.histograms["view_latency_batch_processing"] }
	inFlight := func() float64 { return recorder.gauges["view_in_flight_proposals"] }

	// Observing a sequence that was never pre-prepared observes nothing.
	v.observeBatchProcessingLatency(7)
	require.Empty(t, latencies())
	require.Zero(t, inFlight())

	// A pre-prepare records its sequence and counts it as in flight.
	v.recordPrePrepareBegin(7)
	require.Contains(t, v.prePrepareBegin, uint64(7))
	require.InDelta(t, 1.0, inFlight(), 0)

	// A second sequence in flight does not overwrite the first.
	v.recordPrePrepareBegin(8)
	require.Len(t, v.prePrepareBegin, 2)
	require.InDelta(t, 2.0, inFlight(), 0)

	// Make the first sequence old, so that the two are distinguishable.
	v.prePrepareBegin[7] = time.Now().Add(-time.Hour)

	// A commit quorum on the second sequence is measured from its own pre-prepare and forgets only it.
	v.observeBatchProcessingLatency(8)
	require.Len(t, latencies(), 1)
	require.Less(t, latencies()[0], time.Minute.Seconds())
	require.NotContains(t, v.prePrepareBegin, uint64(8))
	require.Contains(t, v.prePrepareBegin, uint64(7))

	// The first sequence is still measured from its own pre-prepare.
	v.observeBatchProcessingLatency(7)
	require.Len(t, latencies(), 2)
	require.GreaterOrEqual(t, latencies()[1], time.Hour.Seconds())
	require.Empty(t, v.prePrepareBegin)

	// A sequence is observed once.
	v.observeBatchProcessingLatency(7)
	require.Len(t, latencies(), 2)

	// Re-proposing a sequence, as a view restored from the WAL does, starts a new measurement.
	v.recordPrePrepareBegin(7)
	require.InDelta(t, 1.0, inFlight(), 0)
	v.observeBatchProcessingLatency(7)
	require.Len(t, latencies(), 3)
	require.Less(t, latencies()[2], time.Minute.Seconds())
}
