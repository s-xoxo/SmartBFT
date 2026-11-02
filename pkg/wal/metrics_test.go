// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package wal

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// histogramsRecorder is a metrics provider that records the values observed by its histograms, by name.
type histogramsRecorder struct {
	disabled.Provider
	values map[string][]float64
}

func (p *histogramsRecorder) NewHistogram(opts metrics.HistogramOpts) metrics.Histogram {
	return &recordedHistogram{recorder: p, name: opts.Name}
}

type recordedHistogram struct {
	recorder *histogramsRecorder
	name     string
}

func (h *recordedHistogram) With(...string) metrics.Histogram { return h }
func (h *recordedHistogram) Observe(value float64) {
	if h.recorder.values == nil {
		h.recorder.values = make(map[string][]float64)
	}
	h.recorder.values[h.name] = append(h.recorder.values[h.name], value)
}

// TestWriteAheadLogFile_AppendObservesLatency makes sure every Append observes its latency once,
// and that a WAL created without metrics still appends.
func TestWriteAheadLogFile_AppendObservesLatency(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	logger := basicLog.Sugar()

	recorder := &histogramsRecorder{}
	wal, err := Create(logger, t.TempDir(), &Options{Metrics: NewMetrics(recorder)})
	require.NoError(t, err)
	defer wal.Close()

	// Initialize observes a zero so that the histogram exists before the first append.
	require.Equal(t, []float64{0}, recorder.values["wal_latency_append"])

	require.NoError(t, wal.Append([]byte{1, 2, 3}, false))
	require.NoError(t, wal.Append([]byte{4, 5, 6}, true))

	latencies := recorder.values["wal_latency_append"][1:]
	require.Len(t, latencies, 2)
	for _, latency := range latencies {
		require.GreaterOrEqual(t, latency, 0.0)
		require.Less(t, latency, time.Minute.Seconds())
	}

	// Empty data is rejected before any record is appended, so nothing is observed.
	require.Error(t, wal.Append(nil, false))
	require.Len(t, recorder.values["wal_latency_append"], 3)

	noMetrics, err := Create(logger, t.TempDir(), nil)
	require.NoError(t, err)
	defer noMetrics.Close()
	require.NoError(t, noMetrics.Append([]byte{7}, false))
}
