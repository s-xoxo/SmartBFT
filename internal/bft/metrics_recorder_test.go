// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"strings"
	"sync"

	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
)

// metricsRecorder is a metrics provider that records the last value set on its gauges
// and the sum added to its counters, keyed by metric name and label values.
type metricsRecorder struct {
	disabled.Provider
	lock     sync.Mutex
	gauges   map[string]float64
	counters map[string]float64
}

func (p *metricsRecorder) NewGauge(opts metrics.GaugeOpts) metrics.Gauge {
	return &recordedGauge{recorder: p, name: opts.Name}
}

func (p *metricsRecorder) NewCounter(opts metrics.CounterOpts) metrics.Counter {
	return &recordedCounter{recorder: p, name: opts.Name}
}

// gauge returns the last value set on the unlabelled gauge with the given name.
func (p *metricsRecorder) gauge(name string) float64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.gauges[metricKey(name, nil)]
}

// counter returns the sum added to the counter with the given name and label values.
func (p *metricsRecorder) counter(name string, labelValues ...string) float64 {
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.counters[metricKey(name, labelValues)]
}

func metricKey(name string, labelValues []string) string {
	if len(labelValues) == 0 {
		return name
	}
	return name + "{" + strings.Join(labelValues, ",") + "}"
}

type recordedGauge struct {
	recorder    *metricsRecorder
	name        string
	labelValues []string
}

func (g *recordedGauge) With(labelValues ...string) metrics.Gauge {
	return &recordedGauge{recorder: g.recorder, name: g.name, labelValues: labelValues}
}

func (g *recordedGauge) Add(delta float64) {
	g.recorder.lock.Lock()
	defer g.recorder.lock.Unlock()
	if g.recorder.gauges == nil {
		g.recorder.gauges = make(map[string]float64)
	}
	g.recorder.gauges[metricKey(g.name, g.labelValues)] += delta
}

func (g *recordedGauge) Set(value float64) {
	g.recorder.lock.Lock()
	defer g.recorder.lock.Unlock()
	if g.recorder.gauges == nil {
		g.recorder.gauges = make(map[string]float64)
	}
	g.recorder.gauges[metricKey(g.name, g.labelValues)] = value
}

type recordedCounter struct {
	recorder    *metricsRecorder
	name        string
	labelValues []string
}

func (c *recordedCounter) With(labelValues ...string) metrics.Counter {
	return &recordedCounter{recorder: c.recorder, name: c.name, labelValues: labelValues}
}

func (c *recordedCounter) Add(delta float64) {
	c.recorder.lock.Lock()
	defer c.recorder.lock.Unlock()
	if c.recorder.counters == nil {
		c.recorder.counters = make(map[string]float64)
	}
	c.recorder.counters[metricKey(c.name, c.labelValues)] += delta
}
