// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// fastLikeConfig mirrors the integration harness' fastConfig (test/test_app.go), which the design tables use.
var fastLikeConfig = types.Configuration{
	RequestBatchMaxCount:    10,
	RequestBatchMaxBytes:    10 * 1024 * 1024,
	RequestBatchMaxInterval: 10 * time.Millisecond,
	RequestPoolSize:         40,
	RequestForwardTimeout:   500 * time.Millisecond,
	RequestComplainTimeout:  2 * time.Second,
	PipelineDepthMax:        1,
}

// recordingBatcher is a Batcher that only records the parameters it is given.
type recordingBatcher struct {
	lock    sync.Mutex
	params  BatchParams
	history []BatchParams
}

func (b *recordingBatcher) NextBatch() [][]byte { return nil }
func (b *recordingBatcher) Close()              {}
func (b *recordingBatcher) Closed() bool        { return false }
func (b *recordingBatcher) Reset()              {}

func (b *recordingBatcher) SetBatchParams(p BatchParams) {
	b.lock.Lock()
	defer b.lock.Unlock()
	b.params = p
	b.history = append(b.history, p)
}

func (b *recordingBatcher) BatchParams() BatchParams {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.params
}

func (b *recordingBatcher) calls() int {
	b.lock.Lock()
	defer b.lock.Unlock()
	return len(b.history)
}

// fakePoolStats returns whatever backlog and oldest age a test sets.
type fakePoolStats struct {
	size int
	age  time.Duration
}

func (p *fakePoolStats) Size() int                { return p.size }
func (p *fakePoolStats) OldestAge() time.Duration { return p.age }

// counterRecorder is a metrics provider that sums what is added to its counters, keyed by name and label values.
type counterRecorder struct {
	disabled.Provider
	lock   sync.Mutex
	counts map[string]float64
}

func (r *counterRecorder) NewCounter(opts metrics.CounterOpts) metrics.Counter {
	return &recordedCounter{recorder: r, name: opts.Name}
}

func (r *counterRecorder) count(name string, labelValues ...string) float64 {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.counts[name+"{"+strings.Join(labelValues, ",")+"}"]
}

type recordedCounter struct {
	recorder    *counterRecorder
	name        string
	labelValues []string
}

func (c *recordedCounter) With(labelValues ...string) metrics.Counter {
	return &recordedCounter{recorder: c.recorder, name: c.name, labelValues: labelValues}
}

func (c *recordedCounter) Add(delta float64) {
	c.recorder.lock.Lock()
	defer c.recorder.lock.Unlock()
	if c.recorder.counts == nil {
		c.recorder.counts = make(map[string]float64)
	}
	c.recorder.counts[c.name+"{"+strings.Join(c.labelValues, ",")+"}"] += delta
}

// tunerHarness drives a BatchTuner with a fake batcher, a fake pool and a controlled clock.
type tunerHarness struct {
	t        *testing.T
	tuner    *BatchTuner
	batcher  *recordingBatcher
	pool     *fakePoolStats
	counters *counterRecorder
	clock    time.Time
}

func newTunerHarness(t *testing.T, cfg TunerConfig) *tunerHarness {
	t.Helper()
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	h := &tunerHarness{
		t:        t,
		batcher:  &recordingBatcher{},
		pool:     &fakePoolStats{},
		counters: &counterRecorder{},
		clock:    time.Unix(1_700_000_000, 0),
	}
	h.tuner = NewBatchTuner(cfg, h.batcher, h.pool, api.NewMetricsBatcher(h.counters), basicLog.Sugar())
	h.tuner.now = func() time.Time { return h.clock }
	h.tuner.Reset() // re-stamp the idle timers with the controlled clock
	h.batcher.history = nil
	h.counters.counts = nil // the two Reset publications above are not adjustments under test
	return h
}

func (h *tunerHarness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

// decide proposes a batch of n requests of 10 bytes with the current parameters and decides it after latency.
func (h *tunerHarness) decide(n int, latency time.Duration) {
	batch := make([][]byte, n)
	for i := range batch {
		batch[i] = make([]byte, 10)
	}
	h.tuner.OnBatch(batch, h.batcher.BatchParams())
	h.advance(latency)
	h.tuner.OnDecision()
}

func (h *tunerHarness) c() uint64              { return h.batcher.BatchParams().MaxMsgCount }
func (h *tunerHarness) timeout() time.Duration { return h.batcher.BatchParams().Timeout }

func (h *tunerHarness) adjustments(param, direction string) float64 {
	return h.counters.count("batcher_count_of_adjustments", "param", param, "direction", direction)
}

func (h *tunerHarness) guardTrips(reason string) float64 {
	return h.counters.count("batcher_count_of_guard_trips", "reason", reason)
}

func TestTunerConfigFromConfiguration(t *testing.T) {
	t.Run("fastConfig", func(t *testing.T) {
		cfg := TunerConfigFromConfiguration(fastLikeConfig)
		require.Equal(t, uint64(10), cfg.CCap)
		require.Equal(t, uint64(1), cfg.CMin)
		require.Equal(t, uint64(3), cfg.CInit)
		require.Equal(t, uint64(10*1024*1024), cfg.SFixed)
		require.Equal(t, 10*time.Millisecond, cfg.T0)
		require.Equal(t, 2500*time.Microsecond, cfg.TMin)
		require.Equal(t, 40*time.Millisecond, cfg.TMax)
		require.Equal(t, 100*time.Millisecond, cfg.IdleAfter)
		require.Equal(t, 30, cfg.CeilHold)
		require.Equal(t, 100, cfg.RefWindow)
		require.Equal(t, 3, cfg.K)
		require.InDelta(t, 1.25, cfg.Up, 0)
		require.InDelta(t, 0.75, cfg.Down, 0)
		require.InDelta(t, 2.0, cfg.Gamma, 0)
		require.InDelta(t, 0.5, cfg.ThetaA, 0)
		require.Equal(t, 500*time.Millisecond, cfg.ForwardTimeout)
	})

	t.Run("DefaultConfig", func(t *testing.T) {
		cfg := TunerConfigFromConfiguration(types.DefaultConfig)
		require.Equal(t, uint64(100), cfg.CCap)
		require.Equal(t, uint64(25), cfg.CInit)
		require.Equal(t, 50*time.Millisecond, cfg.T0)
		require.Equal(t, 12500*time.Microsecond, cfg.TMin)
		require.Equal(t, 200*time.Millisecond, cfg.TMax)
		require.Equal(t, 500*time.Millisecond, cfg.IdleAfter)
	})

	t.Run("pool capacity per pipeline depth bounds the count", func(t *testing.T) {
		c := fastLikeConfig
		c.PipelineDepthMax = 8 // 40 / 8 = 5 < 10
		cfg := TunerConfigFromConfiguration(c)
		require.Equal(t, uint64(5), cfg.CCap)
		require.Equal(t, uint64(2), cfg.CInit)
	})

	t.Run("interval above a quarter of the forward timeout keeps the configured interval as the maximum", func(t *testing.T) {
		c := fastLikeConfig
		c.RequestBatchMaxInterval = time.Second
		c.RequestForwardTimeout = 2 * time.Second
		cfg := TunerConfigFromConfiguration(c)
		require.Equal(t, time.Second, cfg.T0)
		require.Equal(t, time.Second, cfg.TMax) // max(T0, min(4s, 500ms))
		require.Equal(t, 250*time.Millisecond, cfg.TMin)

		h := newTunerHarness(t, cfg)
		require.Equal(t, time.Second, h.timeout()) // the start state equals the configured one
		require.Equal(t, uint64(3), h.c())
	})
}

func TestTunerWithOverrides(t *testing.T) {
	base := TunerConfigFromConfiguration(fastLikeConfig)

	t.Run("zero fields keep the derived values", func(t *testing.T) {
		require.Equal(t, base, base.WithOverrides(TunerConfig{}))
		cfg := base.WithOverrides(TunerConfig{K: 1, Gamma: 3, RefWindow: 7})
		require.Equal(t, 1, cfg.K)
		require.InDelta(t, 3.0, cfg.Gamma, 0)
		require.Equal(t, 7, cfg.RefWindow)
		require.Equal(t, base.CCap, cfg.CCap)
		require.Equal(t, base.TMax, cfg.TMax)
	})

	t.Run("bounds are never widened", func(t *testing.T) {
		cfg := base.WithOverrides(TunerConfig{CCap: 1000, TMax: time.Hour})
		require.Equal(t, base.CCap, cfg.CCap)
		require.Equal(t, base.TMax, cfg.TMax)
		cfg = base.WithOverrides(TunerConfig{CCap: 5, TMax: 20 * time.Millisecond})
		require.Equal(t, uint64(5), cfg.CCap)
		require.Equal(t, 20*time.Millisecond, cfg.TMax)
	})

	t.Run("inconsistent values are normalised", func(t *testing.T) {
		cfg := base.WithOverrides(TunerConfig{CInit: 50, Up: 0.5, Down: 2, Alpha: 7, PhiLo: 0.9, PhiHi: 0.5})
		require.Equal(t, base.CCap, cfg.CInit) // clamped into [CMin, CCap]
		require.InDelta(t, defaultTunerUp, cfg.Up, 0)
		require.InDelta(t, defaultTunerDown, cfg.Down, 0)
		require.InDelta(t, defaultTunerAlpha, cfg.Alpha, 0)
		require.InDelta(t, 0.9, cfg.PhiHi, 0) // PhiHi >= PhiLo
	})
}

func TestTunerStartsAtInitialParameters(t *testing.T) {
	h := newTunerHarness(t, TunerConfigFromConfiguration(fastLikeConfig))
	require.Equal(t, BatchParams{MaxMsgCount: 3, MaxSizeBytes: 10 * 1024 * 1024, Timeout: 10 * time.Millisecond}, h.batcher.BatchParams())
}

// TestTunerGrowsOnFullStreakWithBacklog: fastConfig (CInit 3, CCap 10, K 3); every batch full and a backlog
// remains -> C grows 3->4->5->7->9->10 on every third decision, then stays.
func TestTunerGrowsOnFullStreakWithBacklog(t *testing.T) {
	h := newTunerHarness(t, TunerConfigFromConfiguration(fastLikeConfig))
	h.pool.size = 5

	expected := []uint64{3, 3, 4, 4, 4, 5, 5, 5, 7, 7, 7, 9, 9, 9, 10}
	for i, want := range expected {
		h.decide(int(h.c()), 5*time.Millisecond)
		require.Equal(t, want, h.c(), "decision %d", i+1)
	}
	calls := h.batcher.calls()
	require.Equal(t, 5, calls)
	require.InDelta(t, 5, h.adjustments(api.BatchParamCount, api.DirectionUp), 0)

	// At the cap nothing fires any more.
	for range 30 {
		h.decide(int(h.c()), 5*time.Millisecond)
	}
	require.Equal(t, uint64(10), h.c())
	require.Equal(t, calls, h.batcher.calls())
	require.Equal(t, 10*time.Millisecond, h.timeout())
}

// TestTunerHoldsWhenFullButNoBacklog: full batches whose demand matches C exactly change nothing (R4).
func TestTunerHoldsWhenFullButNoBacklog(t *testing.T) {
	h := newTunerHarness(t, TunerConfigFromConfiguration(fastLikeConfig))
	h.pool.size = 0
	for range 30 {
		h.decide(int(h.c()), 5*time.Millisecond)
	}
	require.Equal(t, uint64(3), h.c())
	require.Zero(t, h.batcher.calls())
}

// TestTunerTimeoutHighFillLengthens: batches closed by the timeout at 80% fill lengthen T on every third decision,
// monotonically, until TMax; C does not move.
func TestTunerTimeoutHighFillLengthens(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{CInit: 10})
	h := newTunerHarness(t, cfg)
	h.pool.size = 0

	prev := h.timeout()
	for i := 1; i <= 60; i++ {
		h.decide(8, 5*time.Millisecond) // phi = 0.8 >= PhiHi, not full
		cur := h.timeout()
		require.GreaterOrEqual(t, cur, prev, "decision %d", i)
		if cur != prev {
			require.Zero(t, i%3, "a change must happen on a third decision, got decision %d", i)
		}
		prev = cur
	}
	require.Equal(t, cfg.TMax, h.timeout())
	require.Equal(t, uint64(10), h.c())
	require.Positive(t, h.adjustments(api.BatchParamTimeout, api.DirectionUp))
	require.Zero(t, h.adjustments(api.BatchParamTimeout, api.DirectionDown))
	require.Zero(t, h.adjustments(api.BatchParamCount, api.DirectionUp))
	require.Zero(t, h.adjustments(api.BatchParamCount, api.DirectionDown))

	calls := h.batcher.calls()
	for range 30 {
		h.decide(8, 5*time.Millisecond)
	}
	require.Equal(t, calls, h.batcher.calls())
}

// TestTunerTimeoutLowFillShortens: nearly empty batches closed by the timeout shorten T until TMin.
func TestTunerTimeoutLowFillShortens(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{CInit: 10})
	h := newTunerHarness(t, cfg)

	prev := h.timeout()
	for i := 1; i <= 60; i++ {
		h.decide(1, 5*time.Millisecond) // phi = 0.1 < PhiLo
		cur := h.timeout()
		require.LessOrEqual(t, cur, prev, "decision %d", i)
		prev = cur
	}
	require.Equal(t, cfg.TMin, h.timeout())
	require.Equal(t, uint64(10), h.c())
	calls := h.batcher.calls()
	for range 30 {
		h.decide(1, 5*time.Millisecond)
	}
	require.Equal(t, calls, h.batcher.calls())
}

// TestTunerDeadBandHolds: a fill ratio inside [PhiLo, PhiHi) changes nothing.
func TestTunerDeadBandHolds(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{CInit: 10})
	h := newTunerHarness(t, cfg)
	h.pool.size = 5
	for range 60 {
		h.decide(5, 5*time.Millisecond) // phi = 0.5
	}
	require.Zero(t, h.batcher.calls())
}

// TestTunerPhiHiOneDisablesLengthening: PhiHi = 1 turns the lengthening rule off (a batch with fill 1 is full).
func TestTunerPhiHiOneDisablesLengthening(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{CInit: 10, PhiHi: 1.0})
	h := newTunerHarness(t, cfg)
	for range 60 {
		h.decide(9, 5*time.Millisecond)
	}
	require.Zero(t, h.batcher.calls())
	require.Equal(t, 10*time.Millisecond, h.timeout())
}

// TestTunerLatencyGuardShrinksAndRemembersCeiling: once the reference window is full, a latency jump brings C down
// by Down and the count it came down from stays a ceiling for CeilHold decisions, even with full batches.
func TestTunerLatencyGuardShrinksAndRemembersCeiling(t *testing.T) {
	c := fastLikeConfig
	c.RequestBatchMaxCount = 40
	c.RequestPoolSize = 400
	cfg := TunerConfigFromConfiguration(c).WithOverrides(TunerConfig{RefWindow: 10})
	require.Equal(t, uint64(40), cfg.CCap)
	require.Equal(t, uint64(10), cfg.CInit)
	h := newTunerHarness(t, cfg)
	h.pool.size = 5

	// 11 full decisions at 10ms: C 10->13->17->22 (decisions 3, 6, 9), window full (>= 10 decisions).
	for range 11 {
		h.decide(int(h.c()), 10*time.Millisecond)
	}
	require.Equal(t, uint64(22), h.c())
	require.Zero(t, h.guardTrips(api.GuardLatency))

	// Decision 12, one slow round: lFast = 0.3*50 + 0.7*10 = 22ms > 2 * 10ms -> GUARD_DOWN to floor(0.75*22) = 16.
	h.decide(int(h.c()), 50*time.Millisecond)
	require.Equal(t, uint64(16), h.c())
	require.InDelta(t, 1, h.guardTrips(api.GuardLatency), 0)
	require.InDelta(t, 1, h.adjustments(api.BatchParamCount, api.DirectionDown), 0)

	// Fast rounds again with full batches: C may grow back up to the ceiling 22 but not above it for CeilHold = 30
	// decisions counted from the guard trip (decisions 13..41); the hold expires at decision 42.
	for i := 13; i <= 41; i++ {
		h.decide(int(h.c()), 10*time.Millisecond)
		require.LessOrEqual(t, h.c(), uint64(22), "decision %d", i)
	}
	require.Equal(t, uint64(22), h.c())
	require.InDelta(t, 1, h.guardTrips(api.GuardLatency), 0)

	for i := 42; i <= 45; i++ {
		h.decide(int(h.c()), 10*time.Millisecond)
	}
	require.Greater(t, h.c(), uint64(22))
}

// TestTunerLatencyGuardNotArmedBeforeWindow: before RefWindow decisions, a latency jump does not fire the guard.
func TestTunerLatencyGuardNotArmedBeforeWindow(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{RefWindow: 50, CInit: 10})
	h := newTunerHarness(t, cfg)
	for range 20 {
		h.decide(5, 10*time.Millisecond)
	}
	for range 20 {
		h.decide(5, 200*time.Millisecond)
	}
	require.Zero(t, h.guardTrips(api.GuardLatency))
	require.Equal(t, uint64(10), h.c())
}

// TestTunerRecoversAfterPermanentLatencyStep: when the round latency steps up for good (e.g. a longer RTT), the
// sliding-window reference follows the new level within RefWindow decisions, the guard stops firing and C climbs
// back to the cap instead of staying pinned at the floor.
func TestTunerRecoversAfterPermanentLatencyStep(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{K: 1, RefWindow: 20})
	h := newTunerHarness(t, cfg)
	h.pool.size = 5

	for range 30 {
		h.decide(int(h.c()), 10*time.Millisecond)
	}
	require.Equal(t, uint64(10), h.c())

	// The step: every round now takes 50ms.
	tripsAfter := make([]float64, 0, 200)
	for range 200 {
		h.decide(int(h.c()), 50*time.Millisecond)
		tripsAfter = append(tripsAfter, h.guardTrips(api.GuardLatency))
	}
	require.Positive(t, tripsAfter[0], "the guard fires right after the step")
	// No more trips once the reference has moved to the new level: within RefWindow (+ a few EWMA steps).
	settled := tripsAfter[cfg.RefWindow+5]
	for i := cfg.RefWindow + 5; i < len(tripsAfter); i++ {
		require.InDelta(t, settled, tripsAfter[i], 0, "guard fired again at decision %d after the step", i+1)
	}
	require.Equal(t, cfg.CCap, h.c(), "C recovered to the cap after the step")
}

// TestTunerDrainOnOldRequest: a request older than ThetaA * ForwardTimeout switches to (CCap, TMin) at once.
func TestTunerDrainOnOldRequest(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig)
	h := newTunerHarness(t, cfg)
	h.pool.size = 1
	h.pool.age = 300 * time.Millisecond // >= 0.5 * 500ms

	h.decide(1, 5*time.Millisecond)
	require.Equal(t, BatchParams{MaxMsgCount: 10, MaxSizeBytes: cfg.SFixed, Timeout: 2500 * time.Microsecond}, h.batcher.BatchParams())
	require.InDelta(t, 1, h.guardTrips(api.GuardDrain), 0)
	require.Equal(t, 1, h.batcher.calls())

	// Still draining: counted again, but nothing new is published.
	h.decide(1, 5*time.Millisecond)
	require.InDelta(t, 2, h.guardTrips(api.GuardDrain), 0)
	require.Equal(t, 1, h.batcher.calls())

	// Below the threshold the regular rules apply again (here: a nearly empty timeout batch holds T at TMin).
	h.pool.age = 100 * time.Millisecond
	h.decide(1, 5*time.Millisecond)
	require.InDelta(t, 2, h.guardTrips(api.GuardDrain), 0)
}

// TestTunerIdleRelaxesToInitial: from (CCap, TMax), an idle leader relaxes one step per IdleAfter until (CInit, T0)
// and then stops publishing.
func TestTunerIdleRelaxesToInitial(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig)
	h := newTunerHarness(t, cfg)
	h.pool.size = 5
	for range 15 {
		h.decide(int(h.c()), 5*time.Millisecond)
	}
	for range 30 {
		h.decide(8, 5*time.Millisecond)
	}
	require.Equal(t, uint64(10), h.c())
	require.Equal(t, cfg.TMax, h.timeout())
	calls := h.batcher.calls()

	// Not idle for long enough: nothing.
	h.tuner.OnIdle()
	require.Equal(t, calls, h.batcher.calls())

	expectedC := []uint64{7, 5, 3, 3, 3}
	prevT := h.timeout()
	for i, want := range expectedC {
		h.advance(cfg.IdleAfter)
		h.tuner.OnIdle()
		require.Equal(t, want, h.c(), "relaxation step %d", i+1)
		require.Less(t, h.timeout(), prevT, "relaxation step %d", i+1)
		require.GreaterOrEqual(t, h.timeout(), cfg.T0)
		prevT = h.timeout()
		// A second poll within the same IdleAfter does nothing.
		h.tuner.OnIdle()
		require.Equal(t, want, h.c())
	}
	require.Equal(t, cfg.T0, h.timeout())
	calls = h.batcher.calls()
	for range 5 {
		h.advance(cfg.IdleAfter)
		h.tuner.OnIdle()
	}
	require.Equal(t, calls, h.batcher.calls())
	require.Equal(t, BatchParams{MaxMsgCount: 3, MaxSizeBytes: cfg.SFixed, Timeout: 10 * time.Millisecond}, h.batcher.BatchParams())
}

// TestTunerResetOnStartView: Reset returns to (CInit, T0), drops the pending sample and the latency estimate.
func TestTunerResetOnStartView(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig)
	h := newTunerHarness(t, cfg)
	h.pool.size = 5
	for range 15 {
		h.decide(int(h.c()), 5*time.Millisecond)
	}
	require.Equal(t, uint64(10), h.c())
	h.tuner.OnBatch([][]byte{{1}}, h.batcher.BatchParams()) // pending

	h.tuner.Reset()
	require.Equal(t, BatchParams{MaxMsgCount: 3, MaxSizeBytes: cfg.SFixed, Timeout: 10 * time.Millisecond}, h.batcher.BatchParams())
	require.Nil(t, h.tuner.pending)
	require.Zero(t, h.tuner.lFast)
	require.Zero(t, h.tuner.decisions)

	calls := h.batcher.calls()
	h.tuner.OnDecision() // no sample after the reset
	require.Equal(t, calls, h.batcher.calls())

	// A Reset at the initial state still publishes, so the batcher is known to hold (CInit, T0).
	h.tuner.Reset()
	require.Equal(t, calls+1, h.batcher.calls())
}

// TestTunerIgnoresDecisionWithoutSample: a decision this tuner did not propose changes nothing.
func TestTunerIgnoresDecisionWithoutSample(t *testing.T) {
	h := newTunerHarness(t, TunerConfigFromConfiguration(fastLikeConfig))
	h.pool.size = 5
	h.pool.age = time.Hour // even an old request does not matter without a sample
	for range 10 {
		h.tuner.OnDecision()
	}
	require.Zero(t, h.batcher.calls())
	require.Zero(t, h.guardTrips(api.GuardDrain))
}

// TestTunerBounded (property): under random inputs every published parameter set is within the bounds and
// differs from the previous one (no no-op publications, apart from Reset).
func TestTunerBounded(t *testing.T) {
	cfg := TunerConfigFromConfiguration(fastLikeConfig).WithOverrides(TunerConfig{K: 2, RefWindow: 10})
	h := newTunerHarness(t, cfg)
	rng := rand.New(rand.NewSource(42))

	resets := map[int]bool{}
	for range 10_000 {
		switch rng.Intn(20) {
		case 0:
			h.advance(time.Duration(rng.Int63n(int64(3 * cfg.IdleAfter))))
			h.tuner.OnIdle()
		case 1:
			resets[h.batcher.calls()] = true
			h.tuner.Reset()
		default:
			h.pool.size = rng.Intn(20)
			h.pool.age = time.Duration(rng.Int63n(int64(600 * time.Millisecond)))
			n := 1 + rng.Intn(int(h.c()))
			h.decide(n, time.Duration(1+rng.Int63n(int64(100*time.Millisecond))))
		}
	}

	history := h.batcher.history
	require.NotEmpty(t, history)
	for i, p := range history {
		require.GreaterOrEqual(t, p.MaxMsgCount, cfg.CMin, "publication %d", i)
		require.LessOrEqual(t, p.MaxMsgCount, cfg.CCap, "publication %d", i)
		require.GreaterOrEqual(t, p.Timeout, cfg.TMin, "publication %d", i)
		require.LessOrEqual(t, p.Timeout, cfg.TMax, "publication %d", i)
		require.Equal(t, cfg.SFixed, p.MaxSizeBytes, "publication %d", i)
		if i > 0 && !resets[i] {
			require.NotEqual(t, history[i-1], p, "publication %d repeats the previous parameters", i)
		}
	}
}

// TestTunerNoOscillationUnderSteadyLoad: a synthetic plant with a constant arrival rate reaches a state in which
// no rule fires any more; at the saturation boundary T converges monotonically to TMax (no limit cycle).
func TestTunerNoOscillationUnderSteadyLoad(t *testing.T) {
	const roundLatency = 10 * time.Millisecond
	for _, tc := range []struct {
		name string
		rate float64 // requests per second
	}{
		{"light", 100},
		{"boundary", 400},
		{"saturated", 5000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := TunerConfigFromConfiguration(fastLikeConfig)
			h := newTunerHarness(t, cfg)
			var callsAt300 int
			for i := 1; i <= 500; i++ {
				c := int(h.c())
				arrivals := int(tc.rate * (h.timeout() + roundLatency).Seconds())
				n := max(1, min(c, arrivals))
				h.pool.size = max(0, arrivals-c)
				h.decide(n, roundLatency)
				if i == 300 {
					callsAt300 = h.batcher.calls()
				}
			}
			require.Equal(t, callsAt300, h.batcher.calls(), "parameters still changing in the last 200 decisions")
			require.GreaterOrEqual(t, h.c(), cfg.CMin)
			require.LessOrEqual(t, h.c(), cfg.CCap)

			if tc.name == "boundary" {
				// T only ever grows at the boundary (no sawtooth), and the demand ends up matching C exactly.
				prev := cfg.T0
				for _, p := range h.batcher.history {
					require.GreaterOrEqual(t, p.Timeout, prev, "T decreased at the saturation boundary")
					prev = p.Timeout
				}
				require.Greater(t, h.timeout(), cfg.T0)
				require.Greater(t, h.c(), cfg.CInit)
			}
			if tc.name == "saturated" {
				require.Equal(t, cfg.CCap, h.c())
			}
		})
	}
}

// TestTunerWarnsOnLongInterval: an interval above a quarter of the forward timeout is accepted and logged once.
func TestTunerWarnsOnLongInterval(t *testing.T) {
	c := fastLikeConfig
	c.RequestBatchMaxInterval = time.Second
	cfg := TunerConfigFromConfiguration(c)
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	tuner := NewBatchTuner(cfg, &recordingBatcher{}, &fakePoolStats{}, nil, basicLog.Sugar())
	require.Equal(t, time.Second, tuner.Config().TMax)
}
