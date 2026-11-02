// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"math"
	"sync"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
)

// Reasons the tuner logs with every parameter change ("Adaptive batch: max count %d, timeout %s (%s)").
const (
	tunerReasonReset     = "RESET"             // Reset: back to (CInit, T0)
	tunerReasonRelax     = "RELAX"             // idle relaxation towards (CInit, T0)
	tunerReasonDrain     = "DRAIN"             // request-age guard: maximal throughput (CCap, TMin)
	tunerReasonGuardDown = "GUARD_DOWN"        // round-latency guard: count reduced
	tunerReasonFull      = "FULL"              // full batches with a backlog: count increased
	tunerReasonHighFill  = "TIMEOUT_HIGH_FILL" // almost full batches closed by the timeout: timeout lengthened
	tunerReasonLowFill   = "TIMEOUT_LOW_FILL"  // nearly empty batches closed by the timeout: timeout shortened
	minimalBatchTimeout  = time.Millisecond
)

// TunerConfig holds the parameters of the adaptive batch control law. Every parameter has a default derived
// from types.Configuration (TunerConfigFromConfiguration); tests and benchmarks may override any of them
// (WithOverrides) without exposing them in types.Configuration.
type TunerConfig struct {
	// CMin, CInit and CCap are the lower bound, the start (and idle) value and the upper bound of the maximal
	// number of requests in a batch. CCap defaults to min(RequestBatchMaxCount, RequestPoolSize/depth) and
	// is never raised above that by an override.
	CMin, CInit, CCap uint64
	// SFixed is the maximal batch size in bytes. It is never adjusted (= RequestBatchMaxBytes).
	SFixed uint64
	// T0, TMin and TMax are the start (and idle) value, the lower bound and the upper bound of the batch timeout.
	// TMax defaults to max(T0, min(4*T0, RequestForwardTimeout/4)) and is never raised above that by an override.
	T0, TMin, TMax time.Duration
	// Up and Down are the multiplicative step sizes (u > 1, 0 < d < 1).
	Up, Down float64
	// K is the number of consecutive decisions with the same evidence a rule needs before it changes a knob.
	K int
	// PhiLo and PhiHi bound the dead band of the fill ratio (requests/count) of a batch closed by the timeout:
	// a ratio of PhiHi or more lengthens the timeout, a ratio below PhiLo shortens it. PhiHi = 1 disables lengthening.
	PhiLo, PhiHi float64
	// Alpha is the EWMA coefficient of the fast round-latency estimate.
	Alpha float64
	// Gamma is the round-latency guard multiplier: the guard fires when the fast estimate exceeds Gamma times
	// the minimum of the fast estimate over the last RefWindow decisions.
	Gamma float64
	// RefWindow is the number of decisions the round-latency reference (sliding-window minimum) looks back.
	// The guard is not armed before RefWindow decisions have been observed since the last Reset.
	RefWindow int
	// ThetaA is the request-age guard threshold as a fraction of ForwardTimeout.
	ThetaA float64
	// ForwardTimeout is RequestForwardTimeout.
	ForwardTimeout time.Duration
	// IdleAfter is how long the leader must have proposed nothing before the parameters relax one step
	// towards (CInit, T0), and the minimal interval between two relaxation steps.
	IdleAfter time.Duration
	// CeilHold is the number of decisions the count stays below the value the latency guard brought it down from.
	CeilHold int
}

// Default control-law parameters that do not depend on the configuration.
const (
	defaultTunerUp        = 1.25
	defaultTunerDown      = 0.75
	defaultTunerK         = 3
	defaultTunerPhiLo     = 0.25
	defaultTunerPhiHi     = 0.75
	defaultTunerAlpha     = 0.3
	defaultTunerGamma     = 2.0
	defaultTunerRefWindow = 100
	defaultTunerThetaA    = 0.5
)

// TunerConfigFromConfiguration derives the default control-law parameters from the configuration:
//
//	CCap   = min(RequestBatchMaxCount, RequestPoolSize / EffectivePipelineDepth())   (at least 1)
//	CMin   = 1
//	CInit  = max(CMin, ceil(CCap / 4))
//	T0     = RequestBatchMaxInterval
//	TMin   = max(T0 / 4, 1ms), but never above T0
//	TMax   = max(T0, min(4 * T0, RequestForwardTimeout / 4))
//	u, d, k, phi, alpha, gamma, theta, RefWindow = 1.25, 0.75, 3, [0.25, 0.75), 0.3, 2.0, 0.5, 100
//	IdleAfter = 10 * T0; CeilHold = 10 * k
func TunerConfigFromConfiguration(c types.Configuration) TunerConfig {
	cCap := c.RequestBatchMaxCount
	if perDepth := c.RequestPoolSize / c.EffectivePipelineDepth(); perDepth < cCap {
		cCap = perDepth
	}
	if cCap == 0 {
		cCap = 1
	}
	t0 := c.RequestBatchMaxInterval
	tMin := max(t0/4, minimalBatchTimeout)
	tMin = min(tMin, t0)
	tMax := max(t0, min(4*t0, c.RequestForwardTimeout/4))
	return TunerConfig{
		CMin:           1,
		CInit:          max(1, (cCap+3)/4),
		CCap:           cCap,
		SFixed:         c.RequestBatchMaxBytes,
		T0:             t0,
		TMin:           tMin,
		TMax:           tMax,
		Up:             defaultTunerUp,
		Down:           defaultTunerDown,
		K:              defaultTunerK,
		PhiLo:          defaultTunerPhiLo,
		PhiHi:          defaultTunerPhiHi,
		Alpha:          defaultTunerAlpha,
		Gamma:          defaultTunerGamma,
		RefWindow:      defaultTunerRefWindow,
		ThetaA:         defaultTunerThetaA,
		ForwardTimeout: c.RequestForwardTimeout,
		IdleAfter:      10 * t0,
		CeilHold:       10 * defaultTunerK,
	}
}

// WithOverrides returns c with every non-zero field of o in place of c's. The bounds CCap and TMax are never
// raised above c's (they protect the pool capacity and the request timeout budget), and the result is normalised
// so that CMin <= CInit <= CCap, TMin <= TMax and every rate or count is usable.
func (c TunerConfig) WithOverrides(o TunerConfig) TunerConfig {
	r := c
	if o.CMin != 0 {
		r.CMin = o.CMin
	}
	if o.CInit != 0 {
		r.CInit = o.CInit
	}
	if o.CCap != 0 {
		r.CCap = min(o.CCap, c.CCap)
	}
	if o.SFixed != 0 {
		r.SFixed = o.SFixed
	}
	if o.T0 != 0 {
		r.T0 = o.T0
	}
	if o.TMin != 0 {
		r.TMin = o.TMin
	}
	if o.TMax != 0 {
		r.TMax = min(o.TMax, c.TMax)
	}
	if o.Up != 0 {
		r.Up = o.Up
	}
	if o.Down != 0 {
		r.Down = o.Down
	}
	if o.K != 0 {
		r.K = o.K
	}
	if o.PhiLo != 0 {
		r.PhiLo = o.PhiLo
	}
	if o.PhiHi != 0 {
		r.PhiHi = o.PhiHi
	}
	if o.Alpha != 0 {
		r.Alpha = o.Alpha
	}
	if o.Gamma != 0 {
		r.Gamma = o.Gamma
	}
	if o.RefWindow != 0 {
		r.RefWindow = o.RefWindow
	}
	if o.ThetaA != 0 {
		r.ThetaA = o.ThetaA
	}
	if o.ForwardTimeout != 0 {
		r.ForwardTimeout = o.ForwardTimeout
	}
	if o.IdleAfter != 0 {
		r.IdleAfter = o.IdleAfter
	}
	if o.CeilHold != 0 {
		r.CeilHold = o.CeilHold
	}
	return r.normalised()
}

// normalised repairs an inconsistent parameter set so that the control law is always well defined.
func (c TunerConfig) normalised() TunerConfig {
	c.CMin = max(c.CMin, 1)
	c.CCap = max(c.CCap, c.CMin)
	c.CInit = min(max(c.CInit, c.CMin), c.CCap)
	c.SFixed = max(c.SFixed, 1)
	c.TMin = max(c.TMin, time.Nanosecond)
	c.TMax = max(c.TMax, c.TMin)
	c.T0 = min(max(c.T0, c.TMin), c.TMax)
	if c.Up <= 1 {
		c.Up = defaultTunerUp
	}
	if c.Down <= 0 || c.Down >= 1 {
		c.Down = defaultTunerDown
	}
	c.K = max(c.K, 1)
	if c.PhiLo < 0 {
		c.PhiLo = 0
	}
	c.PhiHi = max(c.PhiHi, c.PhiLo)
	if c.Alpha <= 0 || c.Alpha > 1 {
		c.Alpha = defaultTunerAlpha
	}
	if c.Gamma < 1 {
		c.Gamma = defaultTunerGamma
	}
	c.RefWindow = max(c.RefWindow, 1)
	if c.ThetaA < 0 {
		c.ThetaA = 0
	}
	c.CeilHold = max(c.CeilHold, 0)
	return c
}

// PoolStats are the request pool signals the tuner reads once per decision. *Pool implements it.
type PoolStats interface {
	Size() int
	OldestAge() time.Duration
}

// batchSample is a proposed batch waiting for its decision.
type batchSample struct {
	n          int
	bytes      uint64
	params     BatchParams
	proposedAt time.Time
}

// BatchTuner adjusts the batcher's maximal request count (C) and timeout (T) from what the leader observes:
// whether a batch was closed full or by the timeout and how full it was, the pool backlog, the age of the
// oldest waiting request and the propose-to-decide latency. It has no goroutine: the controller calls OnBatch
// after NextBatch returned a batch, OnIdle after NextBatch returned nothing, OnDecision before the leader token
// is reacquired after a decision, and Reset when a view starts on the leader.
//
// Control law (one knob per signal, hysteresis of K consecutive decisions, multiplicative steps, clamps):
//
//	G1 drain  : oldest request age >= ThetaA * ForwardTimeout      -> (CCap, TMin)
//	G2 latency: fast latency > Gamma * min over RefWindow decisions -> C *= Down, remember the ceiling for CeilHold
//	R1 full   : K full batches in a row and a backlog remains       -> C *= Up (not above the remembered ceiling)
//	R2 high   : K timeout batches with fill >= PhiHi                -> T *= Up
//	R3 low    : K timeout batches with fill <  PhiLo                -> T *= Down
//	R4 band   : anything else                                       -> hold
//	R5 idle   : nothing proposed for IdleAfter                      -> one step back towards (CInit, T0)
type BatchTuner struct {
	mu      sync.Mutex
	cfg     TunerConfig
	batcher Batcher
	pool    PoolStats
	metrics *api.MetricsBatcher
	logger  api.Logger
	now     func() time.Time

	c uint64        // current C (= batcher.BatchParams().MaxMsgCount)
	t time.Duration // current T (= batcher.BatchParams().Timeout)

	lFast     float64   // EWMA of the propose-to-decide latency, seconds; 0 = nothing observed
	lWindow   []float64 // ring buffer of the last RefWindow lFast values
	lWinPos   int
	lWinCount int
	decisions int // decisions observed since the last Reset (arms the latency guard at RefWindow)

	fullStreak, hiStreak, loStreak int
	cCeil                          uint64 // the count the latency guard brought C down from
	cCeilLeft                      int    // decisions the ceiling is still honoured

	lastBatchAt time.Time // when a non-empty batch was last proposed
	lastRelaxAt time.Time
	pending     *batchSample // the proposed batch awaiting its decision (at most one with a depth of 1)
}

// NewBatchTuner creates a tuner that drives the given batcher, starting at (CInit, T0).
// A nil metrics disables the tuner metrics.
func NewBatchTuner(cfg TunerConfig, batcher Batcher, pool PoolStats, metrics *api.MetricsBatcher, logger api.Logger) *BatchTuner {
	if metrics == nil {
		metrics = api.NewMetricsBatcher(&disabled.Provider{})
	}
	cfg = cfg.normalised()
	t := &BatchTuner{
		cfg:     cfg,
		batcher: batcher,
		pool:    pool,
		metrics: metrics,
		logger:  logger,
		now:     time.Now,
		lWindow: make([]float64, cfg.RefWindow),
	}
	if cfg.T0 > cfg.ForwardTimeout/4 {
		logger.Warnf("Adaptive batch: RequestBatchMaxInterval %s exceeds RequestForwardTimeout/4 (%s); the batch timeout will not be lengthened beyond it",
			cfg.T0, cfg.ForwardTimeout/4)
	}
	t.Reset()
	return t
}

// Config returns the parameters the tuner runs with, after normalisation.
func (t *BatchTuner) Config() TunerConfig {
	return t.cfg
}

// OnBatch records a batch the leader is about to propose, with the parameters NextBatch used to build it.
func (t *BatchTuner) OnBatch(batch [][]byte, p BatchParams) {
	t.mu.Lock()
	defer t.mu.Unlock()
	var bytes uint64
	for _, r := range batch {
		bytes += uint64(len(r))
	}
	now := t.now()
	t.pending = &batchSample{n: len(batch), bytes: bytes, params: p, proposedAt: now}
	t.lastBatchAt = now
}

// OnIdle is called whenever NextBatch returned an empty batch (about once per timeout while the leader has
// nothing to propose). Once the leader has been idle for IdleAfter it relaxes the parameters one step towards
// (CInit, T0), at most once per IdleAfter.
func (t *BatchTuner) OnIdle() {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	if now.Sub(t.lastBatchAt) < t.cfg.IdleAfter || now.Sub(t.lastRelaxAt) < t.cfg.IdleAfter {
		return
	}
	t.lastRelaxAt = now
	newC := t.c
	if t.c > t.cfg.CInit {
		newC = max(t.cfg.CInit, mulDown(t.c, t.cfg.Down))
	}
	newT := t.t
	switch {
	case t.t > t.cfg.T0:
		newT = max(t.cfg.T0, mulDownD(t.t, t.cfg.Down))
	case t.t < t.cfg.T0:
		newT = min(t.cfg.T0, mulUpD(t.t, t.cfg.Up))
	}
	t.resetStreaks()
	t.apply(newC, newT, tunerReasonRelax)
}

// OnDecision is called on the leader after a decision was delivered and the delivered requests were removed from
// the pool, right before the leader token is reacquired. It consumes the pending sample and applies the control law.
func (t *BatchTuner) OnDecision() {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.pending
	t.pending = nil
	if s == nil {
		// A decision this tuner did not propose (e.g. an in-flight proposal restored from the WAL): no sample.
		return
	}
	latency := t.now().Sub(s.proposedAt).Seconds()

	// Latency estimators: a fast EWMA and its minimum over the last RefWindow decisions as the reference.
	if t.lFast == 0 {
		t.lFast = latency
	} else {
		t.lFast = t.cfg.Alpha*latency + (1-t.cfg.Alpha)*t.lFast
	}
	t.pushLatency(t.lFast)
	t.decisions++
	if t.cCeilLeft > 0 {
		t.cCeilLeft--
	}

	full := s.n >= int(s.params.MaxMsgCount) || s.bytes >= s.params.MaxSizeBytes
	phi := float64(s.n) / float64(s.params.MaxMsgCount)
	backlog := t.pool.Size()
	age := t.pool.OldestAge()

	// G1: a request has waited in the leader's pool for too long -> maximal throughput, before the forward
	// timeout and the complaints it leads to fire.
	if age >= time.Duration(t.cfg.ThetaA*float64(t.cfg.ForwardTimeout)) {
		t.countGuardTrip(api.GuardDrain)
		t.resetStreaks()
		t.apply(t.cfg.CCap, t.cfg.TMin, tunerReasonDrain)
		return
	}
	// G2: the round latency rose well above its recent minimum -> the batches make the rounds heavy, reduce C.
	// Not armed before the reference window is full (warm-up).
	if lRef := t.referenceLatency(); t.decisions >= t.cfg.RefWindow && lRef > 0 && t.lFast > t.cfg.Gamma*lRef && t.c > t.cfg.CMin {
		t.countGuardTrip(api.GuardLatency)
		t.cCeil, t.cCeilLeft = t.c, t.cfg.CeilHold
		t.resetStreaks()
		t.apply(max(t.cfg.CMin, mulDown(t.c, t.cfg.Down)), t.t, tunerReasonGuardDown)
		return
	}

	newC, newT, why := t.c, t.t, ""
	if full {
		// R1: the batch was full and a backlog remains -> a bigger batch delivers more per round.
		t.hiStreak, t.loStreak = 0, 0
		t.fullStreak++
		if t.fullStreak >= t.cfg.K && backlog > 0 {
			ceiling := t.cfg.CCap
			if t.cCeilLeft > 0 && t.cCeil < ceiling {
				ceiling = t.cCeil
			}
			newC = min(ceiling, mulUp(t.c, t.cfg.Up))
			why = tunerReasonFull
			t.fullStreak = 0
		}
		// full with no backlog: the demand matches C exactly -> hold (R4).
	} else {
		t.fullStreak = 0
		switch {
		case phi >= t.cfg.PhiHi:
			// R2: almost full when the timeout closed it -> waiting a little longer fills it (amortisation).
			t.loStreak = 0
			t.hiStreak++
			if t.hiStreak >= t.cfg.K {
				newT = min(t.cfg.TMax, mulUpD(t.t, t.cfg.Up))
				why = tunerReasonHighFill
				t.hiStreak = 0
			}
		case phi < t.cfg.PhiLo:
			// R3: waiting does not fill the batch -> the wait only adds latency, shorten it.
			t.hiStreak = 0
			t.loStreak++
			if t.loStreak >= t.cfg.K {
				newT = max(t.cfg.TMin, mulDownD(t.t, t.cfg.Down))
				why = tunerReasonLowFill
				t.loStreak = 0
			}
		default:
			// R4: dead band -> hold.
			t.hiStreak, t.loStreak = 0, 0
		}
	}
	t.apply(newC, newT, why)
}

// Reset brings the tuner back to its initial state and the batcher to (CInit, T0). The controller calls it
// when a view starts on the leader; the constructor calls it once.
func (t *BatchTuner) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = nil
	t.lFast = 0
	t.lWinPos, t.lWinCount, t.decisions = 0, 0, 0
	t.resetStreaks()
	t.cCeil, t.cCeilLeft = 0, 0
	now := t.now()
	t.lastBatchAt, t.lastRelaxAt = now, now
	t.apply(t.cfg.CInit, t.cfg.T0, tunerReasonReset)
}

func (t *BatchTuner) resetStreaks() {
	t.fullStreak, t.hiStreak, t.loStreak = 0, 0, 0
}

func (t *BatchTuner) pushLatency(l float64) {
	t.lWindow[t.lWinPos] = l
	t.lWinPos = (t.lWinPos + 1) % len(t.lWindow)
	if t.lWinCount < len(t.lWindow) {
		t.lWinCount++
	}
}

// referenceLatency is the minimum of the fast latency estimate over the last RefWindow decisions, or 0 if none.
func (t *BatchTuner) referenceLatency() float64 {
	if t.lWinCount == 0 {
		return 0
	}
	ref := math.Inf(1)
	for _, l := range t.lWindow[:t.lWinCount] {
		ref = min(ref, l)
	}
	return ref
}

func (t *BatchTuner) countGuardTrip(reason string) {
	t.metrics.CountOfGuardTrips.With(
		t.metrics.LabelsForWith("reason", reason)...,
	).Add(1)
}

func (t *BatchTuner) countAdjustment(param, direction string) {
	t.metrics.CountOfAdjustments.With(
		t.metrics.LabelsForWith("param", param, "direction", direction)...,
	).Add(1)
}

// apply is the only write path: it clamps both knobs to their bounds and publishes them to the batcher if
// anything changed (a Reset always publishes, so that the batcher is known to hold (CInit, T0)).
func (t *BatchTuner) apply(c uint64, d time.Duration, why string) {
	c = min(max(c, t.cfg.CMin), t.cfg.CCap)
	d = min(max(d, t.cfg.TMin), t.cfg.TMax)
	if c == t.c && d == t.t && why != tunerReasonReset {
		return
	}
	switch {
	case c > t.c:
		t.countAdjustment(api.BatchParamCount, api.DirectionUp)
	case c < t.c:
		t.countAdjustment(api.BatchParamCount, api.DirectionDown)
	}
	switch {
	case d > t.t:
		t.countAdjustment(api.BatchParamTimeout, api.DirectionUp)
	case d < t.t:
		t.countAdjustment(api.BatchParamTimeout, api.DirectionDown)
	}
	t.c, t.t = c, d
	t.batcher.SetBatchParams(BatchParams{MaxMsgCount: c, MaxSizeBytes: t.cfg.SFixed, Timeout: d})
	t.logger.Debugf("Adaptive batch: max count %d, timeout %s (%s)", c, d, why)
}

// mulUp returns c scaled up by u, at least c+1.
func mulUp(c uint64, u float64) uint64 {
	return max(c+1, uint64(math.Ceil(u*float64(c))))
}

// mulDown returns c scaled down by d, at most c-1, and never below 1.
func mulDown(c uint64, d float64) uint64 {
	if c <= 1 {
		return 1
	}
	return min(c-1, uint64(math.Floor(d*float64(c))))
}

// mulUpD returns t scaled up by u, rounded up to the nanosecond.
func mulUpD(t time.Duration, u float64) time.Duration {
	return time.Duration(math.Ceil(u * float64(t)))
}

// mulDownD returns t scaled down by d, rounded down to the nanosecond.
func mulDownD(t time.Duration, d float64) time.Duration {
	return time.Duration(math.Floor(d * float64(t)))
}
