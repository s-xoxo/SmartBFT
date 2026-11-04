package api

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hyperledger/SmartBFT/pkg/metrics"
)

const (
	ReasonRequestMaxBytes      = "MAX_BYTES"
	ReasonSemaphoreAcquireFail = "SEMAPHORE_ACQUIRE_FAIL"

	// ReasonBatchFull labels a batch that was returned because it reached the maximum request count or size.
	ReasonBatchFull = "FULL"
	// ReasonBatchTimeout labels a batch that was returned because the batch timeout expired before it got full.
	ReasonBatchTimeout = "TIMEOUT"

	// BatchParamCount and BatchParamTimeout label an adaptive batch adjustment by the parameter it changed.
	BatchParamCount   = "COUNT"
	BatchParamTimeout = "TIMEOUT"
	// DirectionUp and DirectionDown label an adaptive batch adjustment by its direction.
	DirectionUp   = "UP"
	DirectionDown = "DOWN"
	// GuardDrain labels a trip of the request-age guard: a request waited too long in the leader's pool,
	// so the batcher switched to maximal throughput. GuardLatency labels a trip of the round-latency guard:
	// the leader's propose-to-decide latency rose well above its recent minimum, so the batch count was reduced.
	GuardDrain   = "DRAIN"
	GuardLatency = "LATENCY"
)

func NewGaugeOpts(old metrics.GaugeOpts, labelNames []string) metrics.GaugeOpts {
	return metrics.GaugeOpts{
		Namespace:    old.Namespace,
		Subsystem:    old.Subsystem,
		Name:         old.Name,
		Help:         old.Help,
		LabelNames:   makeLabelNames(labelNames, old.LabelNames...),
		LabelHelp:    old.LabelHelp,
		StatsdFormat: makeStatsdFormat(labelNames, old.StatsdFormat),
	}
}

func NewCounterOpts(old metrics.CounterOpts, labelNames []string) metrics.CounterOpts {
	return metrics.CounterOpts{
		Namespace:    old.Namespace,
		Subsystem:    old.Subsystem,
		Name:         old.Name,
		Help:         old.Help,
		LabelNames:   makeLabelNames(labelNames, old.LabelNames...),
		LabelHelp:    old.LabelHelp,
		StatsdFormat: makeStatsdFormat(labelNames, old.StatsdFormat),
	}
}

func NewHistogramOpts(old metrics.HistogramOpts, labelNames []string) metrics.HistogramOpts {
	return metrics.HistogramOpts{
		Namespace:    old.Namespace,
		Subsystem:    old.Subsystem,
		Name:         old.Name,
		Help:         old.Help,
		Buckets:      old.Buckets,
		LabelNames:   makeLabelNames(labelNames, old.LabelNames...),
		LabelHelp:    old.LabelHelp,
		StatsdFormat: makeStatsdFormat(labelNames, old.StatsdFormat),
	}
}

func makeStatsdFormat(labelNames []string, str string) string {
	sort.Strings(labelNames)
	var b strings.Builder
	b.WriteString(str)
	for _, s := range labelNames {
		fmt.Fprintf(&b, ".%%{%s}", s)
	}

	return b.String()
}

func makeLabelNames(labelNames []string, names ...string) []string {
	ln := make([]string, 0, len(names)+len(labelNames))
	ln = append(ln, names...)
	sort.Strings(labelNames)
	ln = append(ln, labelNames...)
	return ln
}

type Metrics struct {
	MetricsRequestPool *MetricsRequestPool
	MetricsBatcher     *MetricsBatcher
	MetricsBlacklist   *MetricsBlacklist
	MetricsConsensus   *MetricsConsensus
	MetricsView        *MetricsView
	MetricsViewChange  *MetricsViewChange
	MetricsState       *MetricsState
}

func NewMetrics(p metrics.Provider, labelNames ...string) *Metrics {
	return &Metrics{
		MetricsRequestPool: NewMetricsRequestPool(p, labelNames...),
		MetricsBatcher:     NewMetricsBatcher(p, labelNames...),
		MetricsBlacklist:   NewMetricsBlacklist(p, labelNames...),
		MetricsConsensus:   NewMetricsConsensus(p, labelNames...),
		MetricsView:        NewMetricsView(p, labelNames...),
		MetricsViewChange:  NewMetricsViewChange(p, labelNames...),
		MetricsState:       NewMetricsState(p, labelNames...),
	}
}

func (m *Metrics) With(labelValues ...string) *Metrics {
	return &Metrics{
		MetricsRequestPool: m.MetricsRequestPool.With(labelValues...),
		MetricsBatcher:     m.MetricsBatcher.With(labelValues...),
		MetricsBlacklist:   m.MetricsBlacklist.With(labelValues...),
		MetricsConsensus:   m.MetricsConsensus.With(labelValues...),
		MetricsView:        m.MetricsView.With(labelValues...),
		MetricsViewChange:  m.MetricsViewChange.With(labelValues...),
		MetricsState:       m.MetricsState.With(labelValues...),
	}
}

func (m *Metrics) Initialize(nodes []uint64) {
	m.MetricsRequestPool.Initialize()
	m.MetricsBatcher.Initialize()
	m.MetricsBlacklist.Initialize(nodes)
	m.MetricsConsensus.Initialize()
	m.MetricsView.Initialize()
	m.MetricsViewChange.Initialize()
	m.MetricsState.Initialize()
}

// walRecordsSinceBoundaryOpts is the number of WAL records since the last truncation point, that one
// included: what a restart reads back. With a single proposal in flight at a time it is 1 or 2; with a
// pipeline it grows while the lowest open sequence stays open.
var walRecordsSinceBoundaryOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "wal_records_since_boundary",
	Help:         "Number of WAL records since the last truncation point, which a restart would read back.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsState encapsulates the metrics of the persisted state (the records written to the WAL).
type MetricsState struct {
	RecordsSinceBoundary metrics.Gauge
}

// NewMetricsState create new state metrics
func NewMetricsState(p metrics.Provider, labelNames ...string) *MetricsState {
	return &MetricsState{
		RecordsSinceBoundary: p.NewGauge(NewGaugeOpts(walRecordsSinceBoundaryOpts, labelNames)),
	}
}

func (m *MetricsState) With(labelValues ...string) *MetricsState {
	return &MetricsState{RecordsSinceBoundary: m.RecordsSinceBoundary.With(labelValues...)}
}

func (m *MetricsState) Initialize() {
	m.RecordsSinceBoundary.Add(0)
}

var countOfRequestPoolOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_of_elements",
	Help:         "Number of elements in the consensus request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// sizeBytesOfRequestPoolOpts mirrors the byte total the pool keeps for its batching size limit.
var sizeBytesOfRequestPoolOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_size_bytes_of_elements",
	Help:         "Total size in bytes of the elements in the consensus request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfFailAddRequestToPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_of_fail_add_request",
	Help:         "Number of requests pool insertion failure.",
	LabelNames:   []string{"reason"},
	StatsdFormat: "%{#fqname}.%{reason}",
}

// ForwardTimeout
var countOfLeaderForwardRequestOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_leader_forward_request",
	Help:         "Number of requests forwarded to the leader.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTimeoutTwoStepOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_timeout_two_step",
	Help:         "Number of times requests reached second timeout.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfDeleteRequestPoolOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_of_delete_request",
	Help:         "Number of elements removed from the request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countOfRequestPoolAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_count_of_elements_all",
	Help:         "Total amount of elements in the request pool.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencyOfRequestPoolOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "pool_latency_of_elements",
	Help:         "The average request processing time, time request resides in the pool.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsRequestPool encapsulates request pool metrics
type MetricsRequestPool struct {
	CountOfRequestPool          metrics.Gauge
	SizeBytesOfRequestPool      metrics.Gauge
	CountOfFailAddRequestToPool metrics.Counter
	CountOfLeaderForwardRequest metrics.Counter
	CountTimeoutTwoStep         metrics.Counter
	CountOfDeleteRequestPool    metrics.Counter
	CountOfRequestPoolAll       metrics.Counter
	LatencyOfRequestPool        metrics.Histogram

	labels []string
}

// NewMetricsRequestPool create new request pool metrics
func NewMetricsRequestPool(p metrics.Provider, labelNames ...string) *MetricsRequestPool {
	countOfRequestPoolOptsTmp := NewGaugeOpts(countOfRequestPoolOpts, labelNames)
	sizeBytesOfRequestPoolOptsTmp := NewGaugeOpts(sizeBytesOfRequestPoolOpts, labelNames)
	countOfFailAddRequestToPoolOptsTmp := NewCounterOpts(countOfFailAddRequestToPoolOpts, labelNames)
	countOfLeaderForwardRequestOptsTmp := NewCounterOpts(countOfLeaderForwardRequestOpts, labelNames)
	countTimeoutTwoStepOptsTmp := NewCounterOpts(countTimeoutTwoStepOpts, labelNames)
	countOfDeleteRequestPoolOptsTmp := NewCounterOpts(countOfDeleteRequestPoolOpts, labelNames)
	countOfRequestPoolAllOptsTmp := NewCounterOpts(countOfRequestPoolAllOpts, labelNames)
	latencyOfRequestPoolOptsTmp := NewHistogramOpts(latencyOfRequestPoolOpts, labelNames)
	return &MetricsRequestPool{
		CountOfRequestPool:          p.NewGauge(countOfRequestPoolOptsTmp),
		SizeBytesOfRequestPool:      p.NewGauge(sizeBytesOfRequestPoolOptsTmp),
		CountOfFailAddRequestToPool: p.NewCounter(countOfFailAddRequestToPoolOptsTmp),
		CountOfLeaderForwardRequest: p.NewCounter(countOfLeaderForwardRequestOptsTmp),
		CountTimeoutTwoStep:         p.NewCounter(countTimeoutTwoStepOptsTmp),
		CountOfDeleteRequestPool:    p.NewCounter(countOfDeleteRequestPoolOptsTmp),
		CountOfRequestPoolAll:       p.NewCounter(countOfRequestPoolAllOptsTmp),
		LatencyOfRequestPool:        p.NewHistogram(latencyOfRequestPoolOptsTmp),
	}
}

func (m *MetricsRequestPool) With(labelValues ...string) *MetricsRequestPool {
	return &MetricsRequestPool{
		CountOfRequestPool:          m.CountOfRequestPool.With(labelValues...),
		SizeBytesOfRequestPool:      m.SizeBytesOfRequestPool.With(labelValues...),
		CountOfFailAddRequestToPool: m.CountOfFailAddRequestToPool,
		CountOfLeaderForwardRequest: m.CountOfLeaderForwardRequest.With(labelValues...),
		CountTimeoutTwoStep:         m.CountTimeoutTwoStep.With(labelValues...),
		CountOfDeleteRequestPool:    m.CountOfDeleteRequestPool.With(labelValues...),
		CountOfRequestPoolAll:       m.CountOfRequestPoolAll.With(labelValues...),
		LatencyOfRequestPool:        m.LatencyOfRequestPool.With(labelValues...),
		labels:                      labelValues,
	}
}

func (m *MetricsRequestPool) Initialize() {
	m.CountOfRequestPool.Add(0)
	m.SizeBytesOfRequestPool.Add(0)
	m.CountOfFailAddRequestToPool.With(
		m.LabelsForWith("reason", ReasonRequestMaxBytes)...,
	).Add(0)
	m.CountOfFailAddRequestToPool.With(
		m.LabelsForWith("reason", ReasonSemaphoreAcquireFail)...,
	).Add(0)
	m.CountOfLeaderForwardRequest.Add(0)
	m.CountTimeoutTwoStep.Add(0)
	m.CountOfDeleteRequestPool.Add(0)
	m.CountOfRequestPoolAll.Add(0)
	m.LatencyOfRequestPool.Observe(0)
}

func (m *MetricsRequestPool) LabelsForWith(labelValues ...string) []string {
	result := make([]string, 0, len(m.labels)+len(labelValues))
	result = append(result, labelValues...)
	result = append(result, m.labels...)
	return result
}

// countOfBatchesOpts counts the non-empty batches the batcher returned, labeled by why the batch
// was closed: it reached the maximum request count or size (FULL), or the batch timeout expired (TIMEOUT).
// Empty batches returned after a timeout on an idle pool are not counted.
var countOfBatchesOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "batcher_count_of_batches",
	Help:         "Number of batches returned by the batcher, by the reason the batch was closed.",
	LabelNames:   []string{"reason"},
	StatsdFormat: "%{#fqname}.%{reason}",
}

// batcherMaxCountOpts is the maximal number of requests in a batch the batcher currently uses.
// It is the configured RequestBatchMaxCount unless adaptive batching changes it at runtime.
var batcherMaxCountOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "batcher_max_count",
	Help:         "Maximal number of requests in a batch currently in effect.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// batcherTimeoutSecondsOpts is the batch timeout the batcher currently uses, in seconds.
// It is the configured RequestBatchMaxInterval unless adaptive batching changes it at runtime.
var batcherTimeoutSecondsOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "batcher_timeout_seconds",
	Help:         "Batch timeout currently in effect, in seconds.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// batcherCountOfAdjustmentsOpts counts the adaptive batch adjustments, by the parameter that changed
// (COUNT or TIMEOUT) and the direction (UP or DOWN). Only knobs whose value actually changed are counted.
var batcherCountOfAdjustmentsOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "batcher_count_of_adjustments",
	Help:         "Number of adaptive batch parameter adjustments, by parameter and direction.",
	LabelNames:   []string{"param", "direction"},
	StatsdFormat: "%{#fqname}.%{param}.%{direction}",
}

// batcherCountOfGuardTripsOpts counts how often an adaptive batch guard overrode the regular rules,
// by the guard (DRAIN: a request waited too long in the leader's pool; LATENCY: round latency inflated).
var batcherCountOfGuardTripsOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "batcher_count_of_guard_trips",
	Help:         "Number of times an adaptive batch guard fired, by reason.",
	LabelNames:   []string{"reason"},
	StatsdFormat: "%{#fqname}.%{reason}",
}

// MetricsBatcher encapsulates batcher metrics
type MetricsBatcher struct {
	CountOfBatches     metrics.Counter
	MaxCount           metrics.Gauge
	TimeoutSeconds     metrics.Gauge
	CountOfAdjustments metrics.Counter
	CountOfGuardTrips  metrics.Counter

	labels []string
}

// NewMetricsBatcher create new batcher metrics
func NewMetricsBatcher(p metrics.Provider, labelNames ...string) *MetricsBatcher {
	countOfBatchesOptsTmp := NewCounterOpts(countOfBatchesOpts, labelNames)
	batcherMaxCountOptsTmp := NewGaugeOpts(batcherMaxCountOpts, labelNames)
	batcherTimeoutSecondsOptsTmp := NewGaugeOpts(batcherTimeoutSecondsOpts, labelNames)
	batcherCountOfAdjustmentsOptsTmp := NewCounterOpts(batcherCountOfAdjustmentsOpts, labelNames)
	batcherCountOfGuardTripsOptsTmp := NewCounterOpts(batcherCountOfGuardTripsOpts, labelNames)
	return &MetricsBatcher{
		CountOfBatches:     p.NewCounter(countOfBatchesOptsTmp),
		MaxCount:           p.NewGauge(batcherMaxCountOptsTmp),
		TimeoutSeconds:     p.NewGauge(batcherTimeoutSecondsOptsTmp),
		CountOfAdjustments: p.NewCounter(batcherCountOfAdjustmentsOptsTmp),
		CountOfGuardTrips:  p.NewCounter(batcherCountOfGuardTripsOptsTmp),
	}
}

func (m *MetricsBatcher) With(labelValues ...string) *MetricsBatcher {
	return &MetricsBatcher{
		CountOfBatches:     m.CountOfBatches,
		MaxCount:           m.MaxCount.With(labelValues...),
		TimeoutSeconds:     m.TimeoutSeconds.With(labelValues...),
		CountOfAdjustments: m.CountOfAdjustments,
		CountOfGuardTrips:  m.CountOfGuardTrips,
		labels:             labelValues,
	}
}

func (m *MetricsBatcher) Initialize() {
	m.CountOfBatches.With(
		m.LabelsForWith("reason", ReasonBatchFull)...,
	).Add(0)
	m.CountOfBatches.With(
		m.LabelsForWith("reason", ReasonBatchTimeout)...,
	).Add(0)
	m.MaxCount.Add(0)
	m.TimeoutSeconds.Add(0)
	for _, param := range []string{BatchParamCount, BatchParamTimeout} {
		for _, direction := range []string{DirectionUp, DirectionDown} {
			m.CountOfAdjustments.With(
				m.LabelsForWith("param", param, "direction", direction)...,
			).Add(0)
		}
	}
	m.CountOfGuardTrips.With(
		m.LabelsForWith("reason", GuardDrain)...,
	).Add(0)
	m.CountOfGuardTrips.With(
		m.LabelsForWith("reason", GuardLatency)...,
	).Add(0)
}

func (m *MetricsBatcher) LabelsForWith(labelValues ...string) []string {
	result := make([]string, 0, len(m.labels)+len(labelValues))
	result = append(result, labelValues...)
	result = append(result, m.labels...)
	return result
}

var countBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "blacklist_count",
	Help:         "Count of nodes in blacklist on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nodesInBlackListOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "node_id_in_blacklist",
	Help:         "Node ID in blacklist on this channel.",
	LabelNames:   []string{"blackid"},
	StatsdFormat: "%{#fqname}.%{blackid}",
}

// MetricsBlacklist encapsulates blacklist metrics
type MetricsBlacklist struct {
	CountBlackList   metrics.Gauge
	NodesInBlackList metrics.Gauge

	labels []string
}

// NewMetricsBlacklist create new blacklist metrics
func NewMetricsBlacklist(p metrics.Provider, labelNames ...string) *MetricsBlacklist {
	countBlackListOptsTmp := NewGaugeOpts(countBlackListOpts, labelNames)
	nodesInBlackListOptsTmp := NewGaugeOpts(nodesInBlackListOpts, labelNames)
	return &MetricsBlacklist{
		CountBlackList:   p.NewGauge(countBlackListOptsTmp),
		NodesInBlackList: p.NewGauge(nodesInBlackListOptsTmp),
	}
}

func (m *MetricsBlacklist) With(labelValues ...string) *MetricsBlacklist {
	return &MetricsBlacklist{
		CountBlackList:   m.CountBlackList.With(labelValues...),
		NodesInBlackList: m.NodesInBlackList,
		labels:           labelValues,
	}
}

func (m *MetricsBlacklist) Initialize(nodes []uint64) {
	m.CountBlackList.Add(0)
	for _, n := range nodes {
		m.NodesInBlackList.With(
			m.LabelsForWith("blackid", strconv.FormatUint(n, 10))...,
		).Set(0)
	}
}

func (m *MetricsBlacklist) LabelsForWith(labelValues ...string) []string {
	result := make([]string, 0, len(m.labels)+len(labelValues))
	result = append(result, labelValues...)
	result = append(result, m.labels...)
	return result
}

var consensusReconfigOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "consensus_reconfig",
	Help:         "Number of reconfiguration requests.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencySyncOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "consensus_latency_sync",
	Help:         "An average time it takes to sync node.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsConsensus encapsulates consensus metrics
type MetricsConsensus struct {
	CountConsensusReconfig metrics.Counter
	LatencySync            metrics.Histogram
}

// NewMetricsConsensus create new consensus metrics
func NewMetricsConsensus(p metrics.Provider, labelNames ...string) *MetricsConsensus {
	consensusReconfigOptsTmp := NewCounterOpts(consensusReconfigOpts, labelNames)
	latencySyncOptsTmp := NewHistogramOpts(latencySyncOpts, labelNames)
	return &MetricsConsensus{
		CountConsensusReconfig: p.NewCounter(consensusReconfigOptsTmp),
		LatencySync:            p.NewHistogram(latencySyncOptsTmp),
	}
}

func (m *MetricsConsensus) With(labelValues ...string) *MetricsConsensus {
	return &MetricsConsensus{
		CountConsensusReconfig: m.CountConsensusReconfig.With(labelValues...),
		LatencySync:            m.LatencySync.With(labelValues...),
	}
}

func (m *MetricsConsensus) Initialize() {
	m.CountConsensusReconfig.Add(0)
	m.LatencySync.Observe(0)
}

var viewNumberOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_number",
	Help:         "The View number value.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var leaderIDOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_leader_id",
	Help:         "The leader id.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var proposalSequenceOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_proposal_sequence",
	Help:         "The sequence number within current view.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var decisionsInViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_decisions",
	Help:         "The number of decisions in the current view.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var phaseOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_phase",
	Help:         "Current consensus phase.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTxsInBatchOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_count_txs_in_batch",
	Help:         "The number of transactions per batch.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countBatchAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_count_batch_all",
	Help:         "Amount of batched processed.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var countTxsAllOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_count_txs_all",
	Help:         "Total amount of transactions.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// sizeOfBatchOpts is a Counter, not an average: it accumulates the bytes of every decided batch
// (proposal header, payload and metadata plus the commit signatures), so the mean batch size is
// its rate divided by the rate of view_count_batch_all.
var sizeOfBatchOpts = metrics.CounterOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_size_batch",
	Help:         "An average batch size.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var latencyBatchProcessingOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_latency_batch_processing",
	Help:         "Amount of time it take to process batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// latencyBatchSaveOpts measures the time Application.Deliver takes for a decided batch
// (see Controller.Deliver), not the time it takes to persist a record to the WAL; the WAL
// append latency is wal_latency_append in pkg/wal.
var latencyBatchSaveOpts = metrics.HistogramOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_latency_batch_save",
	Help:         "An average time it takes to persist batch.",
	Buckets:      []float64{0.005, 0.01, 0.015, 0.05, 0.1, 1, 10},
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// inFlightProposalsOpts is the number of proposals this node has pre-prepared (sent a prepare for)
// and not yet delivered. With a single proposal in flight at a time it is 0 or 1.
var inFlightProposalsOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "view_in_flight_proposals",
	Help:         "Number of proposals pre-prepared and not yet delivered.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsView encapsulates view metrics
type MetricsView struct {
	ViewNumber             metrics.Gauge
	LeaderID               metrics.Gauge
	ProposalSequence       metrics.Gauge
	DecisionsInView        metrics.Gauge
	Phase                  metrics.Gauge
	CountTxsInBatch        metrics.Gauge
	CountBatchAll          metrics.Counter
	CountTxsAll            metrics.Counter
	SizeOfBatch            metrics.Counter
	LatencyBatchProcessing metrics.Histogram
	LatencyBatchSave       metrics.Histogram
	InFlightProposals      metrics.Gauge
}

// NewMetricsView create new view metrics
func NewMetricsView(p metrics.Provider, labelNames ...string) *MetricsView {
	viewNumberOptsTmp := NewGaugeOpts(viewNumberOpts, labelNames)
	leaderIDOptsTmp := NewGaugeOpts(leaderIDOpts, labelNames)
	proposalSequenceOptsTmp := NewGaugeOpts(proposalSequenceOpts, labelNames)
	decisionsInViewOptsTmp := NewGaugeOpts(decisionsInViewOpts, labelNames)
	phaseOptsTmp := NewGaugeOpts(phaseOpts, labelNames)
	countTxsInBatchOptsTmp := NewGaugeOpts(countTxsInBatchOpts, labelNames)
	countBatchAllOptsTmp := NewCounterOpts(countBatchAllOpts, labelNames)
	countTxsAllOptsTmp := NewCounterOpts(countTxsAllOpts, labelNames)
	sizeOfBatchOptsTmp := NewCounterOpts(sizeOfBatchOpts, labelNames)
	latencyBatchProcessingOptsTmp := NewHistogramOpts(latencyBatchProcessingOpts, labelNames)
	latencyBatchSaveOptsTmp := NewHistogramOpts(latencyBatchSaveOpts, labelNames)
	inFlightProposalsOptsTmp := NewGaugeOpts(inFlightProposalsOpts, labelNames)
	return &MetricsView{
		ViewNumber:             p.NewGauge(viewNumberOptsTmp),
		LeaderID:               p.NewGauge(leaderIDOptsTmp),
		ProposalSequence:       p.NewGauge(proposalSequenceOptsTmp),
		DecisionsInView:        p.NewGauge(decisionsInViewOptsTmp),
		Phase:                  p.NewGauge(phaseOptsTmp),
		CountTxsInBatch:        p.NewGauge(countTxsInBatchOptsTmp),
		CountBatchAll:          p.NewCounter(countBatchAllOptsTmp),
		CountTxsAll:            p.NewCounter(countTxsAllOptsTmp),
		SizeOfBatch:            p.NewCounter(sizeOfBatchOptsTmp),
		LatencyBatchProcessing: p.NewHistogram(latencyBatchProcessingOptsTmp),
		LatencyBatchSave:       p.NewHistogram(latencyBatchSaveOptsTmp),
		InFlightProposals:      p.NewGauge(inFlightProposalsOptsTmp),
	}
}

func (m *MetricsView) With(labelValues ...string) *MetricsView {
	return &MetricsView{
		ViewNumber:             m.ViewNumber.With(labelValues...),
		LeaderID:               m.LeaderID.With(labelValues...),
		ProposalSequence:       m.ProposalSequence.With(labelValues...),
		DecisionsInView:        m.DecisionsInView.With(labelValues...),
		Phase:                  m.Phase.With(labelValues...),
		CountTxsInBatch:        m.CountTxsInBatch.With(labelValues...),
		CountBatchAll:          m.CountBatchAll.With(labelValues...),
		CountTxsAll:            m.CountTxsAll.With(labelValues...),
		SizeOfBatch:            m.SizeOfBatch.With(labelValues...),
		LatencyBatchProcessing: m.LatencyBatchProcessing.With(labelValues...),
		LatencyBatchSave:       m.LatencyBatchSave.With(labelValues...),
		InFlightProposals:      m.InFlightProposals.With(labelValues...),
	}
}

func (m *MetricsView) Initialize() {
	m.ViewNumber.Add(0)
	m.LeaderID.Add(0)
	m.ProposalSequence.Add(0)
	m.DecisionsInView.Add(0)
	m.Phase.Add(0)
	m.CountTxsInBatch.Add(0)
	m.CountBatchAll.Add(0)
	m.CountTxsAll.Add(0)
	m.SizeOfBatch.Add(0)
	m.LatencyBatchProcessing.Observe(0)
	m.LatencyBatchSave.Observe(0)
	m.InFlightProposals.Add(0)
}

var currentViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "viewchange_current_view",
	Help:         "current view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var nextViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "viewchange_next_view",
	Help:         "next view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

var realViewOpts = metrics.GaugeOpts{
	Namespace:    "consensus",
	Subsystem:    "smartbft",
	Name:         "viewchange_real_view",
	Help:         "real view of viewchange on this channel.",
	LabelNames:   []string{},
	StatsdFormat: "%{#fqname}",
}

// MetricsViewChange encapsulates view change metrics
type MetricsViewChange struct {
	CurrentView metrics.Gauge
	NextView    metrics.Gauge
	RealView    metrics.Gauge
}

// NewMetricsViewChange create new view change metrics
func NewMetricsViewChange(p metrics.Provider, labelNames ...string) *MetricsViewChange {
	currentViewOptsTmp := NewGaugeOpts(currentViewOpts, labelNames)
	nextViewOptsTmp := NewGaugeOpts(nextViewOpts, labelNames)
	realViewOptsTmp := NewGaugeOpts(realViewOpts, labelNames)
	return &MetricsViewChange{
		CurrentView: p.NewGauge(currentViewOptsTmp),
		NextView:    p.NewGauge(nextViewOptsTmp),
		RealView:    p.NewGauge(realViewOptsTmp),
	}
}

func (m *MetricsViewChange) With(labelValues ...string) *MetricsViewChange {
	return &MetricsViewChange{
		CurrentView: m.CurrentView.With(labelValues...),
		NextView:    m.NextView.With(labelValues...),
		RealView:    m.RealView.With(labelValues...),
	}
}

func (m *MetricsViewChange) Initialize() {
	m.CurrentView.Add(0)
	m.NextView.Add(0)
	m.RealView.Add(0)
}
