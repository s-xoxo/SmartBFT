// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// These tests cover the pipeline window of the view (design-pipeline-window.md sections 3-5): routing by
// sequence, the in-order delivery gate, the prefix rule for prepares, the leader's pre-prepare resend
// (depth above one only), the log level of a duplicate pre-prepare and the DecisionsInView prediction.
// They live in package bft to reach the slots, head and the resend tick.

const (
	windowView      = uint64(1)
	windowLeader    = uint64(1)
	windowN         = uint64(4)
	windowQuorum    = 3
	windowLow       = uint64(10)
	windowDecisions = uint64(3)
)

var windowRequests = []types.RequestInfo{{ClientID: "c", ID: "1"}}

// fakeComm records what the view broadcasts and sends, and signals every broadcast.
type fakeComm struct {
	mu         sync.Mutex
	broadcasts []*protos.Message
	sent       []sentMessage
	broadcast  chan *protos.Message
}

type sentMessage struct {
	target  uint64
	message *protos.Message
}

func newFakeComm() *fakeComm {
	return &fakeComm{broadcast: make(chan *protos.Message, 1000)}
}

func (c *fakeComm) BroadcastConsensus(m *protos.Message) {
	c.mu.Lock()
	c.broadcasts = append(c.broadcasts, m)
	c.mu.Unlock()
	c.broadcast <- m
}

func (c *fakeComm) SendConsensus(target uint64, m *protos.Message) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, sentMessage{target: target, message: m})
}

func (c *fakeComm) SendTransaction(uint64, []byte) {}

func (c *fakeComm) Nodes() []uint64 { return []uint64{1, 2, 3, 4} }

func (c *fakeComm) broadcastCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.broadcasts)
}

func (c *fakeComm) sentMessages() []sentMessage {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sentMessage(nil), c.sent...)
}

// nextBroadcast waits for the next broadcast message.
func (c *fakeComm) nextBroadcast(t *testing.T, what string) *protos.Message {
	t.Helper()
	select {
	case m := <-c.broadcast:
		return m
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

type fakeVerifier struct{}

func (fakeVerifier) VerificationSequence() uint64 { return 1 }

func (fakeVerifier) VerifyProposal(types.Proposal) ([]types.RequestInfo, error) {
	return windowRequests, nil
}

func (fakeVerifier) VerifyRequest([]byte) (types.RequestInfo, error) {
	return types.RequestInfo{}, nil
}

func (fakeVerifier) VerifyConsenterSig(types.Signature, types.Proposal) ([]byte, error) {
	return nil, nil
}

func (fakeVerifier) VerifySignature(types.Signature) error { return nil }

func (fakeVerifier) RequestsFromProposal(types.Proposal) []types.RequestInfo { return windowRequests }

func (fakeVerifier) AuxiliaryData([]byte) []byte { return nil }

type fakeSigner struct{ id uint64 }

func (fakeSigner) Sign([]byte) []byte { return nil }

func (s fakeSigner) SignProposal(types.Proposal, []byte) *types.Signature {
	return &types.Signature{ID: s.id, Value: []byte{4}}
}

type decisionRecord struct {
	proposal   types.Proposal
	signatures []types.Signature
	requests   []types.RequestInfo
}

type fakeDecider struct{ decisions chan decisionRecord }

func newFakeDecider() *fakeDecider {
	return &fakeDecider{decisions: make(chan decisionRecord, 100)}
}

func (d *fakeDecider) Decide(proposal types.Proposal, signatures []types.Signature, requests []types.RequestInfo) {
	d.decisions <- decisionRecord{proposal: proposal, signatures: signatures, requests: requests}
}

func (d *fakeDecider) next(t *testing.T, what string) decisionRecord {
	t.Helper()
	select {
	case decision := <-d.decisions:
		return decision
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return decisionRecord{}
	}
}

// fakeState records the saved messages under a lock.
type fakeState struct {
	mu    sync.Mutex
	saved []*protos.SavedMessage
}

func (s *fakeState) Save(message *protos.SavedMessage, _ bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saved = append(s.saved, message)
	return nil
}

func (*fakeState) Restore(*View) error { panic("should not be used") }

func (s *fakeState) savedSequences() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var seqs []uint64
	for _, m := range s.saved {
		if proposed := m.GetProposedRecord(); proposed != nil {
			seqs = append(seqs, proposed.GetPrePrepare().GetSeq())
		}
	}
	return seqs
}

// logCollector keeps every log entry and lets a test wait for one.
type logCollector struct {
	mu      sync.Mutex
	entries []zapcore.Entry
	notify  chan struct{}
}

func newLogCollector() (*logCollector, api.Logger) {
	lc := &logCollector{notify: make(chan struct{}, 1)}
	basicLog, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	logger := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		lc.mu.Lock()
		lc.entries = append(lc.entries, entry)
		lc.mu.Unlock()
		select {
		case lc.notify <- struct{}{}:
		default:
		}
		return nil
	})).Sugar()
	return lc, logger
}

func (lc *logCollector) count(substr string) int {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	n := 0
	for _, e := range lc.entries {
		if strings.Contains(e.Message, substr) {
			n++
		}
	}
	return n
}

// level returns the level of the first entry containing substr; the test waited for the entry already.
func (lc *logCollector) level(substr string) zapcore.Level {
	lc.mu.Lock()
	defer lc.mu.Unlock()
	for _, e := range lc.entries {
		if strings.Contains(e.Message, substr) {
			return e.Level
		}
	}
	return zapcore.InvalidLevel
}

// gaugeRecorder records the last value set on every gauge, by name, under a lock: the view goroutine sets
// them while the test reads them.
type gaugeRecorder struct {
	disabled.Provider
	mu     sync.Mutex
	gauges map[string]float64
}

func (p *gaugeRecorder) NewGauge(opts metrics.GaugeOpts) metrics.Gauge {
	return &recordedGauge{recorder: p, name: opts.Name}
}

func (p *gaugeRecorder) gauge(name string) float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gauges[name]
}

type recordedGauge struct {
	recorder *gaugeRecorder
	name     string
}

func (g *recordedGauge) With(...string) metrics.Gauge { return g }

func (g *recordedGauge) Add(delta float64) {
	g.recorder.mu.Lock()
	defer g.recorder.mu.Unlock()
	g.set(g.recorder.gauges[g.name] + delta)
}

func (g *recordedGauge) Set(value float64) {
	g.recorder.mu.Lock()
	defer g.recorder.mu.Unlock()
	g.set(value)
}

func (g *recordedGauge) set(value float64) {
	if g.recorder.gauges == nil {
		g.recorder.gauges = make(map[string]float64)
	}
	g.recorder.gauges[g.name] = value
}

func (lc *logCollector) waitFor(t *testing.T, substr string) {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for lc.count(substr) == 0 {
		select {
		case <-lc.notify:
		case <-deadline:
			t.Fatalf("timed out waiting for a log entry containing %q", substr)
		}
	}
}

// windowProposal is the proposal of the given sequence, carrying the DecisionsInView the view expects for it.
func windowProposal(seq, decisions uint64) types.Proposal {
	return types.Proposal{
		Header:  []byte{0},
		Payload: []byte{byte(seq)},
		Metadata: MarshalOrPanic(&protos.ViewMetadata{
			ViewId:          windowView,
			LatestSequence:  seq,
			DecisionsInView: decisions,
		}),
		VerificationSequence: 1,
	}
}

// windowDecisionsFor is expectedDecisions(seq) for a view that started at windowLow with windowDecisions.
func windowDecisionsFor(seq uint64) uint64 {
	return windowDecisions + (seq - windowLow)
}

func prePrepareMsg(seq uint64, proposal types.Proposal) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: windowView,
				Seq:  seq,
				Proposal: &protos.Proposal{
					Header:               proposal.Header,
					Payload:              proposal.Payload,
					Metadata:             proposal.Metadata,
					VerificationSequence: uint64(proposal.VerificationSequence),
				},
			},
		},
	}
}

func prepareMsg(seq uint64, digest string, assist bool) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{View: windowView, Seq: seq, Digest: digest, Assist: assist},
		},
	}
}

func commitMsg(seq uint64, digest string, signer uint64, assist bool) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:      windowView,
				Seq:       seq,
				Digest:    digest,
				Assist:    assist,
				Signature: &protos.Signature{Signer: signer, Value: []byte{4}},
			},
		},
	}
}

type windowFixture struct {
	view    *View
	comm    *fakeComm
	decider *fakeDecider
	state   *fakeState
	logs    *logCollector
	metrics *gaugeRecorder
}

func newWindowView(selfID, depth uint64) *windowFixture {
	logs, logger := newLogCollector()
	f := &windowFixture{
		comm:    newFakeComm(),
		decider: newFakeDecider(),
		state:   &fakeState{},
		logs:    logs,
		metrics: &gaugeRecorder{},
	}
	f.view = &View{
		RetrieveCheckpoint: (&types.Checkpoint{}).Get,
		SelfID:             selfID,
		N:                  windowN,
		NodesList:          []uint64{1, 2, 3, 4},
		LeaderID:           windowLeader,
		Quorum:             windowQuorum,
		Number:             windowView,
		Decider:            f.decider,
		Logger:             logger,
		Comm:               f.comm,
		Verifier:           fakeVerifier{},
		Signer:             fakeSigner{id: selfID},
		ProposalSequence:   windowLow,
		DecisionsInView:    windowDecisions,
		State:              f.state,
		InMsgQSize:         100,
		PipelineDepth:      depth,
		MetricsView:        api.NewMetricsView(f.metrics),
		MetricsBlacklist:   api.NewMetricsBlacklist(&disabled.Provider{}),
		ViewSequences:      &atomic.Value{},
	}
	f.view.ViewSequences.Store(ViewSequence{ViewActive: true, ProposalSeq: windowLow})
	return f
}

// prepareAndCommit feeds the view a prepare quorum and a commit quorum for the sequence from the given
// senders, waiting for the view's commit in between.
func (f *windowFixture) prepareAndCommit(t *testing.T, seq uint64, digest string, senders ...uint64) {
	t.Helper()
	for _, sender := range senders {
		f.view.HandleMessage(sender, prepareMsg(seq, digest, false))
	}
	commit := f.comm.nextBroadcast(t, fmt.Sprintf("the commit of sequence %d", seq))
	require.NotNil(t, commit.GetCommit(), "expected a commit, got %s", MsgToString(commit))
	require.Equal(t, seq, commit.GetCommit().GetSeq())
	for _, sender := range senders {
		f.view.HandleMessage(sender, commitMsg(seq, digest, sender, false))
	}
}

func viewSequenceOf(v *View) ViewSequence {
	return v.ViewSequences.Load().(ViewSequence)
}

// TestViewSlotRouting covers processMsg's routing by sequence for depths 1, 2 and 4 (I1, I2, I8): a sequence
// below the window of delivered sequences or above the accepted window creates no slot (and a commit there
// feeds discoverIfSyncNeeded), a delivered sequence is answered with the prepare or commit sent for it, and
// [low, low+W] is accepted. With depth 1 the split is {low-1}, {low, low+1} and the rest, as it was.
func TestViewSlotRouting(t *testing.T) {
	for _, depth := range []uint64{1, 2, 4} {
		t.Run(fmt.Sprintf("depth %d", depth), func(t *testing.T) {
			f := newWindowView(2, depth)
			v := f.view
			v.setup() // the runtime state without the goroutine: processMsg is called directly
			low := windowLow
			// The delivered sequences [low-W, low) keep what was sent for them.
			for seq := low - depth; seq < low; seq++ {
				v.slots[seq] = &seqState{
					seq:         seq,
					phase:       slotDelivered,
					prepareSent: prepareMsg(seq, "d", true),
					commitSent:  commitMsg(seq, "d", 2, true),
				}
			}

			// Below the delivered window: no slot, a commit is remembered for discovering a sync.
			tooOld := low - depth - 1
			v.processMsg(3, prepareMsg(tooOld, "d", false))
			v.processMsg(3, commitMsg(tooOld, "d", 3, false))
			require.NotContains(t, v.slots, tooOld)
			require.Contains(t, v.lastVotedProposalByID, uint64(3))
			require.Empty(t, f.comm.sentMessages())
			require.Equal(t, 2, f.logs.count(fmt.Sprintf("2 got message from 3 with sequence %d but our sequence is %d", tooOld, low)))

			// Delivered sequences: answered from the slot, assist messages and pre-prepares are not.
			for _, seq := range []uint64{low - depth, low - 1} {
				before := len(f.comm.sentMessages())
				v.processMsg(3, prepareMsg(seq, "d", false))
				v.processMsg(4, commitMsg(seq, "d", 4, false))
				v.processMsg(3, prepareMsg(seq, "d", true))
				v.processMsg(3, commitMsg(seq, "d", 3, true))
				v.processMsg(1, prePrepareMsg(seq, windowProposal(seq, 0)))
				sent := f.comm.sentMessages()[before:]
				require.Len(t, sent, 2)
				require.Equal(t, uint64(3), sent[0].target)
				require.True(t, proto.Equal(v.slots[seq].prepareSent, sent[0].message))
				require.Equal(t, uint64(4), sent[1].target)
				require.True(t, proto.Equal(v.slots[seq].commitSent, sent[1].message))
				require.Equal(t, slotDelivered, v.slots[seq].phase, "a delivered slot must not be reopened")
			}
			require.Equal(t, 2, f.logs.count("Got pre-prepare for sequence"))

			// Accepted: [low, low+W] get a slot and register votes; own votes and pre-prepares from others are ignored.
			accepted := []uint64{low, low + depth - 1, low + depth}
			if depth == 1 {
				accepted = []uint64{low, low + 1}
			}
			for _, seq := range accepted {
				v.processMsg(3, prepareMsg(seq, "d", false))
				v.processMsg(2, prepareMsg(seq, "d", false)) // our own vote
				v.processMsg(4, commitMsg(seq, "d", 4, false))
				v.processMsg(3, prePrepareMsg(seq, windowProposal(seq, 0)))                // not the leader
				v.processMsg(windowLeader, prePrepareMsg(seq, windowProposal(seq, 0)))     // the leader
				v.processMsg(windowLeader, prePrepareMsg(seq, windowProposal(seq+100, 0))) // a second one
				slot := v.slots[seq]
				require.NotNil(t, slot)
				require.Equal(t, slotEmpty, slot.phase)
				require.Len(t, slot.prepares.votes, 1)
				require.Len(t, slot.commits.votes, 1)
				require.True(t, proto.Equal(prePrepareMsg(seq, windowProposal(seq, 0)), slot.prePrepare), "the first pre-prepare wins")
			}
			require.Equal(t, len(accepted), f.logs.count("got pre-prepare from 3 but the leader is 1"))
			require.Equal(t, 1, f.logs.count("Got a pre-prepare for current sequence without processing previous one, dropping message"))
			require.Equal(t, len(accepted)-1, f.logs.count("Got a pre-prepare for next sequence without processing previous one, dropping message"))

			// Above the accepted window: no slot.
			tooNew := low + depth + 1
			v.processMsg(3, prepareMsg(tooNew, "d", false))
			v.processMsg(3, commitMsg(tooNew, "d", 3, false))
			v.processMsg(windowLeader, prePrepareMsg(tooNew, windowProposal(tooNew, 0)))
			require.NotContains(t, v.slots, tooNew)
			require.Equal(t, 2, f.logs.count(fmt.Sprintf("2 got message from 3 with sequence %d but our sequence is %d", tooNew, low)))
			require.Equal(t, 1, f.logs.count(fmt.Sprintf("2 got message from 1 with sequence %d but our sequence is %d", tooNew, low)))

			require.Len(t, v.slots, int(depth)+len(accepted))
		})
	}
}

// TestViewInOrderDeliver covers the in-order gate (I3, I4, I6): a follower with depth 3 completes the commit
// quorum of low+1 before that of low; low+1 is committed but not delivered, and once low is committed both are
// delivered in order, each with Quorum signatures, ProposalSequence advancing by one per delivery.
func TestViewInOrderDeliver(t *testing.T) {
	f := newWindowView(2, 3)
	v := f.view
	v.Start()
	defer v.Abort()

	low := windowLow
	first := windowProposal(low, windowDecisionsFor(low))
	second := windowProposal(low+1, windowDecisionsFor(low+1))
	v.HandleMessage(windowLeader, prePrepareMsg(low, first))
	v.HandleMessage(windowLeader, prePrepareMsg(low+1, second))
	for _, seq := range []uint64{low, low + 1} {
		prepare := f.comm.nextBroadcast(t, "a prepare")
		require.Equal(t, seq, prepare.GetPrepare().GetSeq())
	}
	require.InDelta(t, 2.0, f.metrics.gauge("view_in_flight_proposals"), 0)

	// The second sequence is committed first, and waits.
	f.prepareAndCommit(t, low+1, second.Digest(), 3, 4)
	f.logs.waitFor(t, fmt.Sprintf("2 processed commits for proposal with seq %d", low+1))
	select {
	case d := <-f.decider.decisions:
		t.Fatalf("sequence %d was delivered before sequence %d", proposalSequenceOf(d.proposal), low)
	default:
	}
	require.Equal(t, ViewSequence{ViewActive: true, ProposalSeq: low}, viewSequenceOf(v))

	// The first sequence is committed: both are delivered, in order.
	f.prepareAndCommit(t, low, first.Digest(), 3, 4)
	for _, expected := range []types.Proposal{first, second} {
		decision := f.decider.next(t, "a decision")
		require.Equal(t, expected, decision.proposal)
		require.Len(t, decision.signatures, windowQuorum)
		require.Equal(t, windowRequests, decision.requests)
	}
	require.Equal(t, ViewSequence{ViewActive: true, ProposalSeq: low + 2}, viewSequenceOf(v))
	require.Equal(t, 1, f.logs.count(fmt.Sprintf("Sequence: %d-->%d", low, low+1)))
	require.Equal(t, 1, f.logs.count(fmt.Sprintf("Sequence: %d-->%d", low+1, low+2)))
	require.InDelta(t, 0.0, f.metrics.gauge("view_in_flight_proposals"), 0)
	require.Equal(t, uint64(0), v.inFlightCount())
}

// TestViewFollowerPreparePrecondition covers the prefix rule (D2, I12): a follower that holds the pre-prepare
// of low+1 but not of low sends no prepare; when low arrives both are processed in order, prepares and WAL
// records included.
func TestViewFollowerPreparePrecondition(t *testing.T) {
	f := newWindowView(2, 3)
	v := f.view
	v.Start()
	defer v.Abort()

	low := windowLow
	second := windowProposal(low+1, windowDecisionsFor(low+1))
	v.HandleMessage(windowLeader, prePrepareMsg(low+1, second))
	// A message above the window is logged once it is processed, after the pre-prepare above: a probe.
	probe := low + 3 + 1
	v.HandleMessage(3, prepareMsg(probe, "d", false))
	f.logs.waitFor(t, fmt.Sprintf("2 got message from 3 with sequence %d but our sequence is %d", probe, low))
	require.Zero(t, f.comm.broadcastCount(), "no prepare may be sent before the pre-prepare of sequence %d", low)
	require.Empty(t, f.state.savedSequences())

	first := windowProposal(low, windowDecisionsFor(low))
	v.HandleMessage(windowLeader, prePrepareMsg(low, first))
	for _, expected := range []types.Proposal{first, second} {
		prepare := f.comm.nextBroadcast(t, "a prepare")
		require.NotNil(t, prepare.GetPrepare())
		require.Equal(t, expected.Digest(), prepare.GetPrepare().GetDigest())
	}
	require.Equal(t, []uint64{low, low + 1}, f.state.savedSequences())
}

// TestViewLeaderResendsPrePrepares covers the pre-prepare resend (section 5): a leader with depth 2 broadcasts
// its open pre-prepares again on every tick, a committed sequence excluded, and a leader with depth 1 has no
// tick at all and never sends anything twice.
func TestViewLeaderResendsPrePrepares(t *testing.T) {
	t.Run("depth 2 resends open sequences", func(t *testing.T) {
		f := newWindowView(windowLeader, 2)
		v := f.view
		tick := make(chan time.Time)
		v.resendTick = tick
		v.PrePrepareResendInterval = time.Hour // the injected channel is used instead of a ticker
		v.Start()
		defer v.Abort()

		low := windowLow
		first := windowProposal(low, windowDecisionsFor(low))
		second := windowProposal(low+1, windowDecisionsFor(low+1))
		v.Propose(first)
		v.Propose(second)
		var originals []*protos.Message
		for _, seq := range []uint64{low, low + 1} {
			pp := f.comm.nextBroadcast(t, "a pre-prepare")
			require.Equal(t, seq, pp.GetPrePrepare().GetSeq())
			originals = append(originals, pp)
			prepare := f.comm.nextBroadcast(t, "a prepare")
			require.Equal(t, seq, prepare.GetPrepare().GetSeq())
		}
		require.Equal(t, uint64(2), v.inFlightCount())

		tick <- time.Now()
		for _, original := range originals {
			resent := f.comm.nextBroadcast(t, "a resent pre-prepare")
			require.True(t, proto.Equal(original, resent), "expected %s, got %s", MsgToString(original), MsgToString(resent))
		}

		// The first sequence is committed and delivered: only the second one is resent.
		f.prepareAndCommit(t, low, first.Digest(), 2, 3)
		f.decider.next(t, "the first decision")
		tick <- time.Now()
		resent := f.comm.nextBroadcast(t, "the resent second pre-prepare")
		require.True(t, proto.Equal(originals[1], resent))
		tick <- time.Now()
		resent = f.comm.nextBroadcast(t, "the resent second pre-prepare, again")
		require.True(t, proto.Equal(originals[1], resent))
		// 2 pre-prepares + 2 prepares, 2 resent, the commit of the first, the second resent twice.
		require.Equal(t, 9, f.comm.broadcastCount())
	})

	t.Run("depth 2 does not resend prepared sequences", func(t *testing.T) {
		f := newWindowView(windowLeader, 2)
		v := f.view
		tick := make(chan time.Time)
		v.resendTick = tick
		v.PrePrepareResendInterval = time.Hour
		v.Start()
		defer v.Abort()

		low := windowLow
		first := windowProposal(low, windowDecisionsFor(low))
		second := windowProposal(low+1, windowDecisionsFor(low+1))
		v.Propose(first)
		v.Propose(second)
		var originals []*protos.Message
		for range []uint64{low, low + 1} {
			originals = append(originals, f.comm.nextBroadcast(t, "a pre-prepare"))
			f.comm.nextBroadcast(t, "a prepare")
		}

		// A prepare quorum for the first sequence: the leader commits it, and it is not resent any more,
		// since every node of the quorum holds its pre-prepare; the second, still open, is.
		v.HandleMessage(2, prepareMsg(low, first.Digest(), false))
		v.HandleMessage(3, prepareMsg(low, first.Digest(), false))
		commit := f.comm.nextBroadcast(t, "the commit of the first sequence")
		require.NotNil(t, commit.GetCommit())
		tick <- time.Now()
		resent := f.comm.nextBroadcast(t, "the resent second pre-prepare")
		require.True(t, proto.Equal(originals[1], resent), "expected %s, got %s", MsgToString(originals[1]), MsgToString(resent))
		tick <- time.Now()
		resent = f.comm.nextBroadcast(t, "the resent second pre-prepare, again")
		require.True(t, proto.Equal(originals[1], resent))
		// 2 pre-prepares + 2 prepares, the commit of the first, the second resent twice.
		require.Equal(t, 7, f.comm.broadcastCount())
	})

	t.Run("depth 2 creates a ticker from the interval", func(t *testing.T) {
		f := newWindowView(windowLeader, 2)
		f.view.PrePrepareResendInterval = time.Hour
		f.view.Start()
		require.NotNil(t, f.view.resendTick)
		require.NotNil(t, f.view.resendTicker)
		f.view.Abort()
	})

	t.Run("followers get no ticker", func(t *testing.T) {
		f := newWindowView(2, 2)
		f.view.PrePrepareResendInterval = time.Hour
		f.view.Start()
		require.Nil(t, f.view.resendTick)
		f.view.Abort()
	})

	t.Run("depth 1 never resends", func(t *testing.T) {
		f := newWindowView(windowLeader, 1)
		v := f.view
		v.PrePrepareResendInterval = time.Millisecond
		v.Start()
		defer v.Abort()
		require.Nil(t, v.resendTick, "a sequential view has no resend tick")

		low := windowLow
		v.Propose(windowProposal(low, windowDecisionsFor(low)))
		require.NotNil(t, f.comm.nextBroadcast(t, "the pre-prepare").GetPrePrepare())
		require.NotNil(t, f.comm.nextBroadcast(t, "the prepare").GetPrepare())
		time.Sleep(20 * time.Millisecond)
		require.Equal(t, 2, f.comm.broadcastCount())
	})
}

// TestViewDuplicatePrePrepareLogLevel covers the log level of a pre-prepare that arrives again (section 2.2):
// with depth above one the same message is debug-logged and dropped, a different one is still warned about;
// with depth 1 even the same message gets the warning the sequential view always logged.
func TestViewDuplicatePrePrepareLogLevel(t *testing.T) {
	const dropped = "Got a pre-prepare for current sequence without processing previous one, dropping message"
	low := windowLow
	proposal := windowProposal(low, windowDecisionsFor(low))

	t.Run("depth 2", func(t *testing.T) {
		f := newWindowView(2, 2)
		v := f.view
		v.Start()
		defer v.Abort()

		v.HandleMessage(windowLeader, prePrepareMsg(low, proposal))
		f.comm.nextBroadcast(t, "the prepare")
		v.HandleMessage(windowLeader, prePrepareMsg(low, proposal))
		f.logs.waitFor(t, fmt.Sprintf("Got the pre-prepare for sequence %d again, ignoring it", low))
		require.Equal(t, zapcore.DebugLevel, f.logs.level("Got the pre-prepare for sequence"))
		require.Zero(t, f.logs.count(dropped))

		other := proposal
		other.Payload = []byte{99}
		v.HandleMessage(windowLeader, prePrepareMsg(low, other))
		f.logs.waitFor(t, dropped)
		require.Equal(t, zapcore.WarnLevel, f.logs.level(dropped))
	})

	t.Run("depth 1", func(t *testing.T) {
		f := newWindowView(2, 1)
		v := f.view
		v.Start()
		defer v.Abort()

		v.HandleMessage(windowLeader, prePrepareMsg(low, proposal))
		f.comm.nextBroadcast(t, "the prepare")
		v.HandleMessage(windowLeader, prePrepareMsg(low, proposal))
		f.logs.waitFor(t, dropped)
		require.Equal(t, zapcore.WarnLevel, f.logs.level(dropped))
		require.Zero(t, f.logs.count("again, ignoring it"))
	})
}

// TestViewPrevSeqPrePrepareLogLevel covers the log level of a pre-prepare for a delivered sequence (section 5):
// with depth above one it is the leader's periodic resend reaching a node that delivered the sequence
// meanwhile, so it is debug-logged; with depth 1 it keeps the warning of the sequential view.
func TestViewPrevSeqPrePrepareLogLevel(t *testing.T) {
	const prev = "Got pre-prepare for sequence"
	low := windowLow
	stale := windowProposal(low-1, windowDecisionsFor(low)-1)

	for _, tc := range []struct {
		depth uint64
		level zapcore.Level
	}{{1, zapcore.WarnLevel}, {2, zapcore.DebugLevel}, {4, zapcore.DebugLevel}} {
		t.Run(fmt.Sprintf("depth %d", tc.depth), func(t *testing.T) {
			f := newWindowView(2, tc.depth)
			v := f.view
			v.Start()
			defer v.Abort()

			v.HandleMessage(windowLeader, prePrepareMsg(low-1, stale))
			f.logs.waitFor(t, fmt.Sprintf("%s %d but we're in sequence %d", prev, low-1, low))
			require.Equal(t, tc.level, f.logs.level(prev))
			require.Zero(t, f.comm.broadcastCount(), "a stale pre-prepare is dropped without a reply")
		})
	}
}

// TestViewGetMetadataPredictsDecisions covers I5: the leader's metadata for the sequence it proposes next
// carries expectedDecisions(head), and a follower verifies a proposal of sequence k against
// expectedDecisions(k), the same formula, so the two agree for every sequence of the window.
func TestViewGetMetadataPredictsDecisions(t *testing.T) {
	leader := newWindowView(windowLeader, 3)
	leader.view.ProposalSequence = 5
	leader.view.DecisionsInView = 2
	leader.view.head = 7
	md := &protos.ViewMetadata{}
	require.NoError(t, proto.Unmarshal(leader.view.GetMetadata(), md))
	require.Equal(t, uint64(7), md.GetLatestSequence())
	require.Equal(t, uint64(4), md.GetDecisionsInView())
	require.Equal(t, windowView, md.GetViewId())
	require.Equal(t, 1, leader.logs.count("GetMetadata with view 1, seq 7, dec 4"))

	follower := newWindowView(2, 3)
	follower.view.ProposalSequence = 5
	follower.view.DecisionsInView = 2
	requests, err := follower.view.verifyProposal(7, windowProposal(7, 4), nil)
	require.NoError(t, err)
	require.Equal(t, windowRequests, requests)

	_, err = follower.view.verifyProposal(7, windowProposal(7, 3), nil)
	require.EqualError(t, err, "invalid decisions in view")
	require.Equal(t, 1, follower.logs.count("Expected decisions in view 4 but got 3"))

	_, err = follower.view.verifyProposal(6, windowProposal(7, 3), nil)
	require.EqualError(t, err, "invalid proposal sequence")
	require.Equal(t, 1, follower.logs.count("Expected proposal sequence 6 but got 7"))
}

// TestInFlightSequenceInWAL covers the check a node with a pipeline depth above one makes before starting:
// a WAL that ends with a pre-prepare, or with a commit after its pre-prepare, of a sequence above the last
// decided one holds a proposal in flight.
func TestInFlightSequenceInWAL(t *testing.T) {
	proposed := func(seq uint64) []byte {
		return MarshalOrPanic(&protos.SavedMessage{Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: &protos.PrePrepare{View: 1, Seq: seq, Proposal: &protos.Proposal{}},
				Prepare:    &protos.Prepare{View: 1, Seq: seq},
			},
		}})
	}
	commit := func(seq uint64) []byte {
		return MarshalOrPanic(&protos.SavedMessage{Content: &protos.SavedMessage_Commit{
			Commit: commitMsg(seq, "d", 1, false),
		}})
	}
	newView := MarshalOrPanic(&protos.SavedMessage{Content: &protos.SavedMessage_NewView{NewView: &protos.ViewMetadata{ViewId: 2}}})

	for _, tc := range []struct {
		description string
		entries     [][]byte
		lastDecided uint64
		seq         uint64
		inFlight    bool
	}{
		{description: "empty"},
		{description: "garbage", entries: [][]byte{{1, 2, 3}}},
		{description: "new view", entries: [][]byte{proposed(5), newView}, lastDecided: 4},
		{description: "pre-prepare in flight", entries: [][]byte{proposed(5)}, lastDecided: 4, seq: 5, inFlight: true},
		{description: "pre-prepare already decided", entries: [][]byte{proposed(5)}, lastDecided: 5},
		{description: "prepared in flight", entries: [][]byte{proposed(5), commit(5)}, lastDecided: 4, seq: 5, inFlight: true},
		{description: "prepared and decided", entries: [][]byte{proposed(5), commit(5)}, lastDecided: 5},
		{description: "commit without pre-prepare", entries: [][]byte{commit(5)}, lastDecided: 4},
	} {
		t.Run(tc.description, func(t *testing.T) {
			seq, inFlight := InFlightSequenceInWAL(tc.entries, tc.lastDecided)
			require.Equal(t, tc.inFlight, inFlight)
			require.Equal(t, tc.seq, seq)
		})
	}
}
