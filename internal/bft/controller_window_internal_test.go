// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"fmt"
	"sync"
	"testing"

	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// These tests cover the leader token semaphore of the controller (design-pipeline-window.md section 6,
// rules R1-R7): how many tokens a view issues, returns and drains, and what the proposer does with a token
// of a view that stopped. They live in package bft to reach startView, decide, propose and the channel.

// fakeView is a Proposer that records what it is asked to propose.
type fakeView struct {
	leader    uint64
	restored  uint64
	abortChan chan struct{}
	stopOnce  sync.Once
	mu        sync.Mutex
	proposed  []types.Proposal
	metadata  int
}

func newFakeView(leader uint64) *fakeView {
	return &fakeView{leader: leader, abortChan: make(chan struct{})}
}

func (v *fakeView) Propose(proposal types.Proposal) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.proposed = append(v.proposed, proposal)
}

func (v *fakeView) Start() {}

func (v *fakeView) RestoredCount() uint64 { return v.restored }

func (v *fakeView) Abort() { v.stop() }

func (v *fakeView) stop() { v.stopOnce.Do(func() { close(v.abortChan) }) }

func (v *fakeView) Stopped() bool {
	select {
	case <-v.abortChan:
		return true
	default:
		return false
	}
}

func (v *fakeView) AbortChan() <-chan struct{} { return v.abortChan }

func (v *fakeView) GetLeaderID() uint64 { return v.leader }

func (v *fakeView) GetMetadata() []byte {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.metadata++
	return MarshalOrPanic(&protos.ViewMetadata{ViewId: 1, LatestSequence: 1})
}

func (v *fakeView) HandleMessage(uint64, *protos.Message) {}

func (v *fakeView) proposals() int {
	v.mu.Lock()
	defer v.mu.Unlock()
	return len(v.proposed)
}

// fakeProposerBuilder builds fake views led by the requested leader, reporting the configured phase.
type fakeProposerBuilder struct {
	phase    Phase
	restored uint64 // what the built views report as restored; 0 means "one if the phase is PROPOSED/PREPARED"
	built    []*fakeView
}

func (b *fakeProposerBuilder) NewProposer(leader, _, _, _ uint64, _ int) (Proposer, Phase) {
	view := newFakeView(leader)
	view.restored = b.restored
	if b.restored == 0 && (b.phase == PROPOSED || b.phase == PREPARED) {
		view.restored = 1
	}
	b.built = append(b.built, view)
	return view, b.phase
}

// fakePool counts reservations.
type fakePool struct {
	mu           sync.Mutex
	reserved     [][]types.RequestInfo
	unreservedAt int
}

func (p *fakePool) Prune(func([]byte) error) {}
func (p *fakePool) Submit([]byte) error      { return nil }
func (p *fakePool) Size() int                { return 0 }
func (p *fakePool) NextRequests(int, uint64, bool) ([][]byte, bool) {
	return nil, false
}

func (p *fakePool) Reserve(requests []types.RequestInfo) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reserved = append(p.reserved, requests)
}

func (p *fakePool) UnreserveAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.unreservedAt++
}

func (p *fakePool) RemoveRequest(types.RequestInfo) error { return nil }
func (p *fakePool) StopTimers()                           {}
func (p *fakePool) RestartTimers()                        {}
func (p *fakePool) Close()                                {}

// fakeBatcher returns the configured batches, running a hook before each.
type fakeBatcher struct {
	batches [][][]byte
	onNext  func()
	calls   int
	closed  bool
}

func (b *fakeBatcher) NextBatch() [][]byte {
	if b.onNext != nil {
		b.onNext()
	}
	b.calls++
	if len(b.batches) == 0 {
		return nil
	}
	batch := b.batches[0]
	b.batches = b.batches[1:]
	return batch
}

func (b *fakeBatcher) Close()                     { b.closed = true }
func (b *fakeBatcher) Closed() bool               { return b.closed }
func (b *fakeBatcher) Reset()                     { b.closed = false }
func (b *fakeBatcher) SetBatchParams(BatchParams) {}
func (b *fakeBatcher) BatchParams() BatchParams   { return BatchParams{} }

type fakeLeaderMonitor struct{}

func (fakeLeaderMonitor) ChangeRole(Role, uint64, uint64)                   {}
func (fakeLeaderMonitor) ProcessMsg(uint64, *protos.Message)                {}
func (fakeLeaderMonitor) InjectArtificialHeartbeat(uint64, *protos.Message) {}
func (fakeLeaderMonitor) HeartbeatWasSent()                                 {}
func (fakeLeaderMonitor) Close()                                            {}
func (fakeLeaderMonitor) StopLeaderSendMsg()                                {}

type rawRequestInspector struct{}

func (rawRequestInspector) RequestID(req []byte) types.RequestInfo {
	return types.RequestInfo{ClientID: "c", ID: string(req)}
}

type fakeAssembler struct{}

func (fakeAssembler) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	return types.Proposal{Metadata: metadata, Payload: requests[0]}
}

// newWindowController returns a controller of a 4-node cluster, with its leader token channel and current
// view set up as Start would, but without its goroutines, so that the tests drive it step by step.
func newWindowController(id, depth uint64, builder *fakeProposerBuilder) (*Controller, *fakePool) {
	metadata := MarshalOrPanic(&protos.ViewMetadata{})
	checkpoint := &types.Checkpoint{}
	checkpoint.Set(types.Proposal{Metadata: metadata}, nil)
	pool := &fakePool{}
	c := &Controller{
		ID:               id,
		N:                4,
		NodesList:        []uint64{1, 2, 3, 4},
		PipelineDepth:    depth,
		Logger:           zap.NewNop().Sugar(),
		Deliver:          applicationFunc(func(types.Proposal, []types.Signature) types.Reconfig { return types.Reconfig{} }),
		Verifier:         noopVerifier{},
		Checkpoint:       checkpoint,
		InFlight:         &InFlightData{},
		RequestPool:      pool,
		RequestInspector: rawRequestInspector{},
		Assembler:        fakeAssembler{},
		Batcher:          &fakeBatcher{},
		LeaderMonitor:    fakeLeaderMonitor{},
		ProposerBuilder:  builder,
		stopChan:         make(chan struct{}),
		leaderToken:      make(chan Proposer, depth),
		quorum:           3,
	}
	return c, pool
}

// TestControllerTokenSemaphore covers invariant T: a view started in phase COMMITTED issues W tokens, one
// started with a restored proposal W-1, a delivered decision returns one (never above W), and aborting the
// view or stopping drains them all. With W=1 this is one token, or none after a restore, as before.
func TestControllerTokenSemaphore(t *testing.T) {
	for _, depth := range []uint64{1, 3} {
		t.Run("depth "+string(rune('0'+depth)), func(t *testing.T) {
			builder := &fakeProposerBuilder{phase: COMMITTED}
			c, pool := newWindowController(1, depth, builder) // node 1 leads view 0
			c.currViewNumber = 0

			c.startView(1)
			require.Len(t, c.leaderToken, int(depth), "a view with nothing in flight issues W tokens")
			require.Equal(t, 1, pool.unreservedAt, "a view that starts releases the reservations of the previous one")
			for range depth {
				require.Same(t, builder.built[0], <-c.leaderToken, "a token carries the view it was issued for")
			}

			// A restored proposal takes one slot of the pipeline.
			builder.phase = PREPARED
			c.startView(1)
			require.Len(t, c.leaderToken, int(depth-1))
			c.drainLeaderTokens()
			builder.phase = PROPOSED
			c.startView(1)
			require.Len(t, c.leaderToken, int(depth-1))

			// A decision in the current view returns one token, up to the depth.
			c.decide(newDecision(t, c.currView))
			require.Len(t, c.leaderToken, int(depth))
			c.decide(newDecision(t, c.currView))
			require.Len(t, c.leaderToken, int(depth), "the channel never holds more than W tokens")

			// Aborting the view drains the tokens; so does stopping.
			require.True(t, c.abortView(0))
			require.Empty(t, c.leaderToken)
			require.True(t, c.currView.Stopped())
			c.acquireLeaderTokens(c.currView, depth)
			c.drainLeaderTokens()
			require.Empty(t, c.leaderToken)

			// A follower issues nothing.
			follower, _ := newWindowController(2, depth, &fakeProposerBuilder{phase: COMMITTED})
			follower.startView(1)
			require.Empty(t, follower.leaderToken)
		})
	}
}

// TestControllerDecideRotationNoDoubleToken covers R4: when a decision rotates the leader, the view that
// starts issues the new leader's tokens and the decision itself returns none. Observable when the rotated
// view restores a proposal: it issues W-1 tokens and the decision must not top them up to W.
func TestControllerDecideRotationNoDoubleToken(t *testing.T) {
	const depth = 3
	builder := &fakeProposerBuilder{phase: COMMITTED}
	c, _ := newWindowController(2, depth, builder) // with rotation, node 1 leads the first decision of view 0, node 2 the second
	c.LeaderRotation = true
	c.DecisionsPerLeader = 1
	c.currViewNumber = 0
	c.startView(1)
	require.Empty(t, c.leaderToken, "node 2 does not lead yet")
	before := c.currView

	builder.phase = PREPARED // the rotated view restores a proposal, so it issues W-1 tokens
	c.decide(newDecision(t, before))

	require.True(t, before.Stopped(), "the decision rotated the leader, so the previous view was aborted")
	require.NotSame(t, before, c.currView)
	require.Equal(t, uint64(1), c.getCurrentDecisionsInView())
	require.Len(t, c.leaderToken, depth-1, "the rotated view issued its tokens; the decision must not add one")
}

// TestProposerLoopDiscardsTokenOnStoppedView covers R7 and the reservation: a token of a view that stopped
// (before or while the batch was built) leads to no proposal, no reservation and no returned token, while a
// token of a running view reserves the batch and proposes it; an empty batch returns the token to its view.
func TestProposerLoopDiscardsTokenOnStoppedView(t *testing.T) {
	req := []byte("r1")

	t.Run("view stopped before the batch", func(t *testing.T) {
		c, pool := newWindowController(1, 2, &fakeProposerBuilder{phase: COMMITTED})
		batcher := &fakeBatcher{batches: [][][]byte{{req}}}
		c.Batcher = batcher
		view := newFakeView(1)
		view.stop()
		c.propose(view)
		require.Zero(t, batcher.calls, "no batch is built for a view that stopped")
		require.Zero(t, view.proposals())
		require.Empty(t, pool.reserved)
		require.Empty(t, c.leaderToken)
	})

	t.Run("view stopped while the batch was built", func(t *testing.T) {
		c, pool := newWindowController(1, 2, &fakeProposerBuilder{phase: COMMITTED})
		view := newFakeView(1)
		batcher := &fakeBatcher{batches: [][][]byte{{req}}, onNext: view.stop}
		c.Batcher = batcher
		c.propose(view)
		require.Equal(t, 1, batcher.calls)
		require.Zero(t, view.proposals(), "the batch is not proposed to a view that stopped")
		require.Empty(t, pool.reserved, "the requests stay available for the next view")
		require.Empty(t, c.leaderToken, "the token belongs to the stopped view and is discarded")
	})

	t.Run("running view", func(t *testing.T) {
		c, pool := newWindowController(1, 2, &fakeProposerBuilder{phase: COMMITTED})
		c.Batcher = &fakeBatcher{batches: [][][]byte{{req}}}
		view := newFakeView(1)
		c.propose(view)
		require.Equal(t, 1, view.proposals())
		require.Equal(t, [][]types.RequestInfo{{{ClientID: "c", ID: "r1"}}}, pool.reserved)
		require.Empty(t, c.leaderToken, "the token is consumed by the proposal")
	})

	t.Run("empty batch returns the token", func(t *testing.T) {
		c, pool := newWindowController(1, 2, &fakeProposerBuilder{phase: COMMITTED})
		c.Batcher = &fakeBatcher{}
		view := newFakeView(1)
		c.propose(view)
		require.Zero(t, view.proposals())
		require.Empty(t, pool.reserved)
		require.Len(t, c.leaderToken, 1)
		require.Same(t, view, <-c.leaderToken)
	})

	t.Run("closed batcher discards the token", func(t *testing.T) {
		c, _ := newWindowController(1, 2, &fakeProposerBuilder{phase: COMMITTED})
		c.Batcher = &fakeBatcher{batches: [][][]byte{{req}}, closed: true}
		view := newFakeView(1)
		c.propose(view)
		require.Zero(t, view.proposals())
		require.Empty(t, c.leaderToken)
	})
}

// TestProposerLoopProposesForEveryToken runs the proposer loop itself: W tokens for a view lead to W
// proposals, in the order the batches come, and the loop exits when the controller stops.
func TestProposerLoopProposesForEveryToken(t *testing.T) {
	const depth = 3
	c, pool := newWindowController(1, depth, &fakeProposerBuilder{phase: COMMITTED})
	batches := make(chan [][]byte, depth)
	for i := range depth {
		batches <- [][]byte{{byte('a' + i)}}
	}
	proposed := make(chan struct{}, depth)
	view := newFakeView(1)
	c.Batcher = &chanBatcher{batches: batches}
	c.Assembler = assemblerFunc(func(metadata []byte, requests [][]byte) types.Proposal {
		proposed <- struct{}{}
		return types.Proposal{Metadata: metadata, Payload: requests[0]}
	})
	c.acquireLeaderTokens(view, depth)

	done := make(chan struct{})
	go func() {
		c.proposerLoop()
		close(done)
	}()
	for range depth {
		<-proposed
	}
	close(c.stopChan)
	<-done

	require.Equal(t, depth, view.proposals())
	require.Len(t, pool.reserved, depth)
	for i, proposal := range view.proposed {
		require.Equal(t, []byte{byte('a' + i)}, proposal.Payload)
		md := &protos.ViewMetadata{}
		require.NoError(t, proto.Unmarshal(proposal.Metadata, md))
	}
	require.Empty(t, c.leaderToken)
}

// chanBatcher returns the batches it receives on a channel, blocking like the real batcher.
type chanBatcher struct {
	batches chan [][]byte
}

func (b *chanBatcher) NextBatch() [][]byte {
	select {
	case batch := <-b.batches:
		return batch
	default:
		return nil
	}
}

func (b *chanBatcher) Close()                     {}
func (b *chanBatcher) Closed() bool               { return false }
func (b *chanBatcher) Reset()                     {}
func (b *chanBatcher) SetBatchParams(BatchParams) {}
func (b *chanBatcher) BatchParams() BatchParams   { return BatchParams{} }

type assemblerFunc func(metadata []byte, requests [][]byte) types.Proposal

func (f assemblerFunc) AssembleProposal(metadata []byte, requests [][]byte) types.Proposal {
	return f(metadata, requests)
}

// TestControllerTokensAfterRestore covers R1 with several restored proposals: a view that starts with m
// proposals in flight issues W-m tokens, so that len(tokens) + in flight + restored == W, and none when the
// restore fills the pipeline.
func TestControllerTokensAfterRestore(t *testing.T) {
	for _, tc := range []struct{ depth, restored, tokens uint64 }{{3, 2, 1}, {3, 3, 0}, {4, 1, 3}, {1, 1, 0}} {
		t.Run(fmt.Sprintf("depth %d restored %d", tc.depth, tc.restored), func(t *testing.T) {
			builder := &fakeProposerBuilder{phase: PREPARED, restored: tc.restored}
			c, _ := newWindowController(1, tc.depth, builder)
			c.currViewNumber = 0
			c.startView(1)
			require.Len(t, c.leaderToken, int(tc.tokens))
			c.drainLeaderTokens()
		})
	}
}
