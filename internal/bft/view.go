// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

// Phase indicates the status of the view
type Phase uint8

// These are the different phases
const (
	COMMITTED = iota
	PROPOSED
	PREPARED
	ABORT
)

func (p Phase) String() string {
	switch p {
	case COMMITTED:
		return "COMMITTED"
	case PROPOSED:
		return "PROPOSED"
	case PREPARED:
		return "PREPARED"
	case ABORT:
		return "ABORT"
	default:
		return "Invalid Phase"
	}
}

// State can save and restore the state
type State interface {
	// Save saves a message. truncateTo tells the WAL whether everything written
	// before the message can be dropped. The caller decides it; Save does not
	// infer the flag from the message type. A pre-prepare is normally a truncation
	// point, while a commit never is: its pre-prepare must survive next to it.
	// View records are not truncation points in this change.
	Save(message *protos.SavedMessage, truncateTo bool) error

	// Restore restores the given view to its latest state
	// before a crash, if applicable.
	Restore(*View) error
}

// Comm adds broadcast to the regular comm interface
type Comm interface {
	api.Comm
	BroadcastConsensus(m *protos.Message)
}

type CheckpointRetriever func() (*protos.Proposal, []*protos.Signature)

// View is responsible for running the view protocol
type View struct {
	// Configuration
	DecisionsPerLeader uint64
	RetrieveCheckpoint CheckpointRetriever
	SelfID             uint64
	N                  uint64
	NodesList          []uint64
	LeaderID           uint64
	Quorum             int
	Number             uint64
	Decider            Decider
	FailureDetector    FailureDetector
	Sync               Synchronizer
	Logger             api.Logger
	Comm               Comm
	Verifier           api.Verifier
	Signer             api.Signer
	MembershipNotifier api.MembershipNotifier
	// ProposalSequence is the lowest sequence this view has not delivered yet (the low watermark).
	// It advances by one at every delivery, together with DecisionsInView, which is the value a
	// proposal with sequence ProposalSequence must carry; a proposal with sequence k must carry
	// expectedDecisions(k) = DecisionsInView + (k - ProposalSequence).
	ProposalSequence uint64
	DecisionsInView  uint64
	State            State
	// Phase is the phase of the lowest undelivered sequence. Before Start it is the hand-off of a restore
	// from the WAL (PROPOSED/PREPARED, with the four in-flight fields below) that Start turns into a slot.
	Phase      Phase
	InMsgQSize int
	// PipelineDepth is the number of proposals the leader may have in flight, W. 0 is treated as 1,
	// the original sequential behaviour: one proposal in flight, the next proposed after delivery.
	PipelineDepth uint64
	// PrePrepareResendInterval is how often a leader with a pipeline depth above one broadcasts again the
	// pre-prepares of the sequences it has not seen committed, so that a follower that missed one can prepare
	// the sequences above it (the prefix rule). 0 disables it. With a depth of one nothing is ever resent:
	// the sequential protocol sends every message once and recovers through sync.
	PrePrepareResendInterval time.Duration
	// Runtime
	lastVotedProposalByID map[uint64]*protos.Commit
	incMsgs               chan *incMsg
	// Restore hand-off (state.go, viewchanger.go): the proposal in flight when the view is created
	// in phase PROPOSED or PREPARED. Start moves them into the slot of ProposalSequence.
	myProposalSig     *types.Signature
	inFlightProposal  *types.Proposal
	inFlightRequests  []types.RequestInfo
	lastBroadcastSent *protos.Message
	// restored is the hand-off of a restore from the WAL with a pipeline: the proposals in flight, lowest
	// first (state.go). Start moves them into their slots; restoredCount keeps how many for the controller.
	restored      []*restoredSlot
	restoredCount uint64
	// seqLock guards ProposalSequence, DecisionsInView and head between the view goroutine, which
	// writes them, and the leader's proposer goroutine, which reads them in GetMetadata and Propose.
	seqLock sync.Mutex
	// head is the next sequence the leader proposes; ProposalSequence <= head <= ProposalSequence + W.
	// A follower keeps head == ProposalSequence.
	head uint64
	// slots holds one state machine per sequence in [ProposalSequence-W, ProposalSequence+W]: the
	// delivered sequences below the low watermark keep only the prepare and commit sent for them, to
	// answer lagging nodes; [ProposalSequence, ProposalSequence+W) are processed; ProposalSequence+W
	// is only accepted (its votes and pre-prepare kept), as the "next" sequence was before.
	slots map[uint64]*seqState
	// verifiedSigs receives the commit signatures the verification goroutines verified, tagged by sequence.
	verifiedSigs chan verifiedSig
	// resendTick fires when the leader should broadcast its open pre-prepares again. It is nil, and never
	// fires, unless the depth is above one and this node leads the view (tests inject a channel).
	resendTick   <-chan time.Time
	resendTicker *time.Ticker

	// prePrepareBegin records, per proposal sequence, when this node accepted the pre-prepare,
	// so that view_latency_batch_processing is measured for the sequence that is committed.
	// Only the view goroutine touches it after Start; it is seeded before Start by a restore
	// from the WAL or by the view changer committing an in-flight proposal.
	prePrepareBegin    map[uint64]time.Time
	MetricsBlacklist   *api.MetricsBlacklist
	MetricsView        *api.MetricsView
	blacklistSupported bool
	abortChan          chan struct{}
	stopOnce           sync.Once
	viewEnded          sync.WaitGroup

	ViewSequences *atomic.Value
}

// slotPhase is the phase of a single sequence in the view.
type slotPhase uint8

const (
	slotEmpty     slotPhase = iota // votes or a pre-prepare arrived, the pre-prepare is not processed yet
	slotProposed                   // pre-prepare verified and persisted, prepare sent (PROPOSED)
	slotPrepared                   // prepare quorum, commit signed, persisted and sent (PREPARED)
	slotCommitted                  // commit quorum collected, waiting to be delivered in order
	slotDelivered                  // delivered; only the prepare and commit sent are kept, for lagging nodes
)

// seqState is the state of one sequence: what the view kept once for "the current proposal",
// and once more for "the next proposal", before the view handled a window of sequences.
type seqState struct {
	seq   uint64
	phase slotPhase

	prePrepare   *protos.Message // the first pre-prepare accepted for this sequence; later ones are dropped
	proposal     *types.Proposal
	proposalHash string
	requests     []types.RequestInfo
	prepares     *voteSet
	commits      *voteSet
	sigCollector *voteVerifier
	signatures   []types.Signature // the verified commit signatures, up to Quorum-1
	mySig        *types.Signature
	voterIDs     []uint64 // who sent a prepare with the expected digest

	lastBroadcastSent *protos.Message // the prepare or commit to (re)broadcast when the phase is entered
	prepareSent       *protos.Message // the prepare sent, as an assist message for lagging nodes
	commitSent        *protos.Message // the commit sent, as an assist message for lagging nodes
	prepareBroadcast  bool            // the prepare was broadcast once since the slot became proposed
	commitBroadcast   bool            // the commit was broadcast once since the slot became prepared
}

// digest returns the digest of the proposal of the slot, computed once.
func (s *seqState) digest() string {
	if s.proposalHash == "" {
		s.proposalHash = s.proposal.Digest()
	}
	return s.proposalHash
}

// release frees what a delivered slot no longer needs, keeping only what lagging nodes may ask for.
func (s *seqState) release() {
	s.prePrepare = nil
	s.proposal = nil
	s.requests = nil
	s.prepares = nil
	s.commits = nil
	s.sigCollector = nil
	s.signatures = nil
	s.voterIDs = nil
	s.lastBroadcastSent = nil
}

// verifiedSig is a commit signature a verification goroutine verified, for the sequence it was cast on.
type verifiedSig struct {
	seq uint64
	sig types.Signature
}

// depth returns the pipeline depth W in effect: PipelineDepth, with 0 treated as 1.
func (v *View) depth() uint64 {
	if v.PipelineDepth == 0 {
		return 1
	}
	return v.PipelineDepth
}

// expectedDecisions returns the DecisionsInView a proposal with the given sequence must carry.
// The leader uses it for the sequence it proposes next and a follower for the sequence it verifies,
// and the two agree because ProposalSequence and DecisionsInView always advance together.
// Call with seq >= ProposalSequence.
func (v *View) expectedDecisions(seq uint64) uint64 {
	return v.DecisionsInView + (seq - v.ProposalSequence)
}

// Start starts the view
func (v *View) Start() {
	v.setup()

	go func() {
		v.run()
	}()
}

// setup initialises the runtime state of the view, before its goroutine runs.
func (v *View) setup() {
	v.stopOnce = sync.Once{}
	v.incMsgs = make(chan *incMsg, v.InMsgQSize)
	v.abortChan = make(chan struct{})
	v.lastVotedProposalByID = make(map[uint64]*protos.Commit)
	v.viewEnded.Add(1)

	v.slots = make(map[uint64]*seqState)
	// Every slot that is in flight at the same time casts at most N-1 verified commit signatures;
	// the sends also give up when the view is aborted, so the capacity is not load-bearing.
	v.verifiedSigs = make(chan verifiedSig, int(v.N)*(2*int(v.depth())+2))
	v.head = v.ProposalSequence

	switch {
	case len(v.restored) > 0:
		// The proposals in flight restored from the WAL, lowest first and contiguous from ProposalSequence.
		// A restored leader keeps their pre-prepares, to broadcast them again to the followers that missed
		// them (resendPrePrepares, depth above one only).
		for _, r := range v.restored {
			slot := v.slot(r.seq)
			slot.proposal = r.proposal
			slot.requests = r.requests
			slot.lastBroadcastSent = r.lastBroadcastSent()
			slot.mySig = r.mySig()
			slot.phase = slotProposed
			if r.commit != nil {
				slot.phase = slotPrepared
			}
			if v.SelfID == v.LeaderID {
				slot.prePrepare = r.prePrepare()
			}
		}
		v.head = v.restored[len(v.restored)-1].seq + 1
		v.restoredCount = uint64(len(v.restored))
		v.restored = nil
	case v.Phase == PROPOSED || v.Phase == PREPARED:
		// The in-flight proposal of a view change (viewchanger.go) is the proposal of the lowest sequence;
		// it has not been proposed to anyone by this view.
		slot := v.slot(v.ProposalSequence)
		slot.proposal = v.inFlightProposal
		slot.requests = v.inFlightRequests
		slot.lastBroadcastSent = v.lastBroadcastSent
		slot.mySig = v.myProposalSig
		slot.phase = slotProposed
		if v.Phase == PREPARED {
			slot.phase = slotPrepared
		}
		v.head = v.ProposalSequence + 1
		v.restoredCount = 1
	default:
		v.restoredCount = 0
	}

	if v.resendTick == nil && v.depth() > 1 && v.SelfID == v.LeaderID && v.PrePrepareResendInterval > 0 {
		v.resendTicker = time.NewTicker(v.PrePrepareResendInterval)
		v.resendTick = v.resendTicker.C
	}
}

// slot returns the state of the given sequence, creating it when it is the first message for that sequence:
// votes may arrive before the pre-prepare and have to be kept.
func (v *View) slot(seq uint64) *seqState {
	if slot, exists := v.slots[seq]; exists {
		return slot
	}
	slot := &seqState{
		seq: seq,
		prepares: &voteSet{
			validVote: func(_ uint64, message *protos.Message) bool {
				return message.GetPrepare() != nil
			},
		},
		commits: &voteSet{
			validVote: func(sender uint64, message *protos.Message) bool {
				commit := message.GetCommit()
				if commit == nil {
					return false
				}
				if commit.GetSignature() == nil {
					return false
				}
				// Sender needs to match the inner signature sender
				return commit.GetSignature().GetSigner() == sender
			},
		},
	}
	slot.prepares.clear(v.N)
	slot.commits.clear(v.N)
	v.slots[seq] = slot
	return slot
}

// HandleMessage handles incoming messages
func (v *View) HandleMessage(sender uint64, m *protos.Message) {
	msg := &incMsg{sender: sender, Message: m}
	select {
	case <-v.abortChan:
		return
	case v.incMsgs <- msg:
	}
}

func (v *View) processMsg(sender uint64, m *protos.Message) {
	if v.Stopped() {
		return
	}
	// Ensure view number is equal to our view
	msgViewNum := viewNumber(m)
	msgProposalSeq := proposalSequence(m)

	if msgViewNum != v.Number {
		v.Logger.Warnf("%d got message %v from %d of view %d, expected view %d", v.SelfID, m, sender, msgViewNum, v.Number)
		if sender != v.LeaderID {
			v.discoverIfSyncNeeded(sender, m)
			return
		}
		v.FailureDetector.Complain(v.Number, false)
		// Else, we got a message with a wrong view from the leader.
		if msgViewNum > v.Number {
			v.Sync.Sync()
		}
		v.stop()
		return
	}

	// Route by sequence, relative to the low watermark (the lowest undelivered sequence) and the depth W:
	//   [low-W, low)   delivered: answer with the prepare or commit sent, to help a lagging node catch up
	//   [low, low+W]   accepted: W sequences are processed, and one more is kept, since a follower may be
	//                  one delivery behind the leader (the "next" sequence of the sequential view)
	//   anything else  too old or too far ahead: find out whether we need to sync
	// With W=1 this is: the previous sequence, the current and the next one, and the rest.
	low, depth := v.ProposalSequence, v.depth()
	if msgProposalSeq < low && low-msgProposalSeq <= depth {
		v.handlePrevSeqMessage(msgProposalSeq, sender, m)
		return
	}

	v.Logger.Debugf("%d got message %s from %d with seq %d", v.SelfID, MsgToString(m), sender, msgProposalSeq)
	if msgProposalSeq < low || msgProposalSeq > low+depth {
		v.Logger.Infof("%d got message from %d with sequence %d but our sequence is %d", v.SelfID, sender, msgProposalSeq, v.ProposalSequence)
		v.discoverIfSyncNeeded(sender, m)
		return
	}

	slot := v.slot(msgProposalSeq)

	if pp := m.GetPrePrepare(); pp != nil {
		v.storePrePrepare(slot, pp, m, sender)
		return
	}

	// Else, it's a prepare or a commit.
	// Ignore votes from ourselves.
	if sender == v.SelfID {
		return
	}

	if prp := m.GetPrepare(); prp != nil {
		slot.prepares.registerVote(sender, m)
		return
	}

	if cmt := m.GetCommit(); cmt != nil {
		slot.commits.registerVote(sender, m)
		return
	}
}

// run drives the view: every event (a message, a verified commit signature) is followed by a pass over
// the open sequences, each advanced as far as what arrived allows. The passes do not block, so the
// loop only wakes up on events.
func (v *View) run() {
	defer v.viewEnded.Done()
	defer func() {
		v.ViewSequences.Store(ViewSequence{
			ProposalSeq: v.ProposalSequence,
			ViewActive:  false,
		})
	}()
	if v.resendTicker != nil {
		defer v.resendTicker.Stop()
	}
	v.doPhase() // a restored proposal broadcasts its prepare or commit again right away
	for {
		select {
		case <-v.abortChan:
			return
		case msg := <-v.incMsgs:
			v.processMsg(msg.sender, msg.Message)
		case vs := <-v.verifiedSigs:
			v.acceptVerifiedSig(vs)
		case <-v.resendTick: // nil, so never, unless the depth is above one and this node leads
			v.resendPrePrepares()
		}
		if v.Stopped() {
			return
		}
		v.doPhase()
	}
}

// doPhase advances the sequences in [low, low+W) in order, as far as the messages that arrived allow,
// and delivers the committed ones in order. A sequence is processed (verified, persisted, prepared) only
// once every lower sequence of the window has been: the prepare of sequence k acknowledges the
// pre-prepares of all sequences up to k, so the processed sequences always form a prefix of the window.
func (v *View) doPhase() {
	for !v.Stopped() {
		low := v.ProposalSequence
		v.stepWindow()
		v.tryDeliver()
		if v.ProposalSequence == low {
			break
		}
		// Something was delivered, so the window moved: the sequences that entered it may be processed now.
	}

	v.Phase = v.phaseOfLowest()
	v.MetricsView.Phase.Set(float64(v.Phase))
}

// stepWindow steps every sequence in [low, low+W) once, in order, stopping at the first gap.
func (v *View) stepWindow() {
	low, depth := v.ProposalSequence, v.depth()
	for seq := low; seq < low+depth; seq++ {
		slot := v.slots[seq]
		if slot == nil {
			return // nothing arrived for this sequence, so nothing above it may be processed
		}
		if slot.phase == slotEmpty {
			if slot.prePrepare == nil {
				return // a gap: its votes and those of the sequences above are kept until it is filled
			}
			if !v.processProposal(slot) {
				return // the proposal was rejected and the view is stopping
			}
		}
		if slot.phase == slotProposed {
			if !slot.prepareBroadcast {
				v.Comm.BroadcastConsensus(slot.lastBroadcastSent) // broadcast here serves also recovery
				slot.prepareBroadcast = true
			}
			v.stepPrepares(slot)
		}
		if slot.phase == slotPrepared {
			if !slot.commitBroadcast {
				v.Comm.BroadcastConsensus(slot.lastBroadcastSent)
				slot.commitBroadcast = true
			}
			v.stepCommits(slot)
		}
		// a committed slot waits for tryDeliver, which delivers in order
	}
}

// phaseOfLowest maps the slot of the lowest undelivered sequence to the phase of the view.
func (v *View) phaseOfLowest() Phase {
	if v.Stopped() {
		return ABORT
	}
	slot := v.slots[v.ProposalSequence]
	if slot == nil {
		return COMMITTED
	}
	switch slot.phase {
	case slotProposed:
		return PROPOSED
	case slotPrepared, slotCommitted:
		return PREPARED
	default:
		return COMMITTED
	}
}

// storePrePrepare keeps the first pre-prepare of a sequence, to be processed in order by doPhase.
func (v *View) storePrePrepare(slot *seqState, pp *protos.PrePrepare, m *protos.Message, sender uint64) {
	if pp.GetProposal() == nil {
		v.Logger.Warnf("%d got pre-prepare from %d with empty proposal", v.SelfID, sender)
		return
	}
	if sender != v.LeaderID {
		v.Logger.Warnf("%d got pre-prepare from %d but the leader is %d", v.SelfID, sender, v.LeaderID)
		return
	}

	if slot.prePrepare != nil || slot.phase != slotEmpty {
		// A sequence accepts one pre-prepare only; a conflicting one is dropped, the first one wins.
		if v.depth() > 1 && proto.Equal(slot.prePrepare, m) {
			// The leader of a pipelined view broadcasts its open pre-prepares again periodically, so
			// the same message is expected to arrive again; it is not worth a warning.
			v.Logger.Debugf("Got the pre-prepare for sequence %d again, ignoring it", slot.seq)
			return
		}
		currentOrNext := "current"
		if slot.seq != v.ProposalSequence {
			currentOrNext = "next"
		}
		v.Logger.Warnf("Got a pre-prepare for %s sequence without processing previous one, dropping message", currentOrNext)
		return
	}

	slot.prePrepare = m
}

// acceptVerifiedSig collects a verified commit signature into its slot, up to the Quorum-1 the slot needs.
func (v *View) acceptVerifiedSig(vs verifiedSig) {
	slot := v.slots[vs.seq]
	if slot == nil || slot.phase != slotPrepared || len(slot.signatures) >= v.Quorum-1 {
		return
	}
	slot.signatures = append(slot.signatures, vs.sig)
}

// resendPrePrepares broadcasts again the pre-prepares of the sequences this leader proposed and has not
// seen a prepare quorum for, so that a follower that missed one fills the gap and prepares what is above it.
// A sequence that is prepared here was accepted by a quorum, which no longer needs the pre-prepare and, with
// the prefix rule, holds every pre-prepare below it too; a node outside that quorum catches up through the
// previous sequence messages or through sync, as it always did. Resending the prepared sequences as well
// would only reach nodes that have them, and some that have already delivered them.
func (v *View) resendPrePrepares() {
	for seq := v.ProposalSequence; seq < v.head; seq++ {
		slot := v.slots[seq]
		if slot == nil || slot.prePrepare == nil || slot.phase >= slotPrepared {
			continue
		}
		v.Comm.BroadcastConsensus(slot.prePrepare)
	}
}

// RestoredCount returns how many proposals in flight the view started with: restored from the WAL, or the
// one carried over by a view change. The leader issues that many tokens less (R1).
func (v *View) RestoredCount() uint64 {
	return v.restoredCount
}

// inFlightCount returns how many sequences the leader proposed and has not delivered yet.
func (v *View) inFlightCount() uint64 {
	v.seqLock.Lock()
	defer v.seqLock.Unlock()
	return v.head - v.ProposalSequence
}

// onCommitQuorum marks a sequence as committed; tryDeliver delivers it once every lower sequence was delivered.
func (v *View) onCommitQuorum(slot *seqState) {
	proposal := slot.proposal
	signatures := slot.signatures
	seq := slot.seq

	v.Logger.Infof("%d processed commits for proposal with seq %d", v.SelfID, seq)

	v.MetricsView.CountBatchAll.Add(1)
	v.MetricsView.CountTxsAll.Add(float64(len(slot.requests)))
	size := 0
	size += len(proposal.Metadata) + len(proposal.Header) + len(proposal.Payload)
	for i := range signatures {
		size += len(signatures[i].Value) + len(signatures[i].Msg)
	}
	v.MetricsView.SizeOfBatch.Add(float64(size))
	v.observeBatchProcessingLatency(seq)

	slot.phase = slotCommitted
}

// recordPrePrepareBegin marks the time this node accepted the pre-prepare of the given sequence,
// from which the latency of processing its proposal is measured, and counts it as in flight.
func (v *View) recordPrePrepareBegin(seq uint64) {
	if v.prePrepareBegin == nil {
		v.prePrepareBegin = make(map[uint64]time.Time)
	}
	v.prePrepareBegin[seq] = time.Now()
	v.MetricsView.InFlightProposals.Set(float64(len(v.prePrepareBegin)))
}

// observeBatchProcessingLatency observes the time from the pre-prepare of the given sequence
// to its commit quorum, and forgets the sequence. A sequence without a recorded pre-prepare is skipped.
func (v *View) observeBatchProcessingLatency(seq uint64) {
	begin, recorded := v.prePrepareBegin[seq]
	if !recorded {
		return
	}
	delete(v.prePrepareBegin, seq)
	v.MetricsView.LatencyBatchProcessing.Observe(time.Since(begin).Seconds())
}

// processProposal verifies the pre-prepare of the slot, persists it and prepares the prepare to broadcast.
// It returns false when the proposal is rejected, in which case the view is stopping.
func (v *View) processProposal(slot *seqState) bool {
	receivedProposal := slot.prePrepare
	prePrepare := receivedProposal.GetPrePrepare()
	prop := prePrepare.GetProposal()
	prevCommits := prePrepare.GetPrevCommitSignatures()
	proposal := types.Proposal{
		VerificationSequence: int64(prop.GetVerificationSequence()),
		Metadata:             prop.GetMetadata(),
		Payload:              prop.GetPayload(),
		Header:               prop.GetHeader(),
	}

	requests, err := v.verifyProposal(slot.seq, proposal, prevCommits)
	if err != nil {
		v.Logger.Warnf("%d received bad proposal from %d: %v", v.SelfID, v.LeaderID, err)
		v.FailureDetector.Complain(v.Number, false)
		v.Sync.Sync()
		v.stop()
		return false
	}

	v.MetricsView.CountTxsInBatch.Set(float64(len(requests)))

	seq := slot.seq
	v.recordPrePrepareBegin(seq)

	prepareMessage := v.createPrepare(seq, proposal)

	// We are about to send a prepare for a pre-prepare, so we record the pre-prepare. The record of the
	// lowest open sequence starts the set of records a restart needs, hence everything written before it
	// may be truncated; the record of a higher sequence of the window must keep the lower ones next to it.
	// With a depth of one the processed sequence is always the lowest, so the record always truncates, as
	// it did. Restore drops the records of the sequences delivered since (the checkpoint filter), which is
	// what makes this truncation point exact.
	savedMsg := &protos.SavedMessage{
		Content: &protos.SavedMessage_ProposedRecord{
			ProposedRecord: &protos.ProposedRecord{
				PrePrepare: receivedProposal.GetPrePrepare(),
				Prepare:    prepareMessage.GetPrepare(),
			},
		},
	}
	if err = v.State.Save(savedMsg, seq == v.ProposalSequence); err != nil {
		v.Logger.Panicf("Failed to save message to state, error: %v", err)
	}
	slot.lastBroadcastSent = prepareMessage
	slot.prepareSent = proto.Clone(prepareMessage).(*protos.Message)
	slot.prepareSent.GetPrepare().Assist = true
	slot.proposal = &proposal
	slot.requests = requests

	if v.SelfID == v.LeaderID {
		v.Comm.BroadcastConsensus(receivedProposal)
	}

	v.Logger.Infof("Processed proposal with seq %d", seq)
	slot.phase = slotProposed
	return true
}

func (v *View) createPrepare(seq uint64, proposal types.Proposal) *protos.Message {
	return &protos.Message{
		Content: &protos.Message_Prepare{
			Prepare: &protos.Prepare{
				Seq:    seq,
				View:   v.Number,
				Digest: proposal.Digest(),
			},
		},
	}
}

// stepPrepares consumes the prepares that arrived for the slot and, on a quorum, signs and persists the commit
// to broadcast. It does not wait: a slot without a quorum yet is stepped again after the next event.
func (v *View) stepPrepares(slot *seqState) {
	proposal := slot.proposal
	expectedDigest := slot.digest()

	for len(slot.voterIDs) < v.Quorum-1 {
		select {
		case vote := <-slot.prepares.votes:
			prepare := vote.GetPrepare()
			if prepare.GetDigest() != expectedDigest {
				seq := v.ProposalSequence
				v.Logger.Warnf("Got wrong digest at processPrepares for prepare with seq %d, expecting %v but got %v, we are in seq %d", prepare.GetSeq(), expectedDigest, prepare.GetDigest(), seq)
				continue
			}
			slot.voterIDs = append(slot.voterIDs, vote.sender)
		default:
			return // no more prepares for now
		}
	}
	voterIDs := slot.voterIDs

	v.Logger.Infof("%d collected %d prepares from %v", v.SelfID, len(voterIDs), voterIDs)

	// SignProposal returns a types.Signature with the following 3 fields:
	// ID: The integer that represents this node.
	// Value: The signature, encoded according to the specific signature specification.
	// Msg: A succinct representation of the proposal that binds this proposal unequivocally.

	// The block proof consists of the aggregation of all these signatures from 2f+1 commits of different nodes.

	prpFrom := &protos.PreparesFrom{
		Ids: voterIDs,
	}

	prpFromRaw, err := proto.Marshal(prpFrom)
	if err != nil {
		v.Logger.Panicf("Failed marshaling prepares from: %v", err)
	}

	slot.mySig = v.Signer.SignProposal(*proposal, prpFromRaw)

	seq := slot.seq

	commitMsg := &protos.Message{
		Content: &protos.Message_Commit{
			Commit: &protos.Commit{
				View:   v.Number,
				Digest: expectedDigest,
				Seq:    seq,
				Signature: &protos.Signature{
					Signer: slot.mySig.ID,
					Value:  slot.mySig.Value,
					Msg:    slot.mySig.Msg,
				},
			},
		},
	}

	preparedProof := &protos.SavedMessage{
		Content: &protos.SavedMessage_Commit{
			Commit: commitMsg,
		},
	}

	// We received enough prepares to send a commit.
	// Save the commit message we are about to send. A commit must not be a
	// truncation point: its pre-prepare has to survive next to it.
	if err = v.State.Save(preparedProof, false); err != nil {
		v.Logger.Panicf("Failed to save message to state, error: %v", err)
	}
	slot.commitSent = proto.Clone(commitMsg).(*protos.Message)
	slot.commitSent.GetCommit().Assist = true
	slot.lastBroadcastSent = commitMsg

	v.Logger.Infof("Processed prepares for proposal with seq %d", seq)
	slot.phase = slotPrepared
}

// stepCommits hands the commits that arrived for the slot to verification goroutines, whose results come
// back through verifiedSigs, and marks the slot committed once Quorum-1 signatures were verified.
func (v *View) stepCommits(slot *seqState) {
	if slot.sigCollector == nil {
		slot.sigCollector = &voteVerifier{
			seq:            slot.seq,
			expectedDigest: slot.digest(),
			proposal:       slot.proposal,
			v:              v,
		}
	}

	collector := slot.sigCollector // the slot may be released before a goroutine runs; the collector stays
	for {
		select {
		case vote := <-slot.commits.votes:
			// Valid votes end up written into the 'verifiedSigs' channel.
			go func(vote *protos.Message) {
				collector.verifyVote(vote)
			}(vote.Message)
			continue
		default:
		}
		break
	}

	if len(slot.signatures) < v.Quorum-1 {
		return
	}

	voterIDs := make([]uint64, 0, len(slot.signatures))
	for _, signature := range slot.signatures {
		voterIDs = append(voterIDs, signature.ID)
	}
	v.Logger.Infof("%d collected %d commits from %v", v.SelfID, len(slot.signatures), voterIDs)

	v.onCommitQuorum(slot)
}

// verifyProposal verifies a proposal that was pre-prepared for the given sequence.
func (v *View) verifyProposal(seq uint64, proposal types.Proposal, prevCommits []*protos.Signature) ([]types.RequestInfo, error) {
	// Verify proposal has correct structure and contains authorized requests.
	requests, err := v.Verifier.VerifyProposal(proposal)
	if err != nil {
		v.Logger.Warnf("Received bad proposal: %v", err)
		return nil, err
	}

	// Verify proposal's metadata is valid.
	md := &protos.ViewMetadata{}
	if err = proto.Unmarshal(proposal.Metadata, md); err != nil {
		return nil, err
	}

	if md.GetViewId() != v.Number {
		v.Logger.Warnf("Expected view number %d but got %d", v.Number, md.GetViewId())
		return nil, errors.New("invalid view number")
	}

	if md.GetLatestSequence() != seq {
		v.Logger.Warnf("Expected proposal sequence %d but got %d", seq, md.GetLatestSequence())
		return nil, errors.New("invalid proposal sequence")
	}

	if expectedDecisions := v.expectedDecisions(seq); md.GetDecisionsInView() != expectedDecisions {
		v.Logger.Warnf("Expected decisions in view %d but got %d", expectedDecisions, md.GetDecisionsInView())
		return nil, errors.New("invalid decisions in view")
	}

	expectedSeq := v.Verifier.VerificationSequence()
	if uint64(proposal.VerificationSequence) != expectedSeq {
		v.Logger.Warnf("Expected verification sequence %d but got %d", expectedSeq, proposal.VerificationSequence)
		return nil, errors.New("verification sequence mismatch")
	}

	prepareAcknowledgements, err := v.verifyPrevCommitSignatures(prevCommits, expectedSeq)
	if err != nil {
		return nil, err
	}

	if err = v.verifyBlacklist(prevCommits, expectedSeq, md.GetBlackList(), prepareAcknowledgements); err != nil {
		return nil, err
	}

	// Check that the metadata contains a digest of the previous commit signatures
	prevCommitDigest := CommitSignaturesDigest(prevCommits)
	if !bytes.Equal(prevCommitDigest, md.GetPrevCommitSignatureDigest()) && v.DecisionsPerLeader > 0 {
		return nil, errors.New("prev commit signatures received from leader mismatches the metadata digest")
	}

	return requests, nil
}

func (v *View) verifyPrevCommitSignatures(prevCommitSignatures []*protos.Signature, currVerificationSeq uint64) (map[uint64]*protos.PreparesFrom, error) {
	prevPropRaw, _ := v.RetrieveCheckpoint()
	prevProposalMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevPropRaw.GetMetadata(), prevProposalMetadata); err != nil {
		v.Logger.Panicf("Couldn't unmarshal the previous persisted proposal metadata: %v", err)
	}

	v.Logger.Debugf("Previous proposal verification sequence: %d, current verification sequence: %d", prevPropRaw.GetVerificationSequence(), currVerificationSeq)
	if prevPropRaw.GetVerificationSequence() != currVerificationSeq {
		v.Logger.Infof("Skipping verifying prev commit signatures due to verification sequence advancing from %d to %d",
			prevPropRaw.GetVerificationSequence(), currVerificationSeq)
		return nil, nil
	}

	prepareAcknowledgements := make(map[uint64]*protos.PreparesFrom)

	prevProp := types.Proposal{
		VerificationSequence: int64(prevPropRaw.GetVerificationSequence()),
		Metadata:             prevPropRaw.GetMetadata(),
		Payload:              prevPropRaw.GetPayload(),
		Header:               prevPropRaw.GetHeader(),
	}

	// All previous commit signatures should be verifiable
	for _, sig := range prevCommitSignatures {
		aux, err := v.Verifier.VerifyConsenterSig(types.Signature{
			ID:    sig.GetSigner(),
			Msg:   sig.GetMsg(),
			Value: sig.GetValue(),
		}, prevProp)
		if err != nil {
			return nil, fmt.Errorf("failed verifying consenter signature of %d: %w", sig.GetSigner(), err)
		}
		prpf := &protos.PreparesFrom{}
		if err = proto.Unmarshal(aux, prpf); err != nil {
			return nil, fmt.Errorf("failed unmarshaling auxiliary input from %d: %w", sig.GetSigner(), err)
		}
		prepareAcknowledgements[sig.GetSigner()] = prpf
	}

	return prepareAcknowledgements, nil
}

func (v *View) verifyBlacklist(prevCommitSignatures []*protos.Signature, currVerificationSeq uint64, pendingBlacklist []uint64, prepareAcknowledgements map[uint64]*protos.PreparesFrom) error {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("DecisionsPerLeader is 0, hence leader rotation is inactive")
		if len(pendingBlacklist) > 0 {
			v.Logger.Warnf("Blacklist cannot be non-empty (%v) if rotation is inactive", pendingBlacklist)
			return fmt.Errorf("rotation is inactive but blacklist is not empty: %v", pendingBlacklist)
		}
		return nil
	}

	prevPropRaw, myLastCommitSignatures := v.RetrieveCheckpoint()
	prevProposalMetadata := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevPropRaw.GetMetadata(), prevProposalMetadata); err != nil {
		v.Logger.Panicf("Couldn't unmarshal the previous persisted proposal metadata: %v", err)
	}

	v.Logger.Debugf("Previous proposal verification sequence: %d, current verification sequence: %d", prevPropRaw.GetVerificationSequence(), currVerificationSeq)
	if prevPropRaw.GetVerificationSequence() != currVerificationSeq {
		// If there has been a reconfiguration, black list should remain the same
		if !slices.Equal(prevProposalMetadata.GetBlackList(), pendingBlacklist) {
			return fmt.Errorf("blacklist changed (%v --> %v) during reconfiguration", prevProposalMetadata.GetBlackList(), pendingBlacklist)
		}
		v.Logger.Infof("Skipping verifying prev commits due to verification sequence advancing from %d to %d",
			prevPropRaw.GetVerificationSequence(), currVerificationSeq)
		return nil
	}

	if v.MembershipNotifier != nil && v.MembershipNotifier.MembershipChange() {
		// If there has been a membership change, black list should remain the same
		if !slices.Equal(prevProposalMetadata.GetBlackList(), pendingBlacklist) {
			return fmt.Errorf("blacklist changed (%v --> %v) during membership change", prevProposalMetadata.GetBlackList(), pendingBlacklist)
		}
		v.Logger.Infof("Skipping verifying prev commits due to membership change")
		return nil
	}

	_, f := computeQuorum(v.N)

	if v.blacklistingSupported(f, myLastCommitSignatures) && len(prevCommitSignatures) < len(myLastCommitSignatures) {
		return fmt.Errorf("only %d out of %d required previous commits is included in pre-prepare",
			len(prevCommitSignatures), len(myLastCommitSignatures))
	}

	// We previously verified the previous commit signatures, now we need to ensure that the blacklist
	// of this proposal is obtained by applying the deterministic blacklist maintenance algorithm
	// on the blacklist of the previous proposal which has been committed.

	blacklist := &blacklist{
		currentLeader:      v.LeaderID,
		leaderRotation:     v.DecisionsPerLeader > 0,
		n:                  v.N,
		prevMD:             prevProposalMetadata,
		decisionsPerLeader: v.DecisionsPerLeader,
		preparesFrom:       prepareAcknowledgements,
		f:                  f,
		logger:             v.Logger,
		metricsBlacklist:   v.MetricsBlacklist,
		nodes:              v.NodesList,
		currView:           v.Number,
	}

	expectedBlacklist := blacklist.computeUpdate()
	if !slices.Equal(pendingBlacklist, expectedBlacklist) {
		return fmt.Errorf("proposed blacklist %v differs from expected %v blacklist", pendingBlacklist, expectedBlacklist)
	}

	return nil
}

func (v *View) handlePrevSeqMessage(msgProposalSeq, sender uint64, m *protos.Message) {
	if m.GetPrePrepare() != nil {
		if v.depth() > 1 {
			// The leader of a pipelined view broadcasts its open pre-prepares again periodically, and one
			// of them may reach a node that delivered the sequence meanwhile: expected, not a warning.
			v.Logger.Debugf("Got pre-prepare for sequence %d but we're in sequence %d", msgProposalSeq, v.ProposalSequence)
			return
		}
		v.Logger.Warnf("Got pre-prepare for sequence %d but we're in sequence %d", msgProposalSeq, v.ProposalSequence)
		return
	}
	msgType := "prepare"
	if m.GetCommit() != nil {
		msgType = "commit"
	}

	var found bool

	// The slot of a delivered sequence keeps the prepare and commit sent for it, if this view sent them.
	var prevPrepareSent, prevCommitSent *protos.Message
	if slot := v.slots[msgProposalSeq]; slot != nil {
		prevPrepareSent, prevCommitSent = slot.prepareSent, slot.commitSent
	}

	switch msgType {
	case "prepare":
		// This is an assist message, we don't need to reply to it.
		if m.GetPrepare().GetAssist() {
			return
		}
		if prevPrepareSent != nil {
			v.Comm.SendConsensus(sender, prevPrepareSent)
			found = true
		}
	case "commit":
		// This is an assist message, we don't need to reply to it.
		if m.GetCommit().GetAssist() {
			return
		}
		if prevCommitSent != nil {
			v.Comm.SendConsensus(sender, prevCommitSent)
			found = true
		}
	}

	prevMsgFound := fmt.Sprintf("but didn't have a previous %s to send back.", msgType)
	if found {
		prevMsgFound = fmt.Sprintf("sent back previous %s.", msgType)
	}
	v.Logger.Debugf("Got %s for previous sequence (%d) from %d, %s", msgType, msgProposalSeq, sender, prevMsgFound)
}

func (v *View) discoverIfSyncNeeded(sender uint64, m *protos.Message) {
	// We're only interested in commit messages.
	commit := m.GetCommit()
	if commit == nil {
		return
	}

	// To commit a block we need 2f + 1 votes.
	// at least f+1 of them are honest and will broadcast
	// their commits to votes to everyone including us.
	// In each such a threshold of f+1 votes there is at least
	// a single honest node that prepared for a proposal
	// which we apparently missed.
	_, f := computeQuorum(v.N)
	threshold := f + 1

	v.lastVotedProposalByID[sender] = commit

	v.Logger.Debugf("Got commit of seq %d in view %d from %d while being in seq %d in view %d",
		commit.GetSeq(), commit.GetView(), sender, v.ProposalSequence, v.Number)

	// If we haven't reached a threshold of proposals yet, abort.
	if len(v.lastVotedProposalByID) < threshold {
		return
	}

	// Make a histogram out of all current seen votes.
	countsByVotes := make(map[proposalInfo]int)
	for _, vote := range v.lastVotedProposalByID {
		info := proposalInfo{
			digest: vote.GetDigest(),
			view:   vote.GetView(),
			seq:    vote.GetSeq(),
		}
		countsByVotes[info]++
	}

	// Check if there is a <digest, view, seq> that collected a threshold of votes,
	// and that sequence is higher than our current sequence, or our view is different.
	for vote, count := range countsByVotes {
		if count < threshold {
			continue
		}

		// Disregard votes for past views.
		if vote.view < v.Number {
			continue
		}

		// Disregard votes for past sequences for this view.
		if vote.seq <= v.ProposalSequence && vote.view == v.Number {
			continue
		}

		v.Logger.Warnf("Seen %d votes for digest %s in view %d, sequence %d but I am in view %d and seq %d",
			count, vote.digest, vote.view, vote.seq, v.Number, v.ProposalSequence)
		v.stop()
		v.Sync.Sync()
		return
	}
}

type voteVerifier struct {
	v              *View
	seq            uint64
	proposal       *types.Proposal
	expectedDigest string
}

func (vv *voteVerifier) verifyVote(vote *protos.Message) {
	commit := vote.GetCommit()
	if commit.GetDigest() != vv.expectedDigest {
		vv.v.Logger.Warnf("Got wrong digest at processCommits for seq %d", commit.GetSeq())
		return
	}

	_, err := vv.v.Verifier.VerifyConsenterSig(types.Signature{
		ID:    commit.GetSignature().GetSigner(),
		Value: commit.GetSignature().GetValue(),
		Msg:   commit.GetSignature().GetMsg(),
	}, *vv.proposal)
	if err != nil {
		vv.v.Logger.Warnf("Couldn't verify %d's signature: %v", commit.GetSignature().GetSigner(), err)
		return
	}

	verified := verifiedSig{
		seq: vv.seq,
		sig: types.Signature{
			ID:    commit.GetSignature().GetSigner(),
			Value: commit.GetSignature().GetValue(),
			Msg:   commit.GetSignature().GetMsg(),
		},
	}
	select {
	case vv.v.verifiedSigs <- verified:
	case <-vv.v.abortChan: // the view ended, nobody collects signatures anymore
	}
}

// tryDeliver delivers the committed sequences in order: only the lowest undelivered sequence is delivered,
// and only once its commit quorum was collected. Delivery blocks the view, as it did, so a later sequence
// that is already committed waits in its slot for the next iteration.
func (v *View) tryDeliver() {
	for {
		slot := v.slots[v.ProposalSequence]
		if slot == nil || slot.phase != slotCommitted || v.Stopped() {
			return
		}
		v.Logger.Infof("Deciding on seq %d", slot.seq)
		// first make preparations for the next sequence so that the view will be ready to continue right after delivery
		v.advanceLow()
		signatures := make([]types.Signature, 0, len(slot.signatures)+1)
		signatures = append(signatures, slot.signatures...)
		signatures = append(signatures, *slot.mySig)
		v.Decider.Decide(*slot.proposal, signatures, slot.requests)
		v.MetricsView.InFlightProposals.Set(float64(len(v.prePrepareBegin)))
		slot.phase = slotDelivered
		slot.release()
		v.gcSlots()
	}
}

// advanceLow moves the low watermark past the sequence that is being delivered: the only place where
// ProposalSequence and DecisionsInView advance, always together.
func (v *View) advanceLow() {
	v.seqLock.Lock()
	prevSeq := v.ProposalSequence

	v.ProposalSequence++
	v.DecisionsInView++
	// A follower, or a leader that restored a proposal, proposes nothing: its head follows the low watermark.
	v.head = max(v.head, v.ProposalSequence)

	nextSeq := v.ProposalSequence
	v.seqLock.Unlock()

	// Publish the sequence this view is about to propose next. Without this the stored
	// sequence stays at the one that was just decided, and every reader of it - the
	// heartbeat a leader broadcasts, the state a node collects from its peers, and the
	// follower that compares its own sequence against it - ends up comparing a decided
	// sequence against a proposal sequence, which hides a follower that is behind by
	// exactly the sequence in flight.
	v.ViewSequences.Store(ViewSequence{ProposalSeq: nextSeq, ViewActive: true})

	v.MetricsView.ProposalSequence.Set(float64(nextSeq))
	v.MetricsView.DecisionsInView.Set(float64(v.DecisionsInView))

	v.Logger.Infof("Sequence: %d-->%d", prevSeq, nextSeq)
}

// gcSlots forgets the delivered sequences below the window that answers lagging nodes.
func (v *View) gcSlots() {
	low, depth := v.ProposalSequence, v.depth()
	if low <= depth {
		return
	}
	for seq := range v.slots {
		if seq < low-depth {
			delete(v.slots, seq)
		}
	}
}

// GetMetadata returns the sequence the leader proposes next and the view number (in a marshaled ViewMetadata protobuf message)
func (v *View) GetMetadata() []byte {
	v.seqLock.Lock()
	seq := v.head
	decisions := v.expectedDecisions(seq)
	v.seqLock.Unlock()

	metadata := &protos.ViewMetadata{
		ViewId:          v.Number,
		LatestSequence:  seq,
		DecisionsInView: decisions,
	}

	v.Logger.Debugf("GetMetadata with view %d, seq %d, dec %d", metadata.GetViewId(), metadata.GetLatestSequence(), metadata.GetDecisionsInView())

	var (
		prevSigs []*protos.Signature
		prevProp *protos.Proposal
	)
	verificationSeq := v.Verifier.VerificationSequence()

	prevProp, prevSigs = v.RetrieveCheckpoint()

	prevMD := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevProp.GetMetadata(), prevMD); err != nil {
		v.Logger.Panicf("Attempted to propose a proposal with invalid unchanged previous proposal view metadata: %v", err)
	}

	metadata.BlackList = prevMD.GetBlackList()

	metadata = v.metadataWithUpdatedBlacklist(metadata, verificationSeq, prevProp, prevSigs)
	metadata = v.bindCommitSignaturesToProposalMetadata(metadata, prevSigs)

	return MarshalOrPanic(metadata)
}

func (v *View) metadataWithUpdatedBlacklist(metadata *protos.ViewMetadata, verificationSeq uint64, prevProp *protos.Proposal, prevSigs []*protos.Signature) *protos.ViewMetadata {
	var membershipChange bool
	if v.MembershipNotifier != nil {
		membershipChange = v.MembershipNotifier.MembershipChange()
	}

	if verificationSeq == prevProp.GetVerificationSequence() && !membershipChange {
		v.Logger.Debugf("Proposing proposal %d with verification sequence of %d and %d commit signatures",
			metadata.GetLatestSequence(), verificationSeq, len(prevSigs))
		return v.updateBlacklistMetadata(metadata, prevSigs, prevProp.GetMetadata())
	}

	if verificationSeq != prevProp.GetVerificationSequence() {
		v.Logger.Infof("Skipping updating blacklist due to verification sequence changing from %d to %d",
			prevProp.GetVerificationSequence(), verificationSeq)
	}
	if membershipChange {
		v.Logger.Infof("Skipping updating blacklist due to membership change")
	}

	return metadata
}

// Propose broadcasts a prePrepare message with the given proposal
func (v *View) Propose(proposal types.Proposal) {
	var prevSigs []*protos.Signature
	if v.DecisionsPerLeader > 0 {
		_, prevSigs = v.RetrieveCheckpoint()
	}

	// The sequence this proposal takes; the next proposal takes the one after it, before this one is delivered.
	v.seqLock.Lock()
	seq := v.head
	v.head++
	v.seqLock.Unlock()
	msg := &protos.Message{
		Content: &protos.Message_PrePrepare{
			PrePrepare: &protos.PrePrepare{
				View: v.Number,
				Seq:  seq,
				Proposal: &protos.Proposal{
					Header:               proposal.Header,
					Payload:              proposal.Payload,
					Metadata:             proposal.Metadata,
					VerificationSequence: uint64(proposal.VerificationSequence),
				},
				PrevCommitSignatures: prevSigs,
			},
		},
	}
	// Send the proposal to yourself in order to pre-prepare yourself and record
	// it in the WAL before sending it to other nodes.
	v.HandleMessage(v.LeaderID, msg)
	v.Logger.Debugf("Proposing proposal sequence %d in view %d", seq, v.Number)
}

func (v *View) bindCommitSignaturesToProposalMetadata(metadata *protos.ViewMetadata, prevSigs []*protos.Signature) *protos.ViewMetadata {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("Leader rotation is disabled, will not bind signatures to proposals")
		return metadata
	}
	metadata.PrevCommitSignatureDigest = CommitSignaturesDigest(prevSigs)

	if len(metadata.GetPrevCommitSignatureDigest()) == 0 {
		v.Logger.Debugf("No previous commit signatures detected")
	} else {
		v.Logger.Debugf("Bound %d commit signatures to proposal", len(prevSigs))
	}
	return metadata
}

func (v *View) stop() {
	v.stopOnce.Do(func() {
		if v.abortChan == nil {
			return
		}
		close(v.abortChan)
	})
}

// Abort forces the view to end
func (v *View) Abort() {
	v.stop()
	v.viewEnded.Wait()
}

func (v *View) Stopped() bool {
	select {
	case <-v.abortChan:
		return true
	default:
		return false
	}
}

func (v *View) AbortChan() <-chan struct{} {
	return v.abortChan
}

func (v *View) GetLeaderID() uint64 {
	return v.LeaderID
}

func (v *View) updateBlacklistMetadata(metadata *protos.ViewMetadata, prevSigs []*protos.Signature, prevMetadata []byte) *protos.ViewMetadata {
	if v.DecisionsPerLeader == 0 {
		v.Logger.Debugf("Rotation is disabled, setting blacklist to be empty")
		metadata.BlackList = nil
		return metadata
	}

	preparesFrom := make(map[uint64]*protos.PreparesFrom)

	for _, sig := range prevSigs {
		aux := v.Verifier.AuxiliaryData(sig.GetMsg())
		prpf := &protos.PreparesFrom{}
		if err := proto.Unmarshal(aux, prpf); err != nil {
			v.Logger.Panicf("Failed unmarshalling auxiliary data from previously persisted signatures: %v", err)
		}
		preparesFrom[sig.GetSigner()] = prpf
	}

	prevMD := &protos.ViewMetadata{}
	if err := proto.Unmarshal(prevMetadata, prevMD); err != nil {
		v.Logger.Panicf("Attempted to propose a proposal with invalid previous proposal view metadata: %v", err)
	}

	_, f := computeQuorum(v.N)

	blacklist := &blacklist{
		currentLeader:      v.LeaderID,
		leaderRotation:     v.DecisionsPerLeader > 0,
		currView:           metadata.GetViewId(),
		prevMD:             prevMD,
		nodes:              v.NodesList,
		f:                  f,
		n:                  v.N,
		logger:             v.Logger,
		metricsBlacklist:   v.MetricsBlacklist,
		preparesFrom:       preparesFrom,
		decisionsPerLeader: v.DecisionsPerLeader,
	}
	metadata.BlackList = blacklist.computeUpdate()
	return metadata
}

func (v *View) blacklistingSupported(f int, myLastCommitSignatures []*protos.Signature) bool {
	// Once we blacklist, there is no way back. This is a one way trip, unless we downgrade the version
	// in all nodes and view change.
	if v.blacklistSupported {
		return true
	}
	// We wish to find whether there are f+1 witnesses for blacklisting being
	// activated among the signed commits of the previous proposal.
	var count int
	for _, commitSig := range myLastCommitSignatures {
		aux := v.Verifier.AuxiliaryData(commitSig.GetMsg())
		if len(aux) > 0 {
			count++
		}
	}

	v.Logger.Debugf("Found %d out of %d required witnesses for auxiliary data", count, f+1)

	blacklistSupported := count > f

	// We cache the result in case it is 'true'.
	// Subsequent invocations will skip the parsing.
	v.blacklistSupported = v.blacklistSupported || blacklistSupported
	return blacklistSupported
}
