// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"google.golang.org/protobuf/proto"
)

type StateRecorder struct {
	SavedMessages []*protos.SavedMessage
	TruncateTo    []bool
}

func (sr *StateRecorder) Save(message *protos.SavedMessage, truncateTo bool) error {
	sr.SavedMessages = append(sr.SavedMessages, message)
	sr.TruncateTo = append(sr.TruncateTo, truncateTo)
	return nil
}

func (*StateRecorder) Restore(_ *View) error {
	panic("should not be used")
}

type PersistedState struct {
	InFlightProposal *InFlightData
	Entries          [][]byte
	Logger           api.Logger
	WAL              api.WriteAheadLog
	Metrics          *api.MetricsState // may be nil

	// recordsSinceBoundary counts the records since the last truncation point, the one included: what a
	// restart would read back. With a pipeline it grows while the lowest open sequence stays open.
	recordsSinceBoundary int
}

func (ps *PersistedState) setRecordsSinceBoundary(n int) {
	ps.recordsSinceBoundary = n
	if ps.Metrics != nil {
		ps.Metrics.RecordsSinceBoundary.Set(float64(n))
	}
}

func (ps *PersistedState) Save(msgToSave *protos.SavedMessage, truncateTo bool) error {
	if proposed := msgToSave.GetProposedRecord(); proposed != nil {
		ps.storeProposal(proposed)
	}
	if prepared := msgToSave.GetCommit(); prepared != nil {
		ps.storePrepared(prepared)
	}

	b, err := proto.Marshal(msgToSave)
	if err != nil {
		ps.Logger.Panicf("Failed marshaling message: %v", err)
	}
	// The caller decides whether this record is a truncation point and passes the
	// flag explicitly. Save stores the flag as given and never infers it from the
	// message type or from shared state.
	if err := ps.WAL.Append(b, truncateTo); err != nil {
		return err
	}
	if truncateTo {
		ps.setRecordsSinceBoundary(1)
	} else {
		ps.setRecordsSinceBoundary(ps.recordsSinceBoundary + 1)
	}
	return nil
}

func (ps *PersistedState) storeProposal(proposed *protos.ProposedRecord) {
	proposal := proposed.GetPrePrepare().GetProposal()
	proposalToStore := types.Proposal{
		VerificationSequence: int64(proposal.GetVerificationSequence()),
		Header:               proposal.GetHeader(),
		Payload:              proposal.GetPayload(),
		Metadata:             proposal.GetMetadata(),
	}
	// Keyed by the pre-prepare's sequence, the one the commit that may follow it carries.
	ps.InFlightProposal.StoreProposalAt(proposed.GetPrePrepare().GetSeq(), proposalToStore)
}

func (ps *PersistedState) storePrepared(commitMsg *protos.Message) {
	cmt := commitMsg.GetCommit()
	if !ps.InFlightProposal.StorePrepares(cmt.GetView(), cmt.GetSeq()) {
		ps.Logger.Warnf("The commit of sequence %d names view %d but the proposal in flight under that sequence is of another view; not marking it prepared", cmt.GetSeq(), cmt.GetView())
	}
}

func (ps *PersistedState) LoadNewViewIfApplicable() (*types.ViewAndSeq, error) {
	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil, nil
	}
	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return nil, fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}
	if newViewMsg := lastPersistedMessage.GetNewView(); newViewMsg != nil {
		ps.Logger.Infof("last entry in WAL is a newView record")
		return &types.ViewAndSeq{View: newViewMsg.GetViewId(), Seq: newViewMsg.GetLatestSequence()}, nil
	}
	return nil, nil
}

func (ps *PersistedState) LoadViewChangeIfApplicable() (*protos.ViewChange, error) {
	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil, nil
	}
	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return nil, fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}
	if viewChangeMsg := lastPersistedMessage.GetViewChange(); viewChangeMsg != nil {
		ps.Logger.Infof("last entry in WAL is a viewChange message")
		return viewChangeMsg, nil
	}
	return nil, nil
}

// restoredSlot is a proposal in flight read back from the WAL: the pre-prepare this node accepted and the
// prepare it sent (phase PROPOSED), plus the commit it sent when it had a prepare quorum (phase PREPARED).
type restoredSlot struct {
	seq             uint64
	view            uint64
	decisionsInView uint64
	proposal        *types.Proposal
	requests        []types.RequestInfo
	proposed        *protos.ProposedRecord
	commit          *protos.Message // nil when the sequence was not prepared
}

func (r *restoredSlot) phase() Phase {
	if r.commit != nil {
		return PREPARED
	}
	return PROPOSED
}

// lastBroadcastSent is the message the restored slot broadcasts again: its commit when it was prepared,
// its prepare otherwise.
func (r *restoredSlot) lastBroadcastSent() *protos.Message {
	if r.commit != nil {
		return r.commit
	}
	return &protos.Message{Content: &protos.Message_Prepare{Prepare: r.proposed.GetPrepare()}}
}

// prePrepare is the pre-prepare as the leader broadcast it, rebuilt from the record, so that a restored
// leader of a pipelined view can broadcast it again to the followers that missed it.
func (r *restoredSlot) prePrepare() *protos.Message {
	return &protos.Message{Content: &protos.Message_PrePrepare{PrePrepare: r.proposed.GetPrePrepare()}}
}

func (r *restoredSlot) mySig() *types.Signature {
	if r.commit == nil {
		return nil
	}
	sig := r.commit.GetCommit().GetSignature()
	return &types.Signature{ID: sig.GetSigner(), Msg: sig.GetMsg(), Value: sig.GetValue()}
}

// Restore reads the proposals in flight back from the WAL into the view: every pre-prepare, and the commit
// that may follow it, recorded after the last view change boundary and above the application's checkpoint.
//
// The WAL holds, since its last truncation point, the records of the sequences that were open when the
// truncation point was written (the pre-prepare of the lowest open sequence), in the order they were
// written: with a pipeline depth W the pre-prepares and commits of up to W sequences interleave. Restore
// folds them by sequence and keeps the ones that are still in flight:
//   - records before the last new-view or view-change record belong to a view that was abandoned, and the
//     view change decided their fate; they are dropped (D1);
//   - a sequence at or below the checkpoint was delivered already (the truncation point is the pre-prepare
//     of the lowest open sequence, which is delivered later, and the restart may fall in between); it is
//     dropped, so that it is neither delivered twice nor counted as in flight;
//   - a sequence above checkpoint+W cannot have been in flight: the WAL is corrupt or the node was restarted
//     with a smaller depth, which is an error;
//   - a record of another view than the boundary's is dropped with a warning: the node catches up by sync.
//
// The truncation rule (a pre-prepare truncates when its sequence is the lowest open one) and the checkpoint
// filter are a pair: the first keeps the WAL short, the second makes what it keeps exact. With a depth of one
// the fold holds one sequence and the result is the one the sequential view restored, except that a stale
// pre-prepare below the checkpoint is now dropped instead of being restored.
func (ps *PersistedState) Restore(v *View) error {
	// Unless we conclude otherwise, we're in a COMMITTED state
	v.Phase = COMMITTED
	v.restored = nil

	entries := ps.Entries
	if len(entries) == 0 {
		ps.Logger.Infof("Nothing to restore")
		return nil
	}

	ps.Logger.Infof("WAL contains %d entries", len(entries))
	ps.setRecordsSinceBoundary(len(entries))

	lastEntry := entries[len(entries)-1]
	lastPersistedMessage := &protos.SavedMessage{}
	if err := proto.Unmarshal(lastEntry, lastPersistedMessage); err != nil {
		ps.Logger.Errorf("Failed unmarshaling last entry from WAL: %v", err)
		return fmt.Errorf("failed unmarshaling last entry from WAL: %w", err)
	}

	switch {
	case lastPersistedMessage.GetNewView() != nil:
		ps.Logger.Infof("last entry in WAL is a newView record")
		return nil
	case lastPersistedMessage.GetViewChange() != nil:
		ps.Logger.Infof("last entry in WAL is a viewChange message")
		return ps.restoreInFlightBeforeViewChange(v, entries[:len(entries)-1], lastPersistedMessage.GetViewChange())
	case lastPersistedMessage.GetProposedRecord() == nil && lastPersistedMessage.GetCommit() == nil:
		return fmt.Errorf("unrecognized record: %v", lastPersistedMessage)
	}

	records, boundary, err := ps.unmarshalAfterBoundary(entries)
	if err != nil {
		return err
	}
	slots, err := foldSlots(records)
	if err != nil {
		return err
	}
	restored, err := ps.filterInFlight(slots, boundary, v)
	if err != nil {
		return err
	}
	restored = ps.validateRestored(restored, v)
	if len(restored) == 0 {
		ps.Logger.Infof("Every proposal in the WAL was delivered already, nothing in flight to restore")
		return nil
	}

	for _, r := range restored {
		r.requests = v.Verifier.RequestsFromProposal(*r.proposal)
		v.recordPrePrepareBegin(r.seq)
		ps.storeProposal(r.proposed)
		if r.commit != nil {
			ps.storePrepared(r.commit)
		}
		ps.Logger.Infof("Restored proposal with sequence %d", r.seq)
	}
	lowest := restored[0]
	v.restored = restored
	v.restoredCount = uint64(len(restored))
	v.Phase = lowest.phase()
	v.Number = lowest.view
	v.ProposalSequence = lowest.seq
	v.DecisionsInView = lowest.decisionsInView
	ps.Logger.Infof("Restored %d proposals in flight, sequences %d to %d", len(restored), lowest.seq, restored[len(restored)-1].seq)
	return nil
}

// restoreInFlightBeforeViewChange handles a WAL that ends with a view-change record: the node was taking part
// in a view change when it stopped, and the proposals it had in flight in the view it was leaving are still the
// evidence it owes the view change (a prepared proposal of this node may be one of the f+1 that protect a value
// some node committed, rule S2). They are restored into the in-flight data only, so that the view data this node
// sends again reports them; the view itself is not restored, the view-change record defines where the node is.
// The view-change record names the view being left in next_view (it is written before the view changer moves
// on), so the proposals kept are those of that view, after the previous view record if any.
func (ps *PersistedState) restoreInFlightBeforeViewChange(v *View, entries [][]byte, viewChange *protos.ViewChange) error {
	records, boundary, err := ps.unmarshalAfterBoundary(entries)
	if err != nil {
		return err
	}
	slots, err := foldSlots(records)
	if err != nil {
		return err
	}
	leaving := boundaryView{found: true, view: viewChange.GetNextView()}
	if boundary.found && boundary.view != leaving.view {
		// A view record of another view sits between: whatever follows it must be of the view being left.
		ps.Logger.Warnf("The view-change record leaves view %d but the previous view record is of view %d", leaving.view, boundary.view)
	}
	restored, err := ps.filterInFlight(slots, leaving, v)
	if err != nil {
		return err
	}
	restored = ps.validateRestored(restored, v)
	if len(restored) == 0 {
		ps.Logger.Infof("Nothing in flight before the viewChange record")
		return nil
	}
	for _, r := range restored {
		ps.storeProposal(r.proposed)
		if r.commit != nil {
			ps.storePrepared(r.commit)
		}
	}
	ps.Logger.Infof("Restored %d proposals in flight of view %d into the in-flight data, sequences %d to %d, for the view change in progress",
		len(restored), leaving.view, restored[0].seq, restored[len(restored)-1].seq)
	return nil
}

// validateRestored checks that every restored record is evidence about the proposal it is filed under: the
// proposal's metadata names the record's view and sequence, and the commit, when there is one, is this node's
// commit of that very proposal (same view and sequence, the proposal's digest). A proposal whose metadata does
// not match ends the restore there (it and what follows are dropped); a commit that does not match is dropped,
// leaving the proposal in phase PROPOSED. Both are logged: the WAL is this node's own, so they signal damage.
func (ps *PersistedState) validateRestored(restored []*restoredSlot, v *View) []*restoredSlot {
	for i, r := range restored {
		md := &protos.ViewMetadata{}
		if err := proto.Unmarshal(r.proposal.Metadata, md); err != nil || md.GetViewId() != r.view || md.GetLatestSequence() != r.seq {
			ps.Logger.Warnf("Dropping the proposal with sequence %d and what follows from the restore: its metadata names view %d sequence %d, the record view %d",
				r.seq, md.GetViewId(), md.GetLatestSequence(), r.view)
			return restored[:i]
		}
		if r.commit == nil {
			continue
		}
		cmt := r.commit.GetCommit()
		switch {
		case cmt.GetView() != r.view || cmt.GetSeq() != r.seq:
			ps.Logger.Warnf("Dropping the commit of sequence %d from the restore: it names view %d sequence %d", r.seq, cmt.GetView(), cmt.GetSeq())
			r.commit = nil
		case cmt.GetDigest() != "" && cmt.GetDigest() != r.proposal.Digest():
			ps.Logger.Warnf("Dropping the commit of sequence %d from the restore: its digest is not the proposal's", r.seq)
			r.commit = nil
		case v.SelfID != 0 && cmt.GetSignature().GetSigner() != v.SelfID:
			ps.Logger.Warnf("Dropping the commit of sequence %d from the restore: it was signed by %d, not by this node %d", r.seq, cmt.GetSignature().GetSigner(), v.SelfID)
			r.commit = nil
		}
	}
	return restored
}

// boundaryView is the view of the last new-view or view-change record, when there is one.
type boundaryView struct {
	found bool
	view  uint64
}

// unmarshalAfterBoundary decodes the WAL and returns the records after the last new-view or view-change
// record, with that record's view; every record must decode and be of a known kind.
func (ps *PersistedState) unmarshalAfterBoundary(entries [][]byte) ([]*protos.SavedMessage, boundaryView, error) {
	records := make([]*protos.SavedMessage, 0, len(entries))
	var boundary boundaryView
	for i, entry := range entries {
		record := &protos.SavedMessage{}
		if err := proto.Unmarshal(entry, record); err != nil {
			ps.Logger.Errorf("Failed unmarshaling entry %d of %d from WAL: %v", i+1, len(entries), err)
			return nil, boundary, fmt.Errorf("failed unmarshaling entry %d of %d from WAL: %w", i+1, len(entries), err)
		}
		switch {
		case record.GetNewView() != nil:
			boundary = boundaryView{found: true, view: record.GetNewView().GetViewId()}
			records = records[:0]
		case record.GetViewChange() != nil:
			boundary = boundaryView{found: true, view: record.GetViewChange().GetNextView()}
			records = records[:0]
		case record.GetProposedRecord() != nil, record.GetCommit() != nil:
			records = append(records, record)
		default:
			return nil, boundary, fmt.Errorf("unrecognized record: %v", record)
		}
	}
	return records, boundary, nil
}

// foldSlots groups the pre-prepare and commit records by sequence, in the order of the sequences.
func foldSlots(records []*protos.SavedMessage) ([]*restoredSlot, error) {
	bySeq := make(map[uint64]*restoredSlot)
	for _, record := range records {
		if proposed := record.GetProposedRecord(); proposed != nil {
			prePrepare := proposed.GetPrePrepare()
			prop := prePrepare.GetProposal()
			md := &protos.ViewMetadata{}
			if err := proto.Unmarshal(prop.GetMetadata(), md); err != nil {
				return nil, fmt.Errorf("failed to unmarshal the metadata of the proposal with sequence %d: %w", prePrepare.GetSeq(), err)
			}
			bySeq[prePrepare.GetSeq()] = &restoredSlot{
				seq:             prePrepare.GetSeq(),
				view:            prePrepare.GetView(),
				decisionsInView: md.GetDecisionsInView(),
				proposal: &types.Proposal{
					VerificationSequence: int64(prop.GetVerificationSequence()),
					Metadata:             prop.GetMetadata(),
					Payload:              prop.GetPayload(),
					Header:               prop.GetHeader(),
				},
				proposed: proposed,
			}
			continue
		}
		commitMsg := record.GetCommit()
		seq := commitMsg.GetCommit().GetSeq()
		slot, exists := bySeq[seq]
		if !exists {
			return nil, fmt.Errorf("the WAL holds a commit of sequence %d, but expected to also have a matching pre-prepare", seq)
		}
		slot.commit = commitMsg
	}
	slots := make([]*restoredSlot, 0, len(bySeq))
	for _, slot := range bySeq {
		slots = append(slots, slot)
	}
	slices.SortFunc(slots, func(a, b *restoredSlot) int { return cmp.Compare(a.seq, b.seq) })
	return slots, nil
}

// filterInFlight keeps the slots that are still in flight: above the checkpoint, within the pipeline
// depth above it, contiguous, and of the view of the boundary (or of the lowest slot).
func (ps *PersistedState) filterInFlight(slots []*restoredSlot, boundary boundaryView, v *View) ([]*restoredSlot, error) {
	checkpoint, known := ps.checkpointSequence(v)
	depth := v.depth()

	inFlight := make([]*restoredSlot, 0, len(slots))
	for _, slot := range slots {
		if known && slot.seq <= checkpoint {
			ps.Logger.Infof("Last proposal with sequence %d has been safely committed", slot.seq)
			continue
		}
		if known && slot.seq > checkpoint+depth {
			err := fmt.Errorf("last proposal sequence persisted into WAL is %d which is greater than last committed sequence %d plus the pipeline depth %d: "+
				"either the WAL is corrupt or PipelineDepthMax is smaller than it was when the node stopped; raise it to at least its previous value or clear the WAL", slot.seq, checkpoint, depth)
			ps.Logger.Errorf("Failed recovery: %s", err)
			return nil, err
		}
		inFlight = append(inFlight, slot)
	}
	if len(inFlight) == 0 {
		return nil, nil
	}

	view := inFlight[0].view
	if boundary.found {
		view = boundary.view
	}
	kept := make([]*restoredSlot, 0, len(inFlight))
	droppedForView := false
	for _, slot := range inFlight {
		if slot.view != view {
			ps.Logger.Warnf("Dropping the proposal with sequence %d of view %d from the restore: the WAL continues view %d", slot.seq, slot.view, view)
			droppedForView = true
			continue
		}
		kept = append(kept, slot)
	}
	if droppedForView {
		// What was dropped may have left a gap; the proposals of the view that are still contiguous from the
		// checkpoint are kept, the rest is dropped too: the node catches up on them by sync.
		contiguous := kept[:0]
		for i, slot := range kept {
			if (known && i == 0 && slot.seq != checkpoint+1) || (i > 0 && slot.seq != kept[i-1].seq+1) {
				ps.Logger.Warnf("Dropping the proposal with sequence %d and what follows from the restore: it does not continue the delivered sequences", slot.seq)
				break
			}
			contiguous = append(contiguous, slot)
		}
		return contiguous, nil
	}
	for i := 1; i < len(kept); i++ {
		if kept[i].seq != kept[i-1].seq+1 {
			return nil, fmt.Errorf("the proposals in flight in the WAL are not contiguous: sequence %d follows %d", kept[i].seq, kept[i-1].seq)
		}
	}
	if known && len(kept) > 0 && kept[0].seq != checkpoint+1 {
		return nil, fmt.Errorf("the lowest proposal in flight in the WAL has sequence %d but the last committed sequence is %d", kept[0].seq, checkpoint)
	}
	return kept, nil
}

// checkpointSequence is the sequence of the last proposal the application delivered, from the checkpoint
// when the view has one, else from the sequence the controller started the view with (checkpoint+1).
// It is unknown when neither is set, which only tests do.
func (ps *PersistedState) checkpointSequence(v *View) (seq uint64, known bool) {
	if v.RetrieveCheckpoint != nil {
		if prop, _ := v.RetrieveCheckpoint(); len(prop.GetMetadata()) > 0 {
			md := &protos.ViewMetadata{}
			if err := proto.Unmarshal(prop.GetMetadata(), md); err == nil {
				return md.GetLatestSequence(), true
			}
		}
	}
	if v.ProposalSequence > 0 {
		return v.ProposalSequence - 1, true
	}
	return 0, false
}
