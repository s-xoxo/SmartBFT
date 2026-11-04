// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// gaugeProvider is a disabled provider whose gauges record the last value set, for the one gauge under test.
type gaugeProvider struct {
	disabled.Provider
	value float64
}

func (p *gaugeProvider) NewGauge(metrics.GaugeOpts) metrics.Gauge { return &recordingGauge{p: p} }

type recordingGauge struct {
	p *gaugeProvider
}

func (g *recordingGauge) With(...string) metrics.Gauge { return g }
func (g *recordingGauge) Add(delta float64)            { g.p.value += delta }
func (g *recordingGauge) Set(value float64)            { g.p.value = value }

// These tests cover the restore of several proposals in flight (design-state-wal-window.md section 5): the
// fold of the WAL by sequence, the view-change boundary, the checkpoint filter, the depth bound, and the
// records of another view.

const walView = 7

func walProposed(seq, view, decisions uint64) []byte {
	return bft.MarshalOrPanic(newProposalRecord(seq, view, &protos.ViewMetadata{
		ViewId: view, LatestSequence: seq, DecisionsInView: decisions,
	}))
}

// walCommit is the commit record of the given sequence in the view the tests use (walView).
func walCommit(seq uint64) []byte { return bft.MarshalOrPanic(newCommitRecord(seq, walView)) }

func walNewView(view, seq uint64) []byte { return bft.MarshalOrPanic(newViewRecord(view, seq)) }

func walViewChange(nextView uint64) []byte {
	return bft.MarshalOrPanic(&protos.SavedMessage{Content: &protos.SavedMessage_ViewChange{
		ViewChange: &protos.ViewChange{NextView: nextView},
	}})
}

// checkpointAt returns a checkpoint retriever reporting the given last delivered sequence; 0 means none.
func checkpointAt(seq uint64) bft.CheckpointRetriever {
	cp := &types.Checkpoint{}
	if seq > 0 {
		cp.Set(types.Proposal{Metadata: bft.MarshalOrPanic(&protos.ViewMetadata{LatestSequence: seq})}, nil)
	}
	return cp.Get
}

type restoredView struct {
	phase     bft.Phase
	view, seq uint64
	decisions uint64
	restored  uint64
	// what the in-flight data reports: the lowest proposal and whether it is prepared
	inFlightSeq      uint64
	inFlightPrepared bool
}

func restoreFromWAL(t *testing.T, entries [][]byte, checkpoint, depth uint64) (restoredView, []string, error) {
	t.Helper()
	logged := make([]string, 0)
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if entry.Level >= zapcore.InfoLevel {
			logged = append(logged, entry.Message)
		}
		return nil
	})).Sugar()

	inFlight := &bft.InFlightData{}
	state := &bft.PersistedState{Entries: entries, Logger: log, InFlightProposal: inFlight}
	verifier := &mocks.VerifierMock{}
	verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
	view := &bft.View{
		Number:             7,
		ProposalSequence:   checkpoint + 1,
		RetrieveCheckpoint: checkpointAt(checkpoint),
		PipelineDepth:      depth,
		SelfID:             1,
		LeaderID:           1,
		MetricsView:        api.NewMetricsView(&disabled.Provider{}),
		Verifier:           verifier,
	}
	err = state.Restore(view)
	result := restoredView{phase: view.Phase, view: view.Number, seq: view.ProposalSequence, decisions: view.DecisionsInView, restored: view.RestoredCount()}
	if prop := inFlight.InFlightProposal(); prop != nil {
		md := &protos.ViewMetadata{}
		require.NoError(t, proto.Unmarshal(prop.Metadata, md))
		result.inFlightSeq = md.GetLatestSequence()
		result.inFlightPrepared = inFlight.IsInFlightPrepared()
	}
	return result, logged, err
}

func TestRestoreFoldsWindow(t *testing.T) {
	const view, depth = walView, 3
	for _, tc := range []struct {
		description string
		entries     [][]byte
		checkpoint  uint64
		expected    restoredView
		logged      []string
	}{
		{
			description: "two proposed, the lowest prepared, interleaved",
			entries:     [][]byte{walProposed(11, view, 4), walProposed(12, view, 5), walCommit(11)},
			checkpoint:  10,
			expected:    restoredView{phase: bft.PREPARED, view: view, seq: 11, decisions: 4, restored: 2, inFlightSeq: 11, inFlightPrepared: true},
			logged:      []string{"Restored proposal with sequence 11", "Restored proposal with sequence 12", "Restored 2 proposals in flight, sequences 11 to 12"},
		},
		{
			description: "three proposed, none prepared",
			entries:     [][]byte{walProposed(11, view, 4), walProposed(12, view, 5), walProposed(13, view, 6)},
			checkpoint:  10,
			expected:    restoredView{phase: bft.PROPOSED, view: view, seq: 11, decisions: 4, restored: 3, inFlightSeq: 11},
		},
		{
			description: "the lowest was delivered before the restart: the checkpoint filter drops it",
			entries:     [][]byte{walProposed(11, view, 4), walCommit(11), walProposed(12, view, 5), walCommit(12)},
			checkpoint:  11,
			expected:    restoredView{phase: bft.PREPARED, view: view, seq: 12, decisions: 5, restored: 1, inFlightSeq: 12, inFlightPrepared: true},
			logged:      []string{"Last proposal with sequence 11 has been safely committed", "Restored proposal with sequence 12"},
		},
		{
			description: "everything was delivered: nothing in flight",
			entries:     [][]byte{walProposed(11, view, 4), walProposed(12, view, 5), walCommit(11), walCommit(12)},
			checkpoint:  12,
			expected:    restoredView{phase: bft.COMMITTED, view: view, seq: 13},
			logged:      []string{"Every proposal in the WAL was delivered already, nothing in flight to restore"},
		},
		{
			description: "a stale pre-prepare below the checkpoint is dropped, not restored (the baseline restored it)",
			entries:     [][]byte{walProposed(11, view, 4)},
			checkpoint:  13,
			expected:    restoredView{phase: bft.COMMITTED, view: view, seq: 14},
			logged:      []string{"Last proposal with sequence 11 has been safely committed"},
		},
		{
			description: "records before a new-view record belong to the abandoned view (D1)",
			entries:     [][]byte{walProposed(11, view-1, 4), walProposed(12, view-1, 5), walNewView(view, 11), walProposed(12, view, 0)},
			checkpoint:  11,
			expected:    restoredView{phase: bft.PROPOSED, view: view, seq: 12, decisions: 0, restored: 1, inFlightSeq: 12},
		},
		{
			description: "a new-view record last: nothing to restore, as before",
			entries:     [][]byte{walProposed(11, view-1, 4), walProposed(12, view-1, 5), walNewView(view, 11)},
			checkpoint:  11,
			expected:    restoredView{phase: bft.COMMITTED, view: view, seq: 12},
			logged:      []string{"last entry in WAL is a newView record"},
		},
		{
			description: "a view-change record last: nothing to restore, as before",
			entries:     [][]byte{walProposed(11, view, 4), walViewChange(view + 1)},
			checkpoint:  10,
			expected:    restoredView{phase: bft.COMMITTED, view: view, seq: 11},
			logged:      []string{"last entry in WAL is a viewChange message"},
		},
		{
			description: "a record of another view than the boundary is dropped with a warning",
			entries:     [][]byte{walNewView(view, 10), walProposed(11, view, 0), walProposed(12, view+1, 1)},
			checkpoint:  10,
			expected:    restoredView{phase: bft.PROPOSED, view: view, seq: 11, decisions: 0, restored: 1, inFlightSeq: 11},
			logged:      []string{"Dropping the proposal with sequence 12 of view 8 from the restore: the WAL continues view 7"},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			result, logged, err := restoreFromWAL(t, tc.entries, tc.checkpoint, depth)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
			for _, expected := range tc.logged {
				require.Contains(t, logged, expected)
			}
		})
	}
}

func TestRestoreErrors(t *testing.T) {
	const view = walView
	for _, tc := range []struct {
		description string
		entries     [][]byte
		checkpoint  uint64
		depth       uint64
		expected    string
	}{
		{
			description: "a commit without its pre-prepare",
			entries:     [][]byte{walProposed(11, view, 4), walCommit(12)},
			checkpoint:  10, depth: 3,
			expected: "the WAL holds a commit of sequence 12, but expected to also have a matching pre-prepare",
		},
		{
			description: "a sequence beyond the depth: the WAL is ahead or the depth was lowered",
			entries:     [][]byte{walProposed(11, view, 4), walProposed(12, view, 5), walProposed(13, view, 6)},
			checkpoint:  10, depth: 2,
			expected: "last proposal sequence persisted into WAL is 13 which is greater than last committed sequence 10 plus the pipeline depth 2: " +
				"either the WAL is corrupt or PipelineDepthMax is smaller than it was when the node stopped",
		},
		{
			description: "a gap between the proposals in flight",
			entries:     [][]byte{walProposed(11, view, 4), walProposed(13, view, 6)},
			checkpoint:  10, depth: 3,
			expected: "the proposals in flight in the WAL are not contiguous: sequence 13 follows 11",
		},
		{
			description: "the lowest proposal in flight does not follow the checkpoint",
			entries:     [][]byte{walProposed(12, view, 5)},
			checkpoint:  10, depth: 3,
			expected: "the lowest proposal in flight in the WAL has sequence 12 but the last committed sequence is 10",
		},
		{
			description: "a malformed record in the middle",
			entries:     [][]byte{walProposed(11, view, 4), {1, 2, 3}, walProposed(12, view, 5)},
			checkpoint:  10, depth: 3,
			expected: "failed unmarshaling entry 2 of 3 from WAL",
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			_, _, err := restoreFromWAL(t, tc.entries, tc.checkpoint, tc.depth)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expected)
		})
	}
}

// TestRestoreDepthOneMatchesBaseline runs the shapes a sequential WAL can have through the fold: the result
// is the one the sequential restore produced (design-state-wal-window.md section 8), the stale pre-prepare
// row excepted, which is covered by TestRestoreFoldsWindow.
func TestRestoreDepthOneMatchesBaseline(t *testing.T) {
	const view = walView
	for _, tc := range []struct {
		description string
		entries     [][]byte
		checkpoint  uint64
		expected    restoredView
	}{
		{"proposed", [][]byte{walProposed(11, view, 4)}, 10, restoredView{phase: bft.PROPOSED, view: view, seq: 11, decisions: 4, restored: 1, inFlightSeq: 11}},
		{"prepared", [][]byte{walProposed(11, view, 4), walCommit(11)}, 10, restoredView{phase: bft.PREPARED, view: view, seq: 11, decisions: 4, restored: 1, inFlightSeq: 11, inFlightPrepared: true}},
		{"prepared and committed", [][]byte{walProposed(11, view, 4), walCommit(11)}, 11, restoredView{phase: bft.COMMITTED, view: view, seq: 12}},
		{"new view last", [][]byte{walProposed(11, view, 4), walNewView(view+1, 11)}, 11, restoredView{phase: bft.COMMITTED, view: view, seq: 12}},
	} {
		t.Run(tc.description, func(t *testing.T) {
			result, _, err := restoreFromWAL(t, tc.entries, tc.checkpoint, 1)
			require.NoError(t, err)
			require.Equal(t, tc.expected, result)
		})
	}
	t.Run("WAL ahead of the checkpoint", func(t *testing.T) {
		_, _, err := restoreFromWAL(t, [][]byte{walProposed(12, view, 5), walCommit(12)}, 10, 1)
		require.ErrorContains(t, err, "last proposal sequence persisted into WAL is 12 which is greater than last committed sequence 10 plus the pipeline depth 1")
	})
}

// TestSaveCountsRecordsSinceBoundary covers the wal_records_since_boundary gauge: a truncating record resets
// it to one, the others add one, and Restore starts it from what the WAL held.
func TestSaveCountsRecordsSinceBoundary(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	provider := &gaugeProvider{}
	state := &bft.PersistedState{
		InFlightProposal: &bft.InFlightData{},
		Logger:           basicLog.Sugar(),
		WAL:              &recordingWAL{},
		Metrics:          api.NewMetricsState(provider),
	}
	require.NoError(t, state.Save(newProposalRecord(11, 7, &protos.ViewMetadata{ViewId: 7}), true))
	require.InDelta(t, 1.0, provider.value, 0)
	require.NoError(t, state.Save(newProposalRecord(12, 7, &protos.ViewMetadata{ViewId: 7}), false))
	require.NoError(t, state.Save(newCommitRecord(11, 7), false))
	require.InDelta(t, 3.0, provider.value, 0)
	require.NoError(t, state.Save(newProposalRecord(13, 7, &protos.ViewMetadata{ViewId: 7}), true))
	require.InDelta(t, 1.0, provider.value, 0)

	state.Entries = [][]byte{walProposed(11, 7, 4), walProposed(12, 7, 5), walCommit(11)}
	verifier := &mocks.VerifierMock{}
	verifier.On("RequestsFromProposal", mock.Anything).Return(nil)
	require.NoError(t, state.Restore(&bft.View{ProposalSequence: 11, PipelineDepth: 3, MetricsView: api.NewMetricsView(&disabled.Provider{}), Verifier: verifier}))
	require.InDelta(t, 3.0, provider.value, 0)
}
