// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// These tests cover the view change with several proposals in flight (design-viewchange-multi-inflight.md
// sections 4-6, 14.1, 14.5): the pipelined view data, its validation, the per-sequence prefix check and the
// refusal of view data from a node with another pipeline depth.

// windowProposalAt builds a proposal of the given sequence, in view 0, with a payload that tells it apart.
func windowProposalAt(seq uint64, tag byte) *protos.Proposal {
	return &protos.Proposal{
		Header:               []byte{0},
		Payload:              []byte{tag},
		Metadata:             bft.MarshalOrPanic(&protos.ViewMetadata{ViewId: 0, LatestSequence: seq}),
		VerificationSequence: 1,
	}
}

func entry(p *protos.Proposal, prepared bool) *protos.InFlightEntry {
	return &protos.InFlightEntry{Proposal: p, Prepared: prepared}
}

// pipelinedViewData is a view data of a node with a pipeline: last decision at sequence 1 (the shared fixture),
// listing the given entries, the single fields repeating the first.
func pipelinedViewData(entries ...*protos.InFlightEntry) *protos.ViewData {
	v := proto.Clone(vd).(*protos.ViewData)
	v.InFlight = entries
	if len(entries) > 0 {
		v.InFlightProposal, v.InFlightPrepared = entries[0].GetProposal(), entries[0].GetPrepared()
	}
	return v
}

func TestCheckInFlightPrefix(t *testing.T) {
	p2, p3, p4 := windowProposalAt(2, 2), windowProposalAt(3, 3), windowProposalAt(4, 4)
	q3 := windowProposalAt(3, 33) // another proposal of sequence 3
	const f, quorum, depth = 1, 3, 3

	for _, tc := range []struct {
		description string
		messages    []*protos.ViewData
		ok          bool
		prefix      []*protos.Proposal
	}{
		{
			description: "nothing in flight anywhere: empty prefix",
			messages:    []*protos.ViewData{pipelinedViewData(), pipelinedViewData(), pipelinedViewData(), pipelinedViewData()},
			ok:          true,
		},
		{
			description: "three prepared everywhere: the whole window is kept",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, true)),
				pipelinedViewData(),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2, p3, p4},
		},
		{
			description: "the second is prepared by f+1 only, the third by none: prefix of two (B at 4)",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, false)),
				pipelinedViewData(entry(p2, true), entry(p3, true)),
				pipelinedViewData(entry(p2, true), entry(p3, false), entry(p4, false)),
				pipelinedViewData(entry(p2, false)),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2, p3},
		},
		{
			description: "an unprepared report of the same proposal counts as pre-prepared (A2), as the sequential check did: the whole window is kept",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(p3, false), entry(p4, true)),
				pipelinedViewData(entry(p2, true), entry(p3, false), entry(p4, true)),
				pipelinedViewData(entry(p2, true), entry(p3, false), entry(p4, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, true)),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2, p3, p4},
		},
		{
			description: "B in the middle: a lone node prepared beyond what the quorum saw; the prefix ends at 2 and its 3 and 4 are the discarded suffix (D1)",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true), entry(p4, true)),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2},
		},
		{
			description: "a byzantine node alone reports a prepared proposal: not A (f+1 needed), B holds, liveness kept",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(q3, true)),
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, true)),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2},
		},
		{
			description: "two prepared for p3, one for q3, one nothing: A for p3 (A1: 3 report p3 or nothing)",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(p3, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true)),
				pipelinedViewData(entry(p2, true), entry(q3, true)),
				pipelinedViewData(entry(p2, true)),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2, p3},
		},
		{
			description: "neither A nor B: two prepared for p3, two for q3 (impossible with honest nodes) -> not decided, retry (S3)",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true), entry(p3, true)),
				pipelinedViewData(entry(p2, true), entry(p3, true)),
				pipelinedViewData(entry(p2, true), entry(q3, true)),
				pipelinedViewData(entry(p2, true), entry(q3, true)),
			},
			ok: false,
		},
		{
			description: "one prepared, two pre-prepared, one nothing: A (pre-prepared by f+1, nobody argues), the sequence is kept",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(entry(p2, false)),
				pipelinedViewData(entry(p2, false)),
				pipelinedViewData(),
			},
			ok:     true,
			prefix: []*protos.Proposal{p2},
		},
		{
			description: "one prepared, three nothing: not A (pre-prepared by one), B holds, empty prefix",
			messages: []*protos.ViewData{
				pipelinedViewData(entry(p2, true)),
				pipelinedViewData(),
				pipelinedViewData(),
				pipelinedViewData(),
			},
			ok: true,
		},
		{
			description: "sequential view data (single fields) are read like a one-entry list",
			messages: func() []*protos.ViewData {
				single := func(p *protos.Proposal, prepared bool) *protos.ViewData {
					v := proto.Clone(vd).(*protos.ViewData)
					v.InFlightProposal, v.InFlightPrepared = p, prepared
					return v
				}
				return []*protos.ViewData{single(p2, true), single(p2, true), single(p2, false), single(nil, false)}
			}(),
			ok:     true,
			prefix: []*protos.Proposal{p2},
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			ok, prefix, err := bft.CheckInFlightPrefix(viewDataBySender(tc.messages), f, quorum, depth)
			require.NoError(t, err)
			require.Equal(t, tc.ok, ok)
			require.Len(t, prefix, len(tc.prefix))
			for i := range tc.prefix {
				require.True(t, proto.Equal(tc.prefix[i], prefix[i]), "prefix entry %d", i)
			}
		})
	}

	t.Run("depth one is the sequential check", func(t *testing.T) {
		messages := []*protos.ViewData{
			pipelinedViewData(entry(p2, true), entry(p3, true)),
			pipelinedViewData(entry(p2, true), entry(p3, true)),
			pipelinedViewData(entry(p2, true), entry(p3, true)),
			pipelinedViewData(),
		}
		ok, prefix, err := bft.CheckInFlightPrefix(viewDataBySender(messages), f, quorum, 1)
		require.NoError(t, err)
		require.True(t, ok)
		require.Len(t, prefix, 1, "a depth of one looks at the sequence after the last decision only")
		okSingle, noInFlight, proposal, err := bft.CheckInFlight(viewDataBySender(messages), f, quorum, 4, nil)
		require.NoError(t, err)
		require.True(t, okSingle)
		require.False(t, noInFlight)
		require.True(t, proto.Equal(p2, proposal))
	})
}

func TestValidateInFlightEntries(t *testing.T) {
	p2, p3, p5 := windowProposalAt(2, 2), windowProposalAt(3, 3), windowProposalAt(5, 5)
	single := func(p *protos.Proposal, prepared bool) *protos.ViewData {
		v := proto.Clone(vd).(*protos.ViewData)
		v.InFlightProposal, v.InFlightPrepared = p, prepared
		return v
	}
	for _, tc := range []struct {
		description string
		viewData    *protos.ViewData
		depth       uint64
		err         string
	}{
		{"depth 1, sequential view data with nothing", single(nil, false), 1, ""},
		{"depth 1, sequential view data with the next sequence", single(p2, true), 1, ""},
		{"depth 1, sequential view data with a wrong sequence", single(p3, true), 1, "the in flight proposal sequence is 3 while the last decision sequence is 1"},
		{"depth 1 refuses a pipelined list (mixed deployment)", pipelinedViewData(entry(p2, true)), 1, "lists 1 proposals in flight of a pipelined node, but this node runs without a pipeline"},
		{"depth 3, nothing in flight", pipelinedViewData(), 3, ""},
		{"depth 3, contiguous list of two", pipelinedViewData(entry(p2, true), entry(p3, false)), 3, ""},
		{"depth 3 refuses a single proposal without the list (mixed deployment)", single(p2, true), 3, "carries a single in flight proposal without the pipelined list"},
		{"depth 3, a gap", pipelinedViewData(entry(p2, true), entry(p5, false)), 3, "in flight entry 1 has sequence 5 while 3 is expected after the last decision sequence 1"},
		{"depth 3, does not start after the last decision", pipelinedViewData(entry(p3, true)), 3, "in flight entry 0 has sequence 3 while 2 is expected"},
		{"depth 2, too long", pipelinedViewData(entry(p2, true), entry(p3, true), entry(windowProposalAt(4, 4), true)), 2, "lists 3 proposals in flight while the pipeline depth is 2"},
		{"depth 3, the single fields do not repeat the first entry", func() *protos.ViewData {
			v := pipelinedViewData(entry(p2, true), entry(p3, true))
			v.InFlightPrepared = false
			return v
		}(), 3, "the first in flight entry differs from the single in flight proposal fields"},
		{"depth 3, an entry without metadata", pipelinedViewData(entry(&protos.Proposal{}, true)), 3, "in flight entry 0 has no proposal metadata"},
	} {
		t.Run(tc.description, func(t *testing.T) {
			err := bft.ValidateInFlightEntries(tc.viewData, 1, tc.depth)
			if tc.err == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.err)
		})
	}
}

// TestPrepareViewDataMultiInFlight covers what a pipelined node reports (section 4): every proposal above the
// checkpoint, contiguous from the sequence after it, the single fields repeating the first; and that a node
// without a pipeline reports exactly what it always did, with the list empty (W=1 bytes unchanged).
func TestPrepareViewDataMultiInFlight(t *testing.T) {
	proposalAt := func(seq uint64) types.Proposal {
		p := windowProposalAt(seq, byte(seq))
		return types.Proposal{Header: p.GetHeader(), Payload: p.GetPayload(), Metadata: p.GetMetadata(), VerificationSequence: int64(p.GetVerificationSequence())}
	}
	for _, tc := range []struct {
		description string
		depth       uint64
		inFlight    func() *bft.InFlightData
		expectList  []uint64 // sequences in the list, in order
		expectFirst uint64   // sequence of the single field, 0 for none
	}{
		{
			description: "depth 3: a delivered one, then two contiguous, then a gap",
			depth:       3,
			inFlight: func() *bft.InFlightData {
				ifp := &bft.InFlightData{}
				ifp.StoreProposal(lastDecision) // sequence 1: delivered, not in flight
				ifp.StoreProposal(proposalAt(2))
				ifp.StoreProposal(proposalAt(3))
				ifp.StorePrepares(0, 3)
				ifp.StoreProposal(proposalAt(5)) // a gap before it: not reported
				return ifp
			},
			expectList:  []uint64{2, 3},
			expectFirst: 2,
		},
		{
			description: "depth 3: nothing above the checkpoint",
			depth:       3,
			inFlight: func() *bft.InFlightData {
				ifp := &bft.InFlightData{}
				ifp.StoreProposal(lastDecision)
				return ifp
			},
		},
		{
			description: "depth 1: the single fields only, as before",
			depth:       1,
			inFlight: func() *bft.InFlightData {
				ifp := &bft.InFlightData{}
				ifp.StoreProposal(proposalAt(2))
				return ifp
			},
			expectFirst: 2,
		},
	} {
		t.Run(tc.description, func(t *testing.T) {
			comm := &mocks.CommMock{}
			broadcastChan := make(chan *protos.Message, 10)
			comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
				broadcastChan <- args.Get(0).(*protos.Message)
			})
			sendChan := make(chan *protos.Message, 10)
			comm.On("SendConsensus", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
				sendChan <- args.Get(1).(*protos.Message)
			})
			signer := &mocks.SignerMock{}
			signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)
			reqTimer := &mocks.RequestsTimer{}
			reqTimer.On("StopTimers")
			controller := &mocks.ViewController{}
			controller.On("AbortView", mock.Anything)
			checkpoint := types.Checkpoint{}
			checkpoint.Set(lastDecision, lastDecisionSignatures)
			state := &mocks.State{}
			state.On("Save", mock.Anything, false).Return(nil)

			vc := &bft.ViewChanger{
				SelfID:        0,
				N:             4,
				NodesList:     []uint64{0, 1, 2, 3},
				Comm:          comm,
				Signer:        signer,
				Logger:        basicLog.Sugar(),
				RequestsTimer: reqTimer,
				Ticker:        make(chan time.Time),
				InFlight:      tc.inFlight(),
				Checkpoint:    &checkpoint,
				Controller:    controller,
				InMsqQSize:    100,
				State:         state,
				PipelineDepth: tc.depth,
			}
			vc.Start(0)
			defer vc.Stop()
			vc.HandleMessage(1, viewChangeMsg)
			vc.HandleMessage(2, viewChangeMsg)
			require.NotNil(t, (<-broadcastChan).GetViewChange())
			m := <-sendChan
			viewData := &protos.ViewData{}
			require.NoError(t, proto.Unmarshal(m.GetViewData().GetRawViewData(), viewData))

			require.Len(t, viewData.GetInFlight(), len(tc.expectList))
			for i, seq := range tc.expectList {
				md := &protos.ViewMetadata{}
				require.NoError(t, proto.Unmarshal(viewData.GetInFlight()[i].GetProposal().GetMetadata(), md))
				require.Equal(t, seq, md.GetLatestSequence())
				require.Equal(t, seq == 3, viewData.GetInFlight()[i].GetPrepared())
			}
			if tc.expectFirst == 0 {
				require.Nil(t, viewData.GetInFlightProposal())
			} else {
				md := &protos.ViewMetadata{}
				require.NoError(t, proto.Unmarshal(viewData.GetInFlightProposal().GetMetadata(), md))
				require.Equal(t, tc.expectFirst, md.GetLatestSequence())
			}
			require.NoError(t, bft.ValidateInFlightEntries(viewData, 1, tc.depth), "what is sent must pass the receiver's check")
		})
	}
}
