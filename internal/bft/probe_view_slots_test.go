// probe: PipeBFT view-slots measurement. Not part of the product test-suite.
//
// A leader that bursts W consecutive pre-prepares (seq 0..W-1) at a follower
// that is still at ProposalSequence 0: how many pre-prepares reach a slot, how
// many are dropped, and by which path (view.go processPrePrepare channel-full
// vs. processMsg "not current/next" -> discoverIfSyncNeeded).

package bft_test

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
	"github.com/hyperledger/SmartBFT/pkg/types"
	protos "github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

func probePrePrepareFor(seq uint64) *protos.Message {
	if seq == 0 {
		return prePrepare // fixture: digest matches prepare/commit2/commit3
	}
	pp := proto.Clone(prePrepare).(*protos.Message)
	ppc := pp.GetPrePrepare()
	ppc.Seq = seq
	ppc.GetProposal().Payload = []byte{byte(seq)}
	ppc.GetProposal().Metadata = bft.MarshalOrPanic(&protos.ViewMetadata{
		ViewId:          1,
		LatestSequence:  seq,
		DecisionsInView: seq,
	})
	return pp
}

func TestProbePipelinedPrePrepareBurst(t *testing.T) {
	for _, w := range []int{1, 2, 3, 4, 8} {
		t.Run(fmt.Sprintf("W=%d", w), func(t *testing.T) {
			basicLog, err := zap.NewDevelopment()
			require.NoError(t, err)

			var logMu sync.Mutex
			var logged []string
			log := basicLog.WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
				logMu.Lock()
				logged = append(logged, entry.Message)
				logMu.Unlock()
				return nil
			})).Sugar()

			state := &bft.StateRecorder{}

			var decided atomic.Int32
			decider := &mocks.Decider{}
			decider.On("Decide", mock.Anything, mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
				decided.Add(1)
			})

			signer := &mocks.SignerMock{}
			signer.On("Sign", mock.Anything).Return([]byte{1, 2, 3})
			signer.On("SignProposal", mock.Anything, mock.Anything).Return(&types.Signature{Value: []byte{4}, ID: 3})

			var bcMu sync.Mutex
			preparesSentForSeq := map[uint64]int{}
			prepareSent := make(chan uint64, 64)
			commitSent := make(chan uint64, 64)
			comm := &mocks.CommMock{}
			comm.On("BroadcastConsensus", mock.Anything).Run(func(args mock.Arguments) {
				m := args.Get(0).(*protos.Message)
				if p := m.GetPrepare(); p != nil {
					bcMu.Lock()
					preparesSentForSeq[p.GetSeq()]++
					first := preparesSentForSeq[p.GetSeq()] == 1
					bcMu.Unlock()
					if first {
						prepareSent <- p.GetSeq()
					}
				}
				if c := m.GetCommit(); c != nil {
					select {
					case commitSent <- c.GetSeq():
					default:
					}
				}
			})

			verifier := &mocks.VerifierMock{}
			verifier.On("VerifyConsenterSig", mock.Anything, mock.Anything).Return(nil, nil)
			verifier.On("VerifySignature", mock.Anything).Return(nil)
			verifier.On("VerificationSequence").Return(uint64(1))

			view := &bft.View{
				RetrieveCheckpoint: (&types.Checkpoint{}).Get,
				Decider:            decider,
				Comm:               comm,
				Signer:             signer,
				Verifier:           verifier,
				State:              state,
				Logger:             log,
				N:                  4,
				NodesList:          []uint64{1, 2, 3, 4},
				LeaderID:           1,
				SelfID:             2,
				Quorum:             3,
				Number:             1,
				ProposalSequence:   0,
				ViewSequences:      &atomic.Value{},
				InMsgQSize:         40,
				MetricsView:        api.NewMetricsView(&disabled.Provider{}),
			}

			// The burst: while the view is inside verifyProposal(seq 0) (synchronous,
			// not draining incMsgs) the leader's seq 1..W-1 pre-prepares queue up in
			// incMsgs and are dispatched by processMsg from the processPrepares loop.
			var burstOnce sync.Once
			verifier.On("VerifyProposal", mock.Anything).Run(func(_ mock.Arguments) {
				burstOnce.Do(func() {
					for s := 1; s < w; s++ {
						view.HandleMessage(1, probePrePrepareFor(uint64(s)))
					}
				})
			}).Return(nil, nil)

			view.Start()
			view.HandleMessage(1, probePrePrepareFor(0))

			waitSeq := func(ch chan uint64, want uint64, what string) {
				select {
				case got := <-ch:
					require.Equal(t, want, got, what)
				case <-time.After(5 * time.Second):
					t.Fatalf("timeout waiting for %s seq %d", what, want)
				}
			}
			waitSeq(prepareSent, 0, "prepare")
			view.HandleMessage(3, prepare)
			view.HandleMessage(4, prepare)
			waitSeq(commitSent, 0, "commit")
			view.HandleMessage(3, commit3)
			view.HandleMessage(4, proto.Clone(commit3).(*protos.Message)) // signer 3 again -> rejected (sender mismatch)
			c4 := proto.Clone(commit3).(*protos.Message)
			c4.GetCommit().GetSignature().Signer = 4
			view.HandleMessage(4, c4)

			require.Eventually(t, func() bool { return decided.Load() == 1 }, 5*time.Second, 10*time.Millisecond)

			nextSlotUsed := false
			if w >= 2 {
				// seq 1 was buffered in nextPrePrepare and is swapped in by startNextSeq.
				select {
				case got := <-prepareSent:
					require.Equal(t, uint64(1), got)
					nextSlotUsed = true
				case <-time.After(2 * time.Second):
				}
			}
			// Give the loop a moment: anything for seq>=2 that survived would be
			// prepared now (it cannot - it was discarded - but we check).
			time.Sleep(300 * time.Millisecond)
			view.Abort()

			logMu.Lock()
			defer logMu.Unlock()
			var chanFullDrops, notCurrentOrNext int
			for _, m := range logged {
				if strings.Contains(m, "without processing previous one, dropping message") {
					chanFullDrops++
				}
				if strings.Contains(m, "but our sequence is") {
					notCurrentOrNext++
				}
			}
			bcMu.Lock()
			prepared := len(preparesSentForSeq)
			bcMu.Unlock()

			t.Logf("PROBE W=%d: pre-prepares sent=%d, slots filled(prepared)=%d (next-slot used=%v), "+
				"dropped via processPrePrepare channel-full=%d, dropped via processMsg not-current/next (discoverIfSyncNeeded)=%d, decided=%d",
				w, w, prepared, nextSlotUsed, chanFullDrops, notCurrentOrNext, decided.Load())

			require.Equal(t, 0, chanFullDrops, "channel-full drop path should not fire for distinct seqs")
			expectedLost := w - 2
			if expectedLost < 0 {
				expectedLost = 0
			}
			require.Equal(t, expectedLost, notCurrentOrNext)
			require.Equal(t, min(w, 2), prepared)
		})
	}
}
