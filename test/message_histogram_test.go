// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
)

// TestMessageHistogramMatchesBaseline checks that, with the default configuration (PipelineDepthMax = 1), the
// nodes send the same number of consensus messages of every kind as the original SmartBFT (the `baseline`
// tag) in two scenarios: a single request delivered by four nodes (TestBasic-like) and
// TestNodeInFlightThenViewChange (a lost commit, a view change, a node that rejoins and syncs, a second view
// change). Messages are counted on the sender side with the MutateSend hook, per kind, on every node.
//
// The golden file testdata/message_histogram_baseline.json was recorded on the `baseline` tag, with this
// test file copied there unchanged (it uses only harness APIs that exist at that tag):
//
//	git worktree add --detach ../wt-baseline baseline
//	cp test/message_histogram_test.go ../wt-baseline/test/
//	(cd ../wt-baseline && SMARTBFT_RECORD_HISTOGRAM=5 go test ./test -run TestMessageHistogramMatchesBaseline -count=1)
//	cp ../wt-baseline/test/testdata/message_histogram_baseline.json test/testdata/
//	git worktree remove --force ../wt-baseline
//
// Record mode runs every scenario SMARTBFT_RECORD_HISTOGRAM times and stores the minimum and maximum of every
// kind. Check mode (the default) runs every scenario once and compares:
//   - consensus and view-change messages (pre-prepare, prepare, commit, view-change, view-data, new-view) and
//     state transfer messages are compared exactly when the recorded runs agreed on their count, and within
//     the recorded range, widened by rangeSlack, when they did not. The first scenario is exact (one proposal:
//     3 pre-prepares, 12 prepares, 12 commits). In the second scenario the proposals, prepares, commits,
//     view-data and new-view messages are exact and the view-change messages (resent on a timer) vary;
//   - state transfer requests and responses are always compared within the recorded range widened by rangeSlack:
//     a node syncs when a heartbeat or a late commit shows that it is behind, and a request to a node that is cut
//     off at that moment gets no response, so their counts follow the timing of the scenario (the first scenario
//     has one round at start, 12 requests and 12 responses, and the range is as good as exact there);
//   - the prepares and commits a node sends back, as assist messages, to a node whose message arrived for a
//     sequence it already delivered (prepare_assist, commit_assist), and the heartbeats and heartbeat responses
//     (the 1 s scheduler tick against a 6 s heartbeat period), depend on timing, so they are only bounded by
//     the recorded maximum plus timingSlack. They are counted apart from the prepares and commits every node
//     broadcasts exactly once per sequence.
//
// A count outside what was recorded means the message flow differs from the original.
func TestMessageHistogramMatchesBaseline(t *testing.T) {
	t.Parallel()
	goldenPath := filepath.Join("testdata", "message_histogram_baseline.json")

	scenarios := []struct {
		name string
		run  func(t *testing.T, h *messageHistogram)
	}{
		{name: "basic", run: histogramBasicScenario},
		{name: "in_flight_then_view_change", run: histogramInFlightThenViewChangeScenario},
	}

	if runs, _ := strconv.Atoi(os.Getenv("SMARTBFT_RECORD_HISTOGRAM")); runs > 0 {
		golden := messageHistogramGolden{Runs: runs, Scenarios: make(map[string]map[string]countRange)}
		for _, scenario := range scenarios {
			ranges := make(map[string]countRange)
			for run := range runs {
				h := &messageHistogram{}
				t.Run(fmt.Sprintf("record/%s/%d", scenario.name, run), func(t *testing.T) { scenario.run(t, h) })
				for kind, count := range h.snapshot() {
					r, seen := ranges[kind]
					if !seen {
						r = countRange{Min: count, Max: count}
					}
					ranges[kind] = countRange{Min: min(r.Min, count), Max: max(r.Max, count)}
				}
			}
			golden.Scenarios[scenario.name] = ranges
		}
		data, err := json.MarshalIndent(golden, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(goldenPath), 0o755))
		require.NoError(t, os.WriteFile(goldenPath, append(data, '\n'), 0o644))
		t.Logf("recorded %d runs per scenario into %s:\n%s", runs, goldenPath, data)
		return
	}

	data, err := os.ReadFile(goldenPath)
	require.NoError(t, err, "the golden file is missing; record it on the baseline tag as described in the test comment")
	golden := messageHistogramGolden{}
	require.NoError(t, json.Unmarshal(data, &golden))

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			recorded, exists := golden.Scenarios[scenario.name]
			require.True(t, exists, "the golden file has no scenario %q", scenario.name)
			h := &messageHistogram{}
			scenario.run(t, h)
			counts := h.snapshot()
			t.Logf("%s: %v", scenario.name, counts)
			for _, kind := range messageKindNames {
				count := counts[kind]
				r := recorded[kind]
				switch kind {
				case "prepare_assist", "commit_assist", "heart_beat", "heart_beat_response":
					require.LessOrEqual(t, count, r.Max+timingSlack, "%s: %d %s messages, the baseline sent at most %d", scenario.name, count, kind, r.Max)
				case "state_transfer_request", "state_transfer_response":
					require.GreaterOrEqual(t, count, r.Min-rangeSlack, "%s: %d %s messages, the baseline sent %d to %d", scenario.name, count, kind, r.Min, r.Max)
					require.LessOrEqual(t, count, r.Max+rangeSlack, "%s: %d %s messages, the baseline sent %d to %d", scenario.name, count, kind, r.Min, r.Max)
				default:
					if r.Min == r.Max {
						require.Equal(t, r.Min, count, "%s: %d %s messages, the baseline sent %d", scenario.name, count, kind, r.Min)
					} else {
						require.GreaterOrEqual(t, count, r.Min-rangeSlack, "%s: %d %s messages, the baseline sent %d to %d", scenario.name, count, kind, r.Min, r.Max)
						require.LessOrEqual(t, count, r.Max+rangeSlack, "%s: %d %s messages, the baseline sent %d to %d", scenario.name, count, kind, r.Min, r.Max)
					}
				}
			}
		})
	}
}

const (
	timingSlack = int64(3)
	rangeSlack  = int64(3)
)

type countRange struct {
	Min int64 `json:"min"`
	Max int64 `json:"max"`
}

type messageHistogramGolden struct {
	Runs      int                              `json:"runs"`
	Scenarios map[string]map[string]countRange `json:"scenarios"`
}

var messageKindNames = []string{
	"pre_prepare", "prepare", "commit",
	"prepare_assist", "commit_assist",
	"heart_beat", "heart_beat_response",
	"view_change", "view_data", "new_view",
	"state_transfer_request", "state_transfer_response",
	"other",
}

func messageKindOf(m *smartbftprotos.Message) int {
	switch m.GetContent().(type) {
	case *smartbftprotos.Message_PrePrepare:
		return 0
	case *smartbftprotos.Message_Prepare:
		if m.GetPrepare().GetAssist() {
			return 3
		}
		return 1
	case *smartbftprotos.Message_Commit:
		if m.GetCommit().GetAssist() {
			return 4
		}
		return 2
	case *smartbftprotos.Message_HeartBeat:
		return 5
	case *smartbftprotos.Message_HeartBeatResponse:
		return 6
	case *smartbftprotos.Message_ViewChange:
		return 7
	case *smartbftprotos.Message_ViewData:
		return 8
	case *smartbftprotos.Message_NewView:
		return 9
	case *smartbftprotos.Message_StateTransferRequest:
		return 10
	case *smartbftprotos.Message_StateTransferResponse:
		return 11
	default:
		return 12
	}
}

// messageHistogram counts the consensus messages the nodes send, per kind.
type messageHistogram struct {
	counts [13]atomic.Int64
}

// attach counts every message a node sends to another node, through the sender-side mutating hook.
func (h *messageHistogram) attach(nodes []*App) {
	for _, n := range nodes {
		for _, target := range nodes {
			if target.ID == n.ID {
				continue
			}
			n.MutateSend(target.ID, func(_ uint64, m *smartbftprotos.Message) {
				h.counts[messageKindOf(m)].Add(1)
			})
		}
	}
}

func (h *messageHistogram) snapshot() map[string]int64 {
	counts := make(map[string]int64, len(messageKindNames))
	for i, kind := range messageKindNames {
		counts[kind] = h.counts[i].Load()
	}
	return counts
}

// waitQuiescent returns once no message was sent for a while: the last commits of a decision are still on their
// way when the first node delivers it.
func (h *messageHistogram) waitQuiescent(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	last := h.snapshot()
	for {
		time.Sleep(500 * time.Millisecond)
		current := h.snapshot()
		same := true
		for kind, count := range current {
			if last[kind] != count {
				same = false
			}
		}
		if same {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("messages kept flowing")
		}
		last = current
	}
}

// histogramBasicScenario is TestBasic with a single request: four nodes start (and sync), the leader proposes
// once, every node delivers once.
func histogramBasicScenario(t *testing.T, h *messageHistogram) {
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		nodes = append(nodes, n)
	}
	h.attach(nodes)
	startNodes(nodes, network)

	nodes[0].Submit(Request{ID: "1", ClientID: "alice"})

	for i := range numberOfNodes {
		<-nodes[i].Delivered
	}
	h.waitQuiescent(t)
}

// histogramInFlightThenViewChangeScenario is TestNodeInFlightThenViewChange, with the messages counted.
func histogramInFlightThenViewChangeScenario(t *testing.T, h *messageHistogram) {
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	numberOfNodes := 4
	nodes := make([]*App, 0)

	var blockCommits atomic.Uint32

	for i := 1; i <= numberOfNodes; i++ {
		n := newNode(uint64(i), network, t.Name(), testDir, false, 0)
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			if msg.GetCommit() == nil {
				return false
			}
			return blockCommits.Load() == 1
		})
		nodes = append(nodes, n)
	}
	h.attach(nodes)
	startNodes(nodes, network)

	blockCommits.Store(1)

	nodes[0].Submit(Request{ID: "first request", ClientID: "alice"})

	// Ensure no one commits a block because of not enough commits gathered
	var wg sync.WaitGroup
	wg.Add(numberOfNodes)

	var committedTooEarly atomic.Bool
	for i := range numberOfNodes {
		go func(deliverChannel chan *AppRecord) {
			defer wg.Done()
			select {
			case <-deliverChannel:
				committedTooEarly.Store(true)
			case <-time.After(time.Second):
			}
		}(nodes[i].Delivered)
	}

	wg.Wait()
	require.False(t, committedTooEarly.Load(), "should not have committed because first commit message should have been lost")
	blockCommits.Store(0)

	nodes[len(nodes)-1].Disconnect() // Last node in partition

	// Submit second request to all nodes to trigger view change
	nodes[0].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[1].Submit(Request{ID: "second request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "second request", ClientID: "alice"})

	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered // first request
		<-nodes[i].Delivered // second request
	}

	// Submit a third request to one of the nodes
	nodes[0].Submit(Request{ID: "third request", ClientID: "alice"})

	for i := range numberOfNodes - 1 {
		<-nodes[i].Delivered
	}

	// Reconnect the offline node and wait for it to sync
	nodes[len(nodes)-1].Connect()
	<-nodes[len(nodes)-1].Delivered
	<-nodes[len(nodes)-1].Delivered
	<-nodes[len(nodes)-1].Delivered

	// Disconnect leader
	nodes[1].Disconnect()

	// Submit fourth request to all nodes to trigger view change
	nodes[0].Submit(Request{ID: "fourth request", ClientID: "alice"})
	nodes[2].Submit(Request{ID: "fourth request", ClientID: "alice"})
	nodes[3].Submit(Request{ID: "fourth request", ClientID: "alice"})

	// Wait for a view change and a commit
	<-nodes[0].Delivered
	<-nodes[2].Delivered
	<-nodes[3].Delivered
	h.waitQuiescent(t)
}
