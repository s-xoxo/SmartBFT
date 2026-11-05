// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/protobuf/proto"
)

// These tests run the pipeline with PipelineDepthMax above one (design-pipeline-window.md section 11.2,
// design-state-wal-window.md section 9): several proposals in flight, delivery in order, the leader bounded by
// the window, and restarts with proposals in flight restored from the WAL. Every request is its own proposal
// (RequestBatchMaxCount = 1).

// windowLogs keeps the log entries of every node of a cluster, so a test can count them and wait for one.
type windowLogs struct {
	mu      sync.Mutex
	entries map[uint64][]string
	notify  chan struct{}
	// leaderInFlight tracks, on the leader, "Proposing proposal sequence" minus "Deciding on seq" and its maximum.
	leaderInFlight, leaderMaxInFlight int
}

func newWindowLogs() *windowLogs {
	return &windowLogs{entries: make(map[uint64][]string), notify: make(chan struct{}, 1)}
}

func (l *windowLogs) hook(id uint64) func(zapcore.Entry) error {
	return func(entry zapcore.Entry) error {
		l.mu.Lock()
		l.entries[id] = append(l.entries[id], entry.Message)
		if id == 1 {
			switch {
			case strings.Contains(entry.Message, "Proposing proposal sequence"):
				l.leaderInFlight++
				l.leaderMaxInFlight = max(l.leaderMaxInFlight, l.leaderInFlight)
			case strings.Contains(entry.Message, "Deciding on seq"):
				l.leaderInFlight--
			}
		}
		l.mu.Unlock()
		select {
		case l.notify <- struct{}{}:
		default:
		}
		return nil
	}
}

func (l *windowLogs) count(id uint64, substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, e := range l.entries[id] {
		if strings.Contains(e, substr) {
			n++
		}
	}
	return n
}

// indexOf returns the position of the first entry of the node containing substr, or -1.
func (l *windowLogs) indexOf(id uint64, substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, e := range l.entries[id] {
		if strings.Contains(e, substr) {
			return i
		}
	}
	return -1
}

func (l *windowLogs) maxInFlight() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leaderMaxInFlight
}

func (l *windowLogs) waitFor(t *testing.T, id uint64, substr string) {
	t.Helper()
	l.waitForWithin(t, id, substr, 30*time.Second)
}

// waitForWithin is waitFor with its own deadline, for events that wait on a heartbeat period or a view-change
// timeout rather than on message round trips.
func (l *windowLogs) waitForWithin(t *testing.T, id uint64, substr string, within time.Duration) {
	t.Helper()
	deadline := time.After(within)
	for l.count(id, substr) == 0 {
		select {
		case <-l.notify:
		case <-deadline:
			t.Fatalf("timed out waiting for node %d to log %q", id, substr)
		}
	}
}

// windowCluster is a 4-node cluster with PipelineDepthMax = depth, one request per proposal, and the logs of
// every node captured. Node 1 leads view 0.
type windowCluster struct {
	t       *testing.T
	network *Network
	nodes   []*App
	logs    *windowLogs
	nextID  int
	// delivered keeps, per node (1-based), every record the collect helpers took from its Delivered channel.
	deliveredMu sync.Mutex
	delivered   map[int][]*AppRecord
}

func newWindowCluster(t *testing.T, depth uint64, prepare func(n *App)) *windowCluster {
	t.Helper()
	cfg := fastConfig
	cfg.SyncOnStart = true
	cfg.PipelineDepthMax = depth
	cfg.LeaderRotation = false
	cfg.DecisionsPerLeader = 0
	cfg.RequestBatchMaxCount = 1 // one request is one proposal

	c := &windowCluster{t: t, network: NewNetwork(), logs: newWindowLogs(), delivered: make(map[int][]*AppRecord)}
	testDir := t.TempDir()
	for i := 1; i <= 4; i++ {
		n := newNodeWithConfig(uint64(i), c.network, t.Name(), testDir, cfg)
		n.logger = n.logger.Desugar().WithOptions(zap.Hooks(c.logs.hook(uint64(i)))).Sugar()
		if prepare != nil {
			prepare(n)
		}
		n.Setup() // rebuild the consensus with the hooked logger
		c.nodes = append(c.nodes, n)
	}
	startNodes(c.nodes, c.network)
	return c
}

// submit submits count requests to the leader, each with a fresh id.
func (c *windowCluster) submit(count int) {
	for range count {
		c.nextID++
		c.nodes[0].Submit(Request{ID: strconv.Itoa(c.nextID), ClientID: "alice"})
	}
}

func (c *windowCluster) remember(node int, r *AppRecord) {
	c.deliveredMu.Lock()
	defer c.deliveredMu.Unlock()
	c.delivered[node] = append(c.delivered[node], r)
}

// deliveredSoFar returns the records the given node (1-based) delivered so far in this test, in order.
func (c *windowCluster) deliveredSoFar(node int) []*AppRecord {
	c.deliveredMu.Lock()
	defer c.deliveredMu.Unlock()
	return c.delivered[node]
}

// collect waits until every node delivered count more records and returns them per node.
func (c *windowCluster) collect(count int) [][]*AppRecord {
	c.t.Helper()
	records := make([][]*AppRecord, len(c.nodes))
	for i, n := range c.nodes {
		for range count {
			select {
			case r := <-n.Delivered:
				records[i] = append(records[i], r)
				c.remember(i+1, r)
			case <-time.After(60 * time.Second):
				c.t.Fatalf("node %d delivered %d of %d records", n.ID, len(records[i]), count)
			}
		}
	}
	return records
}

// requireIdenticalInOrder checks that every node delivered the same records, with consecutive sequences
// starting at firstSeq.
func requireIdenticalInOrder(t *testing.T, records [][]*AppRecord, firstSeq uint64) {
	t.Helper()
	for i, nodeRecords := range records {
		for j, r := range nodeRecords {
			md := &smartbftprotos.ViewMetadata{}
			require.NoError(t, proto.Unmarshal(r.Metadata, md))
			require.Equal(t, firstSeq+uint64(j), md.GetLatestSequence(), "node %d record %d", i+1, j)
			if i > 0 {
				require.Equal(t, records[0][j], r, "node %d record %d differs from node 1's", i+1, j)
			}
		}
	}
}

// requireNoDrops checks that no node dropped or ignored a consensus message for being outside its window,
// and that no follower found the leader's heartbeat ahead of its own sequence (I10).
func requireNoDrops(t *testing.T, logs *windowLogs, nodes int) {
	t.Helper()
	for id := 1; id <= nodes; id++ {
		require.Zero(t, logs.count(uint64(id), "but our sequence is"), "node %d dropped a message outside its window", id)
		require.Zero(t, logs.count(uint64(id), "dropping message"), "node %d dropped a pre-prepare", id)
		require.Zero(t, logs.count(uint64(id), "Heartbeat sequence is bigger than expected"), "node %d synced because of a heartbeat", id)
	}
}

// TestMultiInFlightBasic runs a depth-4 pipeline: with the followers' commits held the leader proposes four
// sequences before any decision, then forty more requests flow in groups of four, and nothing is dropped,
// no node syncs, and every node delivers identical records in order.
func TestMultiInFlightBasic(t *testing.T) {
	t.Parallel()
	c := newWindowCluster(t, 4, nil)
	defer c.network.Shutdown()

	release := holdCommits(c.nodes, []uint64{2, 3, 4}, []uint64{1, 2, 3, 4})
	c.submit(4)
	c.logs.waitFor(t, 1, "Proposing proposal sequence 4 in view 0")
	require.Zero(t, c.logs.count(1, "Deciding on seq"), "four proposals must be in flight before any decision")
	require.GreaterOrEqual(t, c.logs.maxInFlight(), 4)
	release()
	records := c.collect(4)

	for range 10 {
		c.submit(4)
		group := c.collect(4)
		for i := range records {
			records[i] = append(records[i], group[i]...)
		}
	}

	requireIdenticalInOrder(t, records, 1)
	require.Len(t, records[0], 44)
	requireNoDrops(t, c.logs, 4)
	require.LessOrEqual(t, c.logs.maxInFlight(), 4, "the leader never has more than four proposals in flight")
}

// TestPipelineDecideInOrder covers the in-order delivery gate across nodes with depth 3: the commits of
// sequence 1 from nodes 2 and 3 are held back (turned into harmless assist prepares and replayed later), so
// nodes 1 and 4 collect the commit quorums of sequences 2 and 3 first. They deliver nothing until sequence 1
// is committed, then deliver 1, 2, 3 in order; every node delivers the same records once.
func TestPipelineDecideInOrder(t *testing.T) {
	t.Parallel()
	c := newWindowCluster(t, 3, nil)
	defer c.network.Shutdown()

	type heldCommit struct {
		from, to uint64
		msg      *smartbftprotos.Message
	}
	var (
		heldLock sync.Mutex
		held     []heldCommit
	)
	for _, from := range []uint64{2, 3} {
		for to := uint64(1); to <= 4; to++ {
			if to == from {
				continue
			}
			c.nodes[from-1].MutateSend(to, func(target uint64, msg *smartbftprotos.Message) {
				commit := msg.GetCommit()
				if commit == nil || commit.GetSeq() != 1 {
					return
				}
				heldLock.Lock()
				held = append(held, heldCommit{from: from, to: target, msg: proto.Clone(msg).(*smartbftprotos.Message)})
				heldLock.Unlock()
				// The receiver already has this node's prepare for the sequence, so a second one is ignored.
				msg.Content = &smartbftprotos.Message_Prepare{Prepare: &smartbftprotos.Prepare{
					View: commit.GetView(), Seq: commit.GetSeq(), Digest: commit.GetDigest(), Assist: true,
				}}
			})
		}
	}

	c.submit(3)
	for _, id := range []uint64{1, 4} {
		c.logs.waitFor(t, id, fmt.Sprintf("%d processed commits for proposal with seq 2", id))
		c.logs.waitFor(t, id, fmt.Sprintf("%d processed commits for proposal with seq 3", id))
		select {
		case r := <-c.nodes[id-1].Delivered:
			t.Fatalf("node %d delivered %v before sequence 1 was committed", id, r.Metadata)
		default:
		}
		require.Zero(t, c.logs.count(id, "Deciding on seq"))
	}

	// Release the commits of sequence 1: nodes 1 and 4 now have everything, and deliver 1, 2, 3.
	heldLock.Lock()
	for _, h := range held {
		c.network.send(h.from, h.to, h.msg)
	}
	heldLock.Unlock()

	records := c.collect(3)
	requireIdenticalInOrder(t, records, 1)
	for _, id := range []uint64{1, 4} {
		require.Equal(t, 1, c.logs.count(id, "Deciding on seq 1"))
		require.Equal(t, 1, c.logs.count(id, "Deciding on seq 2"))
		require.Equal(t, 1, c.logs.count(id, "Deciding on seq 3"))
	}
}

// TestPipelineLeaderStopsAtWindow covers the leader token semaphore with depth 3: with the followers' commits
// held, five requests lead to exactly three proposals; the fourth and fifth follow the first decisions, and at
// no time are more than three proposals in flight.
func TestPipelineLeaderStopsAtWindow(t *testing.T) {
	t.Parallel()
	c := newWindowCluster(t, 3, nil)
	defer c.network.Shutdown()

	release := holdCommits(c.nodes, []uint64{2, 3, 4}, []uint64{1, 2, 3, 4})
	c.submit(5)
	c.logs.waitFor(t, 1, "Proposing proposal sequence 3 in view 0")

	// The fourth proposal must wait for a decision, which the held commits prevent.
	time.Sleep(time.Second)
	require.Zero(t, c.logs.count(1, "Proposing proposal sequence 4"), "the leader proposed beyond its window")
	require.Equal(t, 3, c.logs.maxInFlight())

	release()
	records := c.collect(5)
	c.logs.waitFor(t, 1, "Proposing proposal sequence 5 in view 0")
	requireIdenticalInOrder(t, records, 1)
	require.Equal(t, 3, c.logs.maxInFlight(), "the leader never has more than three proposals in flight")
	requireNoDrops(t, c.logs, 4)
}

// TestPipelineRestartRestoresInFlight covers the restore of several proposals in flight (merge criterion of
// feat/state-wal-window): a node that persisted two pre-prepares and never decided them restarts, reads both
// back from the WAL ("Restored 2 proposals in flight"), and once its messages flow again delivers the same
// records as the others, in order.
func TestPipelineRestartRestoresInFlight(t *testing.T) {
	t.Parallel()
	var losing atomic.Bool
	losing.Store(true)
	c := newWindowCluster(t, 3, func(n *App) {
		if n.ID == 4 {
			// The node accepts pre-prepares only, so it persists the proposals and never decides them.
			n.LoseMessages(func(msg *smartbftprotos.Message) bool {
				return losing.Load() && msg.GetPrePrepare() == nil
			})
		}
	})
	defer c.network.Shutdown()

	c.submit(2)
	records := make([][]*AppRecord, 4)
	for i, n := range c.nodes[:3] {
		for range 2 {
			select {
			case r := <-n.Delivered:
				records[i] = append(records[i], r)
			case <-time.After(30 * time.Second):
				t.Fatalf("node %d did not deliver", n.ID)
			}
		}
	}
	isolated := c.nodes[3]
	c.logs.waitFor(t, 4, "Processed proposal with seq 1")
	c.logs.waitFor(t, 4, "Processed proposal with seq 2")
	require.Zero(t, c.logs.count(4, "Deciding on seq"))

	// Restart without a sync, so that the WAL, not the other nodes, is what the node resumes from.
	losing.Store(false)
	isolated.RestartSync(false)
	c.logs.waitFor(t, 4, "Restored 2 proposals in flight, sequences 1 to 2")
	require.Equal(t, 1, c.logs.count(4, "Restored proposal with sequence 1"))
	require.Equal(t, 1, c.logs.count(4, "Restored proposal with sequence 2"))

	// The restored prepares reach nodes that delivered the sequences; they answer from their slots and the
	// node completes both sequences from where it stopped.
	for range 2 {
		select {
		case r := <-isolated.Delivered:
			records[3] = append(records[3], r)
		case <-time.After(60 * time.Second):
			t.Fatalf("the restarted node delivered %d of 2 records", len(records[3]))
		}
	}
	requireIdenticalInOrder(t, records, 1)

	// And it takes part in what follows.
	c.submit(3)
	requireIdenticalInOrder(t, c.collect(3), 3)
}

// TestPipelineLeaderRestartWithInFlight covers the restored leader: it loses the commits of its first two
// proposals, so it has both in flight, prepared, when it restarts while the followers delivered them. It restores
// both, issues W-2 = 1 token (so it proposes exactly one more sequence before it decides anything), completes the
// two through the followers' answers to its commits, and then every node has the same four records.
func TestPipelineLeaderRestartWithInFlight(t *testing.T) {
	t.Parallel()
	var leaderLosesCommits atomic.Bool
	leaderLosesCommits.Store(true)
	c := newWindowCluster(t, 3, func(n *App) {
		if n.ID == 1 {
			n.LoseMessages(func(msg *smartbftprotos.Message) bool { return leaderLosesCommits.Load() && msg.GetCommit() != nil })
		}
	})
	defer c.network.Shutdown()

	c.submit(2)
	followers := c.collectFrom([]int{2, 3, 4}, 2, 30*time.Second)
	requireIdenticalInOrder(t, followers, 1)
	c.logs.waitFor(t, 1, "Processed prepares for proposal with seq 1")
	c.logs.waitFor(t, 1, "Processed prepares for proposal with seq 2")
	require.Zero(t, c.logs.count(1, "Deciding on seq"), "the leader must not decide while it loses the commits")

	leader := c.nodes[0]
	leaderLosesCommits.Store(false)
	leader.RestartSync(false)
	c.logs.waitFor(t, 1, "Restored 2 proposals in flight, sequences 1 to 2")

	// One token left: the third request is proposed at once, the fourth only after a decision, which the leader
	// reaches once the followers answer its commits from their delivered slots.
	c.submit(2)
	c.logs.waitFor(t, 1, "Proposing proposal sequence 3 in view 0")
	c.logs.waitFor(t, 1, "Proposing proposal sequence 4 in view 0")
	decided := c.logs.indexOf(1, "Deciding on seq 1")
	proposed4 := c.logs.indexOf(1, "Proposing proposal sequence 4 in view 0")
	require.NotEqual(t, -1, decided)
	require.Less(t, decided, proposed4, "the fourth proposal must wait for a decision: W-2 tokens after the restore")

	leaderRecords := c.collectFrom([]int{1}, 4, 60*time.Second)
	rest := c.collectFrom([]int{2, 3, 4}, 2, 60*time.Second)
	require.Equal(t, followers[0], leaderRecords[0][:2])
	requireIdenticalInOrder(t, [][]*AppRecord{leaderRecords[0][2:], rest[0], rest[1], rest[2]}, 3)
}

// TestPipelineRestartAfterDeliveryStartsClean covers the checkpoint filter: a leader that restarts right after
// delivering a sequence finds its records after the truncation point, drops them as delivered, restores nothing,
// and issues W tokens: three requests become three proposals in flight before any decision.
func TestPipelineRestartAfterDeliveryStartsClean(t *testing.T) {
	t.Parallel()
	c := newWindowCluster(t, 3, nil)
	defer c.network.Shutdown()

	c.submit(1)
	requireIdenticalInOrder(t, c.collect(1), 1)

	leader := c.nodes[0]
	leader.RestartSync(false)
	c.logs.waitFor(t, 1, "Last proposal with sequence 1 has been safely committed")
	c.logs.waitFor(t, 1, "Every proposal in the WAL was delivered already, nothing in flight to restore")
	require.Zero(t, c.logs.count(1, "Restored proposal"))

	release := holdCommits(c.nodes, []uint64{2, 3, 4}, []uint64{1, 2, 3, 4})
	c.submit(3)
	c.logs.waitFor(t, 1, "Proposing proposal sequence 4 in view 0")
	require.Equal(t, 1, c.logs.count(1, "Deciding on seq"), "three proposals must be in flight before any decision after the restart")
	release()
	requireIdenticalInOrder(t, c.collect(3), 2)
}
