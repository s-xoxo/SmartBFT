// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package test

import (
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/smartbftprotos"
	"github.com/stretchr/testify/require"
)

// These tests run view changes with several proposals in flight (design-viewchange-multi-inflight.md sections
// 11, 14.2, 14.3): the prefix of prepared proposals is committed by the new view, decided values survive
// repeated leader changes (S1), a new leader that is behind catches up, and a node that restarts around a view
// change reports what it had in flight. PipelineDepthMax = 3, one request per proposal. They run serially: each
// waits on request-complain and view-change timeouts and on heartbeat periods, which the load of the parallel
// tests stretches past the waits.

// submitTo submits count requests to the given node (1-based), each with a fresh id.
func (c *windowCluster) submitTo(node int, count int) {
	for range count {
		c.nextID++
		c.nodes[node-1].Submit(Request{ID: strconv.Itoa(c.nextID), ClientID: "alice"})
	}
}

// collectFrom waits until each of the given nodes delivered count more records.
func (c *windowCluster) collectFrom(nodes []int, count int, within time.Duration) [][]*AppRecord {
	c.t.Helper()
	records := make([][]*AppRecord, len(nodes))
	for i, n := range nodes {
		for range count {
			select {
			case r := <-c.nodes[n-1].Delivered:
				records[i] = append(records[i], r)
				c.remember(n, r)
			case <-time.After(within):
				c.t.Fatalf("node %d delivered %d of %d records", n, len(records[i]), count)
			}
		}
	}
	return records
}

// viewChangeCluster is a window cluster whose nodes drop every commit they receive while lostCommits is set, so
// that proposals get prepared everywhere but decided nowhere. Holding the senders' commits instead would block
// the links the prepares of the next sequences travel on. extra adds per-node loss conditions.
//
// A view change is triggered by silencing the leader's heartbeat clock (stopHeartbeats) rather than by
// disconnecting it: the leader stays connected, joins the view change and the new view, so that all four nodes
// take part and the quorum survives the one node whose first pre-prepare of the new view arrives before it has
// installed the view (a race of the view start; the pre-prepare is resent, the prepare that came with it is not).
type viewChangeCluster struct {
	*windowCluster
	lostCommits    atomic.Bool
	heartbeatStops map[uint64]chan struct{}
}

func newViewChangeCluster(t *testing.T, prepare func(n *App), extraLoss func(n *App, msg *smartbftprotos.Message) bool) *viewChangeCluster {
	t.Helper()
	vc := &viewChangeCluster{heartbeatStops: make(map[uint64]chan struct{})}
	vc.windowCluster = newWindowCluster(t, 3, func(n *App) {
		// A heartbeat every 500 ms, driven by the test's own clock so that it can be silenced; a leader that falls
		// silent for 5 s is replaced. The new leader's pre-prepare resend (PR #5) runs at the heartbeat period too.
		n.Consensus.Config.LeaderHeartbeatTimeout = 5 * time.Second
		n.Consensus.Config.LeaderHeartbeatCount = 10
		n.heartbeatTime = make(chan time.Time, 1)
		stop := make(chan struct{})
		vc.heartbeatStops[n.ID] = stop
		go func(clock chan time.Time) {
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case now := <-ticker.C:
					select {
					case clock <- now:
					default:
					}
				}
			}
		}(n.heartbeatTime)
		if prepare != nil {
			prepare(n)
		}
		n.LoseMessages(func(msg *smartbftprotos.Message) bool {
			if vc.lostCommits.Load() && msg.GetCommit() != nil {
				return true
			}
			return extraLoss != nil && extraLoss(n, msg)
		})
	})
	return vc
}

// stopHeartbeats silences the heartbeat clock of the given node; as the leader it then sends no heartbeats and
// the followers start a view change once their heartbeat timeout passes. The node keeps receiving and sending
// everything else.
func (c *viewChangeCluster) stopHeartbeats(node uint64) {
	close(c.heartbeatStops[node])
}

func (c *viewChangeCluster) shutdown() {
	for id, stop := range c.heartbeatStops {
		select {
		case <-stop:
		default:
			close(stop)
		}
		delete(c.heartbeatStops, id)
	}
	c.network.Shutdown()
}

// prepareWithoutCommits submits count requests to the leader while every node drops the commits it receives, and
// waits until each of the given nodes has a prepare quorum for all of them: the proposals are in flight, prepared,
// and nothing is decided. It returns the function that lets commits through again.
func (c *viewChangeCluster) prepareWithoutCommits(t *testing.T, leader int, prepared []int, count int) (release func()) {
	t.Helper()
	c.lostCommits.Store(true)
	decidedBefore := make([]int, 5)
	for n := 1; n <= 4; n++ {
		decidedBefore[n] = c.logs.count(uint64(n), "Deciding on seq")
	}
	first := c.nextID + 1
	c.submitTo(leader, count)
	for _, f := range prepared {
		for seq := first; seq < first+count; seq++ {
			c.logs.waitFor(t, uint64(f), fmt.Sprintf("Processed prepares for proposal with seq %d", seq))
		}
	}
	for n := 1; n <= 4; n++ {
		require.Equal(t, decidedBefore[n], c.logs.count(uint64(n), "Deciding on seq"), "node %d decided while the commits were dropped", n)
	}
	return func() { c.lostCommits.Store(false) }
}

// TestMultiInFlightThenViewChange covers the prefix recovery: three proposals are prepared everywhere but not
// committed when the leader falls silent. The new view commits all three through its in-flight views, in order,
// every node reports three in its view data, and every node delivers them identically before the new leader's
// own proposals.
func TestMultiInFlightThenViewChange(t *testing.T) {
	c := newViewChangeCluster(t, nil, nil)
	defer c.shutdown()

	release := c.prepareWithoutCommits(t, 1, []int{2, 3, 4}, 3)
	release()
	c.stopHeartbeats(1)

	for _, n := range []uint64{1, 2, 3, 4} {
		for seq := 1; seq <= 3; seq++ {
			c.logs.waitFor(t, n, fmt.Sprintf("In-flight view 0 with latest sequence %d has committed a decision", seq))
		}
		c.logs.waitFor(t, n, "Starting view with number 1, sequence 4")
		require.Positive(t, c.logs.count(n, "reports 3 proposals in flight in its view data for view 1"), "node %d must report its three prepared proposals", n)
	}
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 3, 60*time.Second), 1)

	c.submitTo(2, 2)
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 2, 60*time.Second), 4)
}

// TestRepeatedLeaderChangesKeepDecisions covers S1 experimentally: two leader changes in a row, each with three
// prepared proposals in flight. What the first new view decided stays decided through the second; every node ends
// with the same records in order.
func TestRepeatedLeaderChangesKeepDecisions(t *testing.T) {
	c := newViewChangeCluster(t, nil, nil)
	defer c.shutdown()

	// View 0 -> 1: node 1 falls silent, node 2 leads; sequences 1-3 are recovered.
	release := c.prepareWithoutCommits(t, 1, []int{2, 3, 4}, 3)
	release()
	c.stopHeartbeats(1)
	for _, n := range []uint64{1, 2, 3, 4} {
		c.logs.waitFor(t, n, "Starting view with number 1, sequence 4")
	}
	firstRound := c.collectFrom([]int{1, 2, 3, 4}, 3, 60*time.Second)
	requireIdenticalInOrder(t, firstRound, 1)
	c.submitTo(2, 2)
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 2, 60*time.Second), 4)

	// View 1 -> 2: node 2 falls silent with sequences 6-8 prepared everywhere.
	release = c.prepareWithoutCommits(t, 2, []int{1, 3, 4}, 3)
	release()
	c.stopHeartbeats(2)
	for _, n := range []uint64{1, 2, 3, 4} {
		c.logs.waitFor(t, n, "Starting view with number 2, sequence 9")
	}
	secondRound := c.collectFrom([]int{1, 2, 3, 4}, 3, 60*time.Second)
	requireIdenticalInOrder(t, secondRound, 6)

	// S1: what was delivered before and by the first new view is what every node still holds, unchanged.
	for n := 1; n <= 4; n++ {
		require.Equal(t, firstRound[n-1], c.deliveredSoFar(n)[:3], "node %d's records of sequences 1-3 changed across the view changes", n)
		require.Equal(t, c.deliveredSoFar(1)[:5], c.deliveredSoFar(n)[:5], "node %d's records differ from node 1's", n)
	}
	c.submitTo(3, 1)
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 1, 60*time.Second), 9)
}

// TestLaggingLeaderSyncsThenViewChanges covers the next leader being two sequences behind (section 14.3, L1/L2):
// it refuses the view data of the nodes ahead, so its view times out and the next one takes over; it catches
// up by sync from the new leader's heartbeat and joins. The view change timeout is short, and the test records
// the views that were started.
func TestLaggingLeaderSyncsThenViewChanges(t *testing.T) {
	var losing atomic.Bool
	losing.Store(true)
	c := newViewChangeCluster(t, func(n *App) {
		n.Consensus.Config.ViewChangeTimeout = 5 * time.Second
	}, func(n *App, msg *smartbftprotos.Message) bool {
		// The next leader misses the commits of the first sequences and falls behind by two.
		return n.ID == 2 && losing.Load() && msg.GetCommit() != nil
	})
	defer c.shutdown()

	c.submit(2)
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 3, 4}, 2, 30*time.Second), 1)
	c.logs.waitFor(t, 2, "Processed prepares for proposal with seq 2")
	require.Zero(t, c.logs.count(2, "Deciding on seq"))
	losing.Store(false)

	// The leader falls silent; node 2, behind, cannot complete the view change it should lead.
	c.stopHeartbeats(1)
	c.logs.waitFor(t, 2, "but the last decision seq 2 is greater than this node's current sequence 0")
	for _, n := range []uint64{1, 3, 4} {
		c.logs.waitForWithin(t, n, "Starting view with number 2, sequence 3", time.Minute)
	}
	// Node 2 learns of the decisions from the new leader's heartbeat and joins.
	c.logs.waitForWithin(t, 2, "Synchronizer returned with sequence 2", time.Minute)
	c.logs.waitForWithin(t, 2, "Starting view with number 2, sequence 3", time.Minute)

	c.submitTo(3, 2)
	records := c.collectFrom([]int{1, 3, 4}, 2, 60*time.Second)
	requireIdenticalInOrder(t, records, 3)
	node2 := c.collectFrom([]int{2}, 4, 60*time.Second)
	require.Equal(t, append(c.deliveredSoFar(1)[:2], records[0]...), node2[0])
	for _, n := range []uint64{1, 2, 3, 4} {
		require.Zero(t, c.logs.count(n, "Starting view with number 3"), "node %d went beyond view 2", n)
	}
	t.Logf("views started: 0, 2 (view 1 timed out on the lagging leader); view change timeout %s", 5*time.Second)
}

// TestPipelineRestartThenViewChangeCarriesRestored covers the seam with the WAL restore (section 14.4): a node that
// restarts with two proposals in flight reports both in its view data when the leader falls silent right after,
// and the new view recovers them.
func TestPipelineRestartThenViewChangeCarriesRestored(t *testing.T) {
	var losing atomic.Bool
	losing.Store(true)
	c := newViewChangeCluster(t, nil, func(n *App, msg *smartbftprotos.Message) bool {
		return n.ID == 4 && losing.Load() && msg.GetPrePrepare() == nil
	})
	defer c.shutdown()

	release := c.prepareWithoutCommits(t, 1, []int{2, 3}, 2)
	c.logs.waitFor(t, 4, "Processed proposal with seq 2")
	losing.Store(false)
	c.nodes[3].RestartSync(false)
	c.logs.waitFor(t, 4, "Restored 2 proposals in flight, sequences 1 to 2")

	release()
	c.stopHeartbeats(1)
	for _, n := range []uint64{1, 2, 3, 4} {
		c.logs.waitFor(t, n, "Starting view with number 1, sequence 3")
	}
	require.Positive(t, c.logs.count(4, "reports 2 proposals in flight in its view data for view 1"), "the restored proposals must be in the restarted node's view data")
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 2, 60*time.Second), 1)
}

// TestRestartDuringViewChangeReportsPrefix covers a restart between sending the view data and receiving the new
// view (section 14.2 case b, upstream #748): the WAL ends with the view-change record, the restart restores the
// prepared proposals of the view being left into the in-flight data only, and the view data sent again after the
// restart reports them; the view change completes with identical records. The new leader drops the view data of
// nodes 1 and 4 until node 4 has restarted, so that the quorum is only reached with the one node 4 sends again.
func TestRestartDuringViewChangeReportsPrefix(t *testing.T) {
	var losingViewData atomic.Bool
	losingViewData.Store(true)
	c := newViewChangeCluster(t, nil, func(n *App, msg *smartbftprotos.Message) bool {
		signer := msg.GetViewData().GetSigner()
		return n.ID == 2 && losingViewData.Load() && msg.GetViewData() != nil && (signer == 1 || signer == 4)
	})
	defer c.shutdown()

	release := c.prepareWithoutCommits(t, 1, []int{2, 3, 4}, 3)
	release()
	c.stopHeartbeats(1)
	c.logs.waitFor(t, 4, "sent view data msg, with next view 1")
	losingViewData.Store(false)
	c.nodes[3].RestartSync(false)
	c.logs.waitFor(t, 4, "Restored 3 proposals in flight of view 0 into the in-flight data, sequences 1 to 3, for the view change in progress")
	c.logs.waitFor(t, 4, "reports 3 proposals in flight in its view data for view 1")

	for _, n := range []uint64{1, 2, 3, 4} {
		c.logs.waitFor(t, n, "Starting view with number 1, sequence 4")
	}
	requireIdenticalInOrder(t, c.collectFrom([]int{1, 2, 3, 4}, 3, 60*time.Second), 1)
}
