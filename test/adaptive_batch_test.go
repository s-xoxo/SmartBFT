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

	"github.com/hyperledger/SmartBFT/pkg/consensus"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// adaptiveCInit is ceil(fastConfig.RequestBatchMaxCount / 4): the count an adaptive leader starts a view with.
const adaptiveCInit = 3

// adaptiveCluster is a 4-node cluster started from fastConfig with short view-change and heartbeat timers,
// the leader's "Adaptive batch:" log lines captured, and a goroutine per node draining Delivered.
type adaptiveCluster struct {
	t        *testing.T
	network  *Network
	nodes    []*App
	tunerLog chan string // every "Adaptive batch: ..." line the leader logged
	tunerN   atomic.Int64
	records  []chan *AppRecord // per node, re-buffered copies of Delivered
	done     chan struct{}
}

func newAdaptiveCluster(t *testing.T, adaptive bool, tunerConfig *consensus.TunerConfig) *adaptiveCluster {
	t.Helper()
	cfg := fastConfig
	cfg.SyncOnStart = true
	cfg.AdaptiveBatch = adaptive
	cfg.ViewChangeTimeout = 10 * time.Second
	cfg.LeaderHeartbeatTimeout = 10 * time.Second

	c := &adaptiveCluster{
		t:        t,
		network:  NewNetwork(),
		tunerLog: make(chan string, 10_000),
		done:     make(chan struct{}),
	}
	testDir := t.TempDir()
	const numberOfNodes = 4
	for i := 1; i <= numberOfNodes; i++ {
		n := newNodeWithConfig(uint64(i), c.network, t.Name(), testDir, cfg)
		n.Consensus.BatchTunerConfig = tunerConfig
		c.nodes = append(c.nodes, n)
	}

	// Node 1 leads view 0: capture the tuner's log lines there.
	leader := c.nodes[0]
	leader.Consensus.Logger = leader.logger.Desugar().WithOptions(zap.Hooks(func(entry zapcore.Entry) error {
		if strings.Contains(entry.Message, "Adaptive batch:") {
			c.tunerN.Add(1)
			select {
			case c.tunerLog <- entry.Message:
			default:
			}
		}
		return nil
	})).Sugar()

	startNodes(c.nodes, c.network)

	// Delivered has a buffer of 100 and a leader under load delivers faster than a test reads: drain each node.
	for _, n := range c.nodes {
		out := make(chan *AppRecord, 100_000)
		c.records = append(c.records, out)
		go func() {
			for {
				select {
				case r := <-n.Delivered:
					out <- r
				case <-c.done:
					return
				}
			}
		}()
	}
	return c
}

func (c *adaptiveCluster) shutdown() {
	close(c.done)
	c.network.Shutdown()
}

// submitInBackground has `clients` clients submit `perClient` requests each to the leader, concurrently, and returns
// when all are submitted. Request IDs are unique per client, so the total is clients*perClient distinct requests.
func (c *adaptiveCluster) submitInBackground(clients, perClient int, clientPrefix string) {
	var wg sync.WaitGroup
	for cl := range clients {
		wg.Go(func() {
			clientID := fmt.Sprintf("%s-%d", clientPrefix, cl)
			for i := 1; i <= perClient; i++ {
				c.nodes[0].Submit(Request{ID: strconv.Itoa(i), ClientID: clientID})
			}
		})
	}
	wg.Wait()
}

// collect waits until every node delivered exactly `total` more requests and returns the records per node.
func (c *adaptiveCluster) collect(total int) [][]*AppRecord {
	c.t.Helper()
	deadline := time.After(60 * time.Second)
	all := make([][]*AppRecord, len(c.nodes))
	for i := range c.nodes {
		var got int
		for got < total {
			select {
			case r := <-c.records[i]:
				all[i] = append(all[i], r)
				got += len(r.Batch.Requests)
			case <-deadline:
				c.t.Fatalf("node %d delivered %d of %d requests", i+1, got, total)
			}
		}
		require.Equal(c.t, total, got, "node %d delivered more requests than expected", i+1)
	}
	return all
}

// requireIdenticalAndUnique checks that every node delivered the same records in the same order and that
// no request was delivered twice.
func requireIdenticalAndUnique(t *testing.T, all [][]*AppRecord) {
	t.Helper()
	for i := range len(all) - 1 {
		require.Equal(t, all[i], all[i+1], "nodes %d and %d delivered different records", i+1, i+2)
	}
	seen := make(map[string]struct{})
	for _, r := range all[0] {
		for _, raw := range r.Batch.Requests {
			req := requestFromBytes(raw)
			key := req.ClientID + "/" + req.ID
			_, dup := seen[key]
			require.False(t, dup, "request %s delivered twice", key)
			seen[key] = struct{}{}
		}
	}
}

func maxBatchSize(records []*AppRecord) int {
	m := 0
	for _, r := range records {
		m = max(m, len(r.Batch.Requests))
	}
	return m
}

// waitForTunerLog waits until the leader logs an "Adaptive batch:" line containing every one of the given substrings.
func (c *adaptiveCluster) waitForTunerLog(what string, substrings ...string) {
	c.t.Helper()
	deadline := time.After(30 * time.Second)
	for {
		select {
		case line := <-c.tunerLog:
			matches := true
			for _, s := range substrings {
				matches = matches && strings.Contains(line, s)
			}
			if matches {
				return
			}
		case <-deadline:
			c.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// TestAdaptiveBatchGrowsUnderLoad: with AdaptiveBatch on, a leader starts at CInit (3 of the configured 10) and,
// under a burst that keeps a backlog, grows the batch count towards the configured maximum, never above it, while
// every node delivers the same requests in the same order exactly once.
func TestAdaptiveBatchGrowsUnderLoad(t *testing.T) {
	t.Parallel()
	// K=1: one full decision with a backlog is enough evidence, so the ramp 3->4->5->7->9->10 takes 5 decisions.
	c := newAdaptiveCluster(t, true, &consensus.TunerConfig{K: 1})
	defer c.shutdown()

	const clients, perClient = 3, 100
	c.submitInBackground(clients, perClient, "alice")
	all := c.collect(clients * perClient)
	requireIdenticalAndUnique(t, all)

	leaderRecords := all[0]
	require.LessOrEqual(t, len(leaderRecords[0].Batch.Requests), adaptiveCInit, "the first batch is bounded by CInit")
	biggest := maxBatchSize(leaderRecords)
	require.Greater(t, biggest, adaptiveCInit, "the batch count grew above CInit under load")
	require.LessOrEqual(t, biggest, int(fastConfig.RequestBatchMaxCount), "the batch count never exceeds the configured maximum")

	// The leader logged the growth: at least one adjustment that is not a reset, up to the configured maximum.
	c.waitForTunerLog("the leader to grow the batch count to the configured maximum", "max count 10,")
	require.Greater(t, c.tunerN.Load(), int64(1))
}

// TestAdaptiveBatchRelaxesWhenIdle: after the load is gone the leader relaxes back to CInit, so the first batch of a
// new burst is bounded by CInit again (it would be 10 without the relaxation).
func TestAdaptiveBatchRelaxesWhenIdle(t *testing.T) {
	t.Parallel()
	c := newAdaptiveCluster(t, true, &consensus.TunerConfig{K: 1, IdleAfter: 50 * time.Millisecond})
	defer c.shutdown()

	const clients, perClient = 3, 100
	c.submitInBackground(clients, perClient, "bob")
	all := c.collect(clients * perClient)
	requireIdenticalAndUnique(t, all)
	require.Greater(t, maxBatchSize(all[0]), adaptiveCInit)

	// Idle: the leader polls an empty pool once per timeout and relaxes one step per IdleAfter: 10 -> 7 -> 5 -> 3.
	c.waitForTunerLog("the leader to relax the batch count back to CInit", "(RELAX)", fmt.Sprintf("max count %d,", adaptiveCInit))

	// A burst of 10 at once: the first batch cannot hold more than CInit requests any more.
	const burst = 10
	for i := 1; i <= burst; i++ {
		c.nodes[0].Submit(Request{ID: strconv.Itoa(i), ClientID: "carol"})
	}
	after := c.collect(burst)
	requireIdenticalAndUnique(t, after)
	require.LessOrEqual(t, len(after[0][0].Batch.Requests), adaptiveCInit, "the first batch after the idle period is bounded by CInit")
	require.Greater(t, len(after[0]), 1, "a burst of %d is split into several batches after relaxing", burst)
}

// TestAdaptiveBatchOffIsBaseline: with the flag off the same load produces no adjustment at all: no tuner log line,
// batches bounded by the configured maximum only, every node identical.
func TestAdaptiveBatchOffIsBaseline(t *testing.T) {
	t.Parallel()
	c := newAdaptiveCluster(t, false, &consensus.TunerConfig{K: 1}) // the override is ignored when the flag is off
	defer c.shutdown()

	const clients, perClient = 3, 100
	c.submitInBackground(clients, perClient, "dave")
	all := c.collect(clients * perClient)
	requireIdenticalAndUnique(t, all)
	require.LessOrEqual(t, maxBatchSize(all[0]), int(fastConfig.RequestBatchMaxCount))

	time.Sleep(time.Second) // negative check: nothing adaptive is logged, now or shortly after
	require.Zero(t, c.tunerN.Load(), "the leader logged adaptive batch lines with the flag off")
	require.Equal(t, fastConfig.RequestBatchMaxCount, c.nodes[0].Consensus.Config.RequestBatchMaxCount)
}
