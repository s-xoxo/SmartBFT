package test

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestPipelineConfigSurvivesRestart starts a cluster from an explicit configuration (PipelineDepthMax set, short
// view-change and heartbeat timers) through newNodeWithConfig, and checks that a restarted node keeps that
// configuration rather than falling back to fastConfig, while the network keeps delivering.
func TestPipelineConfigSurvivesRestart(t *testing.T) {
	t.Parallel()
	network := NewNetwork()
	defer network.Shutdown()

	testDir := t.TempDir()

	cfg := fastConfig
	cfg.SyncOnStart = true
	cfg.PipelineDepthMax = 1 // explicit: the original sequential behaviour
	cfg.AdaptiveBatch = false
	cfg.ViewChangeTimeout = 10 * time.Second
	cfg.LeaderHeartbeatTimeout = 10 * time.Second
	require.NotEqual(t, fastConfig.ViewChangeTimeout, cfg.ViewChangeTimeout)
	require.NotEqual(t, fastConfig.LeaderHeartbeatTimeout, cfg.LeaderHeartbeatTimeout)

	numberOfNodes := 4
	nodes := make([]*App, 0, numberOfNodes)
	for i := 1; i <= numberOfNodes; i++ {
		n := newNodeWithConfig(uint64(i), network, t.Name(), testDir, cfg)
		nodes = append(nodes, n)
	}

	assertConfig := func(n *App) {
		t.Helper()
		require.Equal(t, n.ID, n.Consensus.Config.SelfID)
		require.Equal(t, uint64(1), n.Consensus.Config.PipelineDepthMax)
		require.Equal(t, uint64(1), n.Consensus.Config.EffectivePipelineDepth())
		require.False(t, n.Consensus.Config.AdaptiveBatch)
		require.Equal(t, 10*time.Second, n.Consensus.Config.ViewChangeTimeout)
		require.Equal(t, 10*time.Second, n.Consensus.Config.LeaderHeartbeatTimeout)
	}
	for _, n := range nodes {
		assertConfig(n)
	}

	startNodes(nodes, network)

	deliverAndCompare := func(id string) {
		t.Helper()
		nodes[0].Submit(Request{ID: id, ClientID: "alice"})
		data := make([]*AppRecord, 0, numberOfNodes)
		for i := range numberOfNodes {
			d := <-nodes[i].Delivered
			data = append(data, d)
		}
		for i := range numberOfNodes - 1 {
			require.Equal(t, data[i], data[i+1])
		}
	}

	deliverAndCompare("1")
	deliverAndCompare("2")
	deliverAndCompare("3")

	// Restart a follower: Setup rebuilds the consensus from the configuration it was running with.
	nodes[3].Restart()
	assertConfig(nodes[3])
	require.True(t, nodes[3].Consensus.Config.SyncOnStart)

	deliverAndCompare("4")
}

// TestReconfigCarriesPipelineFields checks that the test-side reconfig converters do not drop the new fields,
// so a reconfig does not silently reset PipelineDepthMax/AdaptiveBatch.
func TestReconfigCarriesPipelineFields(t *testing.T) {
	t.Parallel()

	cfg := fastConfig
	cfg.PipelineDepthMax = 3
	cfg.AdaptiveBatch = true

	intConfig := recconfigToInt(types.Reconfig{CurrentConfig: cfg}).CurrentConfig
	require.Equal(t, int64(3), intConfig.PipelineDepthMax)
	require.True(t, intConfig.AdaptiveBatch)

	back := Reconfig{CurrentConfig: intConfig}.recconfigToUint(1).CurrentConfig
	require.Equal(t, uint64(3), back.PipelineDepthMax)
	require.True(t, back.AdaptiveBatch)
	require.Equal(t, uint64(1), back.SelfID)
}
