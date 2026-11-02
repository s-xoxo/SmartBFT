// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package types_test

import (
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestConfigurationValidate(t *testing.T) {
	validConfig := func() types.Configuration {
		config := types.DefaultConfig
		config.SelfID = 1
		return config
	}

	require.NoError(t, validConfig().Validate())

	for _, tst := range []struct {
		name        string
		mutate      func(*types.Configuration)
		expectedErr string
	}{
		{
			name:        "zero SelfID",
			mutate:      func(c *types.Configuration) { c.SelfID = 0 },
			expectedErr: "SelfID should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxCount",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxCount = 0 },
			expectedErr: "RequestBatchMaxCount should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxBytes = 0 },
			expectedErr: "RequestBatchMaxBytes should be greater than zero",
		},
		{
			name:        "zero RequestBatchMaxInterval",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxInterval = 0 },
			expectedErr: "RequestBatchMaxInterval should be greater than zero",
		},
		{
			name:        "negative RequestBatchMaxInterval",
			mutate:      func(c *types.Configuration) { c.RequestBatchMaxInterval = -time.Second },
			expectedErr: "RequestBatchMaxInterval should be greater than zero",
		},
		{
			name:        "zero IncomingMessageBufferSize",
			mutate:      func(c *types.Configuration) { c.IncomingMessageBufferSize = 0 },
			expectedErr: "IncomingMessageBufferSize should be greater than zero",
		},
		{
			name:        "zero RequestPoolSize",
			mutate:      func(c *types.Configuration) { c.RequestPoolSize = 0 },
			expectedErr: "RequestPoolSize should be greater than zero",
		},
		{
			name:        "zero RequestForwardTimeout",
			mutate:      func(c *types.Configuration) { c.RequestForwardTimeout = 0 },
			expectedErr: "RequestForwardTimeout should be greater than zero",
		},
		{
			name:        "zero RequestComplainTimeout",
			mutate:      func(c *types.Configuration) { c.RequestComplainTimeout = 0 },
			expectedErr: "RequestComplainTimeout should be greater than zero",
		},
		{
			name:        "zero RequestAutoRemoveTimeout",
			mutate:      func(c *types.Configuration) { c.RequestAutoRemoveTimeout = 0 },
			expectedErr: "RequestAutoRemoveTimeout should be greater than zero",
		},
		{
			name:        "zero ViewChangeResendInterval",
			mutate:      func(c *types.Configuration) { c.ViewChangeResendInterval = 0 },
			expectedErr: "ViewChangeResendInterval should be greater than zero",
		},
		{
			name:        "zero ViewChangeTimeout",
			mutate:      func(c *types.Configuration) { c.ViewChangeTimeout = 0 },
			expectedErr: "ViewChangeTimeout should be greater than zero",
		},
		{
			name:        "zero LeaderHeartbeatTimeout",
			mutate:      func(c *types.Configuration) { c.LeaderHeartbeatTimeout = 0 },
			expectedErr: "LeaderHeartbeatTimeout should be greater than zero",
		},
		{
			name:        "zero LeaderHeartbeatCount",
			mutate:      func(c *types.Configuration) { c.LeaderHeartbeatCount = 0 },
			expectedErr: "LeaderHeartbeatCount should be greater than zero",
		},
		{
			name:        "zero NumOfTicksBehindBeforeSyncing",
			mutate:      func(c *types.Configuration) { c.NumOfTicksBehindBeforeSyncing = 0 },
			expectedErr: "NumOfTicksBehindBeforeSyncing should be greater than zero",
		},
		{
			name:        "zero CollectTimeout",
			mutate:      func(c *types.Configuration) { c.CollectTimeout = 0 },
			expectedErr: "CollectTimeout should be greater than zero",
		},
		{
			name: "RequestBatchMaxCount bigger than RequestBatchMaxBytes",
			mutate: func(c *types.Configuration) {
				c.RequestBatchMaxBytes = c.RequestMaxBytes
				c.RequestBatchMaxCount = c.RequestBatchMaxBytes + 1
			},
			expectedErr: "RequestBatchMaxCount is bigger than RequestBatchMaxBytes",
		},
		{
			name:        "RequestMaxBytes bigger than RequestBatchMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestMaxBytes = c.RequestBatchMaxBytes + 1 },
			expectedErr: "RequestMaxBytes is bigger than RequestBatchMaxBytes",
		},
		{
			name:        "RequestForwardTimeout bigger than RequestComplainTimeout",
			mutate:      func(c *types.Configuration) { c.RequestForwardTimeout = c.RequestComplainTimeout + time.Second },
			expectedErr: "RequestForwardTimeout is bigger than RequestComplainTimeout",
		},
		{
			name:        "RequestComplainTimeout bigger than RequestAutoRemoveTimeout",
			mutate:      func(c *types.Configuration) { c.RequestComplainTimeout = c.RequestAutoRemoveTimeout + time.Second },
			expectedErr: "RequestComplainTimeout is bigger than RequestAutoRemoveTimeout",
		},
		{
			name:        "ViewChangeResendInterval bigger than ViewChangeTimeout",
			mutate:      func(c *types.Configuration) { c.ViewChangeResendInterval = c.ViewChangeTimeout + time.Second },
			expectedErr: "ViewChangeResendInterval is bigger than ViewChangeTimeout",
		},
		{
			name: "leader rotation without DecisionsPerLeader",
			mutate: func(c *types.Configuration) {
				c.LeaderRotation = true
				c.DecisionsPerLeader = 0
			},
			expectedErr: "DecisionsPerLeader should be greater than zero when leader rotation is active",
		},
		{
			name: "DecisionsPerLeader without leader rotation",
			mutate: func(c *types.Configuration) {
				c.LeaderRotation = false
				c.DecisionsPerLeader = 1
			},
			expectedErr: "DecisionsPerLeader should be zero when leader rotation is off",
		},
		{
			name:        "zero RequestMaxBytes",
			mutate:      func(c *types.Configuration) { c.RequestMaxBytes = 0 },
			expectedErr: "RequestMaxBytes should be greater than zero",
		},
		{
			name:        "zero RequestPoolSubmitTimeout",
			mutate:      func(c *types.Configuration) { c.RequestPoolSubmitTimeout = 0 },
			expectedErr: "RequestPoolSubmitTimeout should be greater than zero",
		},
	} {
		t.Run(tst.name, func(t *testing.T) {
			config := validConfig()
			tst.mutate(&config)
			require.EqualError(t, config.Validate(), tst.expectedErr)
		})
	}

	t.Run("leader rotation off", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		require.NoError(t, config.Validate())
	})
}

func TestConfigurationValidatePipelineDepth(t *testing.T) {
	validConfig := func() types.Configuration {
		config := types.DefaultConfig
		config.SelfID = 1
		return config
	}

	t.Run("default config has depth one and adaptive batch off", func(t *testing.T) {
		config := validConfig()
		require.Equal(t, uint64(1), config.PipelineDepthMax)
		require.False(t, config.AdaptiveBatch)
		require.NoError(t, config.Validate())
	})

	t.Run("depth zero (unset) validates with rotation on", func(t *testing.T) {
		config := validConfig()
		config.PipelineDepthMax = 0
		require.True(t, config.LeaderRotation)
		require.NoError(t, config.Validate())
	})

	t.Run("depth one validates with rotation on", func(t *testing.T) {
		config := validConfig()
		config.PipelineDepthMax = 1
		require.True(t, config.LeaderRotation)
		require.NoError(t, config.Validate())
	})

	t.Run("depth two with rotation on fails", func(t *testing.T) {
		config := validConfig()
		config.PipelineDepthMax = 2
		require.True(t, config.LeaderRotation)
		require.EqualError(t, config.Validate(),
			"PipelineDepthMax greater than one requires LeaderRotation to be off (and DecisionsPerLeader to be zero)")
	})

	t.Run("depth two with rotation off and big enough pool passes", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		config.PipelineDepthMax = 2
		require.Equal(t, uint64(100), config.RequestBatchMaxCount)
		require.Equal(t, uint64(400), config.RequestPoolSize)
		require.NoError(t, config.Validate())
	})

	t.Run("depth four exactly filling the pool passes", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		config.PipelineDepthMax = 4 // 4 * 100 == 400
		require.NoError(t, config.Validate())
	})

	t.Run("depth four with batch count times depth above pool fails", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		config.PipelineDepthMax = 4
		config.RequestPoolSize = 399 // 4 * 100 > 399
		require.EqualError(t, config.Validate(),
			"PipelineDepthMax times RequestBatchMaxCount is bigger than RequestPoolSize")
	})

	t.Run("huge depth does not overflow the pool check", func(t *testing.T) {
		config := validConfig()
		config.LeaderRotation = false
		config.DecisionsPerLeader = 0
		config.PipelineDepthMax = ^uint64(0)
		require.EqualError(t, config.Validate(),
			"PipelineDepthMax times RequestBatchMaxCount is bigger than RequestPoolSize")
	})

	t.Run("adaptive batch flag does not affect validation", func(t *testing.T) {
		config := validConfig()
		config.AdaptiveBatch = true
		require.NoError(t, config.Validate())
	})
}

func TestConfigurationEffectivePipelineDepth(t *testing.T) {
	for _, tst := range []struct {
		depth    uint64
		expected uint64
	}{
		{depth: 0, expected: 1},
		{depth: 1, expected: 1},
		{depth: 2, expected: 2},
		{depth: 7, expected: 7},
	} {
		config := types.Configuration{PipelineDepthMax: tst.depth}
		require.Equal(t, tst.expected, config.EffectivePipelineDepth())
	}
	require.Equal(t, uint64(1), types.DefaultConfig.EffectivePipelineDepth())
}
