// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft_test

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/hyperledger/SmartBFT/internal/bft"
	"github.com/hyperledger/SmartBFT/internal/bft/mocks"
	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestReqPoolBasic(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}
	byteReq1 := makeTestRequest("1", "1", "foo")

	submittedChan := make(chan struct{}, 1)

	t.Run("create close", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 3, ForwardTimeout: time.Hour}, submittedChan)

		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())
		pool.Close()
		require.Equal(t, 0, pool.Size())

		err = pool.Submit(byteReq1)
		require.EqualError(t, err, "pool closed, request rejected: {1 1}")
	})

	t.Run("submit remove next", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 3, ForwardTimeout: time.Hour}, submittedChan)
		defer pool.Close()

		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())
		req1 := types.RequestInfo{
			ID:       "1",
			ClientID: "1",
		}
		byteReq11 := makeTestRequest("11", "11", "foo")
		req11 := types.RequestInfo{
			ID:       "11",
			ClientID: "11",
		}
		err = pool.Submit(byteReq1)
		require.Error(t, err)
		require.Equal(t, 1, pool.Size())
		err = pool.RemoveRequest(req1)
		require.NoError(t, err)
		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq11)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())
		err = pool.RemoveRequest(req11)
		require.NoError(t, err)
		require.Equal(t, 0, pool.Size())

		byteReq2 := makeTestRequest("2", "2", "bar")
		byteReq22 := makeTestRequest("22", "22", "bar")
		req22 := types.RequestInfo{
			ID:       "22",
			ClientID: "22",
		}
		err = pool.Submit(byteReq2)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())
		err = pool.Submit(byteReq22)
		require.NoError(t, err)
		require.Equal(t, 2, pool.Size())
		err = pool.Submit(byteReq22)
		require.Error(t, err)
		err = pool.Submit(byteReq2)
		require.Error(t, err)
		err = pool.RemoveRequest(req22)
		require.NoError(t, err)
		byteReq23 := makeTestRequest("23", "23", "bar")
		req23 := types.RequestInfo{
			ID:       "23",
			ClientID: "23",
		}
		err = pool.Submit(byteReq23)
		require.NoError(t, err)
		req2 := types.RequestInfo{
			ID:       "2",
			ClientID: "2",
		}
		err = pool.RemoveRequest(req2)
		require.NoError(t, err)
		byteReq24 := makeTestRequest("24", "24", "bar")
		req24 := types.RequestInfo{
			ID:       "24",
			ClientID: "24",
		}
		err = pool.Submit(byteReq24)
		require.NoError(t, err)

		byteReq3 := makeTestRequest("3", "3", "bog")
		err = pool.Submit(byteReq3)
		require.NoError(t, err)

		next, full := pool.NextRequests(4, 10000000, false)
		require.Equal(t, "23", insp.RequestID(next[0]).ID)
		require.Equal(t, "24", insp.RequestID(next[1]).ID)
		require.Equal(t, "3", insp.RequestID(next[2]).ID)
		require.Len(t, next, 3)
		require.False(t, full)

		err = pool.RemoveRequest(req24)
		require.NoError(t, err)

		next, _ = pool.NextRequests(4, 10000000, false)
		require.Equal(t, "23", insp.RequestID(next[0]).ID)
		require.Equal(t, "3", insp.RequestID(next[1]).ID)
		require.Len(t, next, 2)
		require.False(t, full)

		next, _ = pool.NextRequests(4, 10000000, true)
		require.Nil(t, next)
		require.False(t, full)

		next, full = pool.NextRequests(1, 10000000, false)
		require.Equal(t, "23", insp.RequestID(next[0]).ID)
		require.Len(t, next, 1)
		require.True(t, full)

		err = pool.RemoveRequest(req23)
		require.NoError(t, err)

		req3 := types.RequestInfo{
			ID:       "3",
			ClientID: "3",
		}

		err = pool.RemoveRequest(req3)
		require.NoError(t, err)

		next, _ = pool.NextRequests(1, 10000000, false)
		require.Empty(t, next)

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		pool.Close()
	})
}

func TestReqPoolCapacity(t *testing.T) {
	numReq := 100
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)

	t.Run("submit storm", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}
		pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 50, ForwardTimeout: time.Hour}, submittedChan)
		defer pool.Close()

		wg := sync.WaitGroup{}
		wg.Add(2 * numReq)
		for i := range numReq {
			go func(i string) {
				byteReq := makeTestRequest(i, i, "foo")
				err := pool.Submit(byteReq)

				if err == nil ||
					errors.Is(err, bft.ErrReqAlreadyProcessed) {
					wg.Done()
				}
			}(strconv.Itoa(i))
		}

		time.Sleep(10 * time.Millisecond)

		for i := range numReq {
			go func(i string) {
				req := types.RequestInfo{
					ID:       i,
					ClientID: i,
				}
				_ = pool.RemoveRequest(req)
				wg.Done()
			}(strconv.Itoa(i))
		}
		wg.Wait()

		require.Equal(t, 0, pool.Size())
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", .0)
		pool.Close()
	})

	t.Run("respect max count and size", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}
		pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 50, ForwardTimeout: time.Hour}, submittedChan)
		defer pool.Close()

		var lengths []int
		var totalLength int
		for i := range 10 {
			r := makeTestRequest("1", strconv.Itoa(i), "aaaaaa")
			lengths = append(lengths, len(r))
			err := pool.Submit(r)
			require.NoError(t, err)
			totalLength += len(r)
		}

		next, full := pool.NextRequests(5, 10000000, false)
		require.Len(t, next, 5)
		require.True(t, full)

		next, full = pool.NextRequests(20, 10000000, false)
		require.Len(t, next, 10)
		require.False(t, full)

		next, full = pool.NextRequests(20, uint64(lengths[0]+lengths[1]), false)
		require.Len(t, next, 2)
		require.True(t, full)

		next, full = pool.NextRequests(20, uint64(lengths[0]+lengths[1]+lengths[2]+lengths[3]/2), false)
		require.Len(t, next, 3)
		require.True(t, full)

		next, full = pool.NextRequests(4, uint64(lengths[0]+lengths[1]+lengths[2]+lengths[3]), false)
		require.Len(t, next, 4)
		require.True(t, full)

		next, full = pool.NextRequests(20, uint64(totalLength), false)
		require.Len(t, next, 10)
		require.True(t, full)

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		pool.Close()
	})
}

func TestReqPoolPrune(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "bar")
	pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 3, ForwardTimeout: time.Hour}, submittedChan)
	defer pool.Close()

	require.Equal(t, 0, pool.Size())

	err = pool.Submit(byteReq1)
	require.NoError(t, err)
	err = pool.Submit(byteReq2)
	require.NoError(t, err)
	require.Equal(t, 2, pool.Size())

	pool.Prune(func(payload []byte) error {
		if bytes.Equal(byteReq1, payload) {
			return errors.New("revoked")
		}
		return nil
	})

	require.Equal(t, 1, pool.Size())
	req, _ := pool.NextRequests(1, 10000000, false)
	client, tx, _ := parseTestRequest(req[0])
	require.Equal(t, "2", client)
	require.Equal(t, "2", tx)

	timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
	timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
	pool.Close()
}

func TestReqPoolTimeout(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo")

	insp := &testRequestInspector{}
	submittedChan := make(chan struct{}, 1)

	t.Run("request size too big", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Return()
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Return()
		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    10 * time.Millisecond,
				ComplainTimeout:   time.Hour,
				AutoRemoveTimeout: time.Hour,
				RequestMaxBytes:   1024,
			},
			nil,
		)
		defer pool.Close()

		payload := make([]byte, 2048)
		rand.Read(payload)
		request := makeTestRequest("1", "1", string(payload))
		require.Equal(t, 0, pool.Size())
		err = pool.Submit(request)
		require.Contains(t, err.Error(), "is bigger than request max bytes")
	})
	t.Run("change request size", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Return()
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Return()
		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    10 * time.Millisecond,
				ComplainTimeout:   time.Hour,
				AutoRemoveTimeout: time.Hour,
				RequestMaxBytes:   1024,
			},
			nil,
		)
		defer pool.Close()

		payload := make([]byte, 2048)
		rand.Read(payload)
		request := makeTestRequest("1", "1", string(payload))
		require.Equal(t, 0, pool.Size())
		err = pool.Submit(request)
		require.Contains(t, err.Error(), "is bigger than request max bytes")

		pool.StopTimers()
		opts := bft.PoolOptions{
			ForwardTimeout:    10 * time.Millisecond,
			ComplainTimeout:   time.Hour,
			AutoRemoveTimeout: time.Hour,
			RequestMaxBytes:   1024 * 3,
		}
		pool.ChangeOptions(timeoutHandler, opts)
		pool.RestartTimers()

		err = pool.Submit(request)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())
	})
	t.Run("request timeout", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnLeaderFwdRequestTimeout")
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    10 * time.Millisecond,
				ComplainTimeout:   time.Hour,
				AutoRemoveTimeout: time.Hour,
			},
			submittedChan,
		)
		defer pool.Close()

		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())

		toWG.Wait()

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq1))
		require.NoError(t, err)
	})

	t.Run("leader fwd timeout", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		to1WG := &sync.WaitGroup{}
		to1WG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to1WG.Done()
		}).Return()

		to2WG := &sync.WaitGroup{}
		to2WG.Add(1)
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to2WG.Done()
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    10 * time.Millisecond,
				ComplainTimeout:   10 * time.Millisecond,
				AutoRemoveTimeout: time.Hour,
			},
			submittedChan,
		)
		defer pool.Close()

		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())

		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		to2WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq1))
		require.NoError(t, err)
	})

	t.Run("auto remove", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		to1WG := &sync.WaitGroup{}
		to1WG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to1WG.Done()
		}).Return()

		to2WG := &sync.WaitGroup{}
		to2WG.Add(1)
		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to2WG.Done()
		}).Return()

		to3WG := &sync.WaitGroup{}
		to3WG.Add(1)
		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			to3WG.Done()
		}).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    10 * time.Millisecond,
				ComplainTimeout:   10 * time.Millisecond,
				AutoRemoveTimeout: 10 * time.Millisecond,
			},
			submittedChan,
		)
		defer pool.Close()

		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())

		to1WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		to2WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 1)
		to3WG.Wait()
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 1)

		require.Equal(t, 0, pool.Size())
	})

	t.Run("stop restart", func(t *testing.T) {
		timeoutHandler := &mocks.RequestTimeoutHandler{}

		toWG := &sync.WaitGroup{}
		toWG.Add(1)
		timeoutHandler.On("OnRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnLeaderFwdRequestTimeout")
		}).Return()
		timeoutHandler.On("OnRequestTimeout", byteReq2, insp.RequestID(byteReq2)).Run(func(args mock.Arguments) {
			toWG.Done()
		}).Return()

		timeoutHandler.On("OnLeaderFwdRequestTimeout", byteReq1, insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnLeaderFwdRequestTimeout")
		}).Return()

		timeoutHandler.On("OnAutoRemoveTimeout", insp.RequestID(byteReq1)).Run(func(args mock.Arguments) {
			require.Fail(t, "called OnAutoRemoveTimeout")
		}).Return()

		pool := bft.NewPool(log, insp, timeoutHandler,
			bft.PoolOptions{
				QueueSize:         3,
				ForwardTimeout:    100 * time.Millisecond,
				ComplainTimeout:   time.Hour,
				AutoRemoveTimeout: time.Hour,
			},
			submittedChan,
		)
		defer pool.Close()
		require.Equal(t, 0, pool.Size())
		err = pool.Submit(byteReq1)
		require.NoError(t, err)
		require.Equal(t, 1, pool.Size())

		pool.StopTimers()
		time.Sleep(500 * time.Millisecond)

		err = pool.RemoveRequest(insp.RequestID(byteReq1))
		require.NoError(t, err)
		err = pool.Submit(byteReq2)
		require.NoError(t, err)

		pool.RestartTimers()
		err = pool.Submit(byteReq2)
		require.Equal(t, bft.ErrReqAlreadyExists, err)
		pool.StopTimers()
		pool.RestartTimers()

		toWG.Wait()

		timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 1)
		timeoutHandler.AssertNumberOfCalls(t, "OnLeaderFwdRequestTimeout", 0)
		timeoutHandler.AssertNumberOfCalls(t, "OnAutoRemoveTimeout", 0)

		err := pool.RemoveRequest(insp.RequestID(byteReq2))
		require.NoError(t, err)
	})
}

func TestMakeRequest(t *testing.T) {
	r := makeTestRequest("AB", "CDE", "FGHI")
	require.Len(t, r, 21)

	clientID, txID, data := parseTestRequest(r)
	require.Equal(t, "AB", clientID)
	require.Equal(t, "CDE", txID)
	require.Equal(t, "FGHI", data)

	ins := &testRequestInspector{}
	info := ins.RequestID(r)
	require.Equal(t, "AB", info.ClientID)
	require.Equal(t, "CDE", info.ID)
}

func makeTestRequest(clientID, txID, data string) []byte {
	buffLen := make([]byte, 4)
	buff := make([]byte, 12)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(clientID)))
	buff = append(buff[0:0], buffLen...)
	buff = append(buff, []byte(clientID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(txID)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(txID)...)

	binary.LittleEndian.PutUint32(buffLen, uint32(len(data)))
	buff = append(buff, buffLen...)
	buff = append(buff, []byte(data)...)

	return buff
}

func parseTestRequest(request []byte) (clientID, txID, data string) {
	l := binary.LittleEndian.Uint32(request)
	buff := request[4:]

	clientID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	txID = string(buff[:l])
	buff = buff[l:]

	l = binary.LittleEndian.Uint32(buff)
	buff = buff[4:]

	data = string(buff[:l])

	return
}

type testRequestInspector struct{}

func (ins *testRequestInspector) RequestID(req []byte) types.RequestInfo {
	var info types.RequestInfo
	info.ClientID, info.ID, _ = parseTestRequest(req)
	return info
}

// TestReqPoolSizeBytesMetric makes sure the pool reports the total size in bytes of the requests it holds.
func TestReqPoolSizeBytesMetric(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}
	recorder := &metricsRecorder{}
	sizeBytes := func() float64 { return recorder.gauge("pool_size_bytes_of_elements") }

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo-bar-foo-bar")

	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{
		QueueSize:      3,
		ForwardTimeout: time.Hour,
		Metrics:        api.NewMetricsRequestPool(recorder),
	}, make(chan struct{}, 1))
	defer pool.Close()

	require.Zero(t, sizeBytes())

	require.NoError(t, pool.Submit(byteReq1))
	require.InDelta(t, float64(len(byteReq1)), sizeBytes(), 0)

	require.NoError(t, pool.Submit(byteReq2))
	require.InDelta(t, float64(len(byteReq1)+len(byteReq2)), sizeBytes(), 0)

	// A request that is already in the pool does not change the size.
	require.ErrorIs(t, pool.Submit(byteReq1), bft.ErrReqAlreadyExists)
	require.InDelta(t, float64(len(byteReq1)+len(byteReq2)), sizeBytes(), 0)

	require.NoError(t, pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"}))
	require.InDelta(t, float64(len(byteReq2)), sizeBytes(), 0)

	require.NoError(t, pool.RemoveRequest(types.RequestInfo{ID: "2", ClientID: "2"}))
	require.Zero(t, sizeBytes())
}

// TestReqPoolSizeGetters makes sure the lock-free Size and SizeBytes mirrors follow every addition and removal,
// including Close, and that OldestAge reports the age of the request at the front of the queue.
func TestReqPoolSizeGetters(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	byteReq1 := makeTestRequest("1", "1", "foo")
	byteReq2 := makeTestRequest("2", "2", "foo-bar-foo-bar")

	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 3, ForwardTimeout: time.Hour}, make(chan struct{}, 1))

	require.Equal(t, 0, pool.Size())
	require.Zero(t, pool.SizeBytes())
	require.Zero(t, pool.OldestAge())

	require.NoError(t, pool.Submit(byteReq1))
	require.Equal(t, 1, pool.Size())
	require.Equal(t, uint64(len(byteReq1)), pool.SizeBytes())
	time.Sleep(20 * time.Millisecond)
	age1 := pool.OldestAge()
	require.GreaterOrEqual(t, age1, 20*time.Millisecond)

	// The second request is younger; the oldest is still the first one.
	require.NoError(t, pool.Submit(byteReq2))
	require.Equal(t, 2, pool.Size())
	require.Equal(t, uint64(len(byteReq1)+len(byteReq2)), pool.SizeBytes())
	require.GreaterOrEqual(t, pool.OldestAge(), age1)

	// A duplicate changes nothing.
	require.ErrorIs(t, pool.Submit(byteReq1), bft.ErrReqAlreadyExists)
	require.Equal(t, 2, pool.Size())
	require.Equal(t, uint64(len(byteReq1)+len(byteReq2)), pool.SizeBytes())

	// Removing the oldest makes the second one the front of the queue, which is younger.
	require.NoError(t, pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"}))
	require.Equal(t, 1, pool.Size())
	require.Equal(t, uint64(len(byteReq2)), pool.SizeBytes())
	require.Less(t, pool.OldestAge(), age1)

	// Removing a request that is not there changes nothing.
	require.Error(t, pool.RemoveRequest(types.RequestInfo{ID: "1", ClientID: "1"}))
	require.Equal(t, 1, pool.Size())

	// Close empties the pool: the count mirror follows (bytes are not accounted by Close, as before).
	pool.Close()
	require.Equal(t, 0, pool.Size())
	require.Zero(t, pool.OldestAge())
}

// TestReqPoolSizeMirrorUnderConcurrency makes sure the Size mirror agrees with the pool after concurrent
// submissions and removals.
func TestReqPoolSizeMirrorUnderConcurrency(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()
	insp := &testRequestInspector{}

	pool := bft.NewPool(log, insp, noopTimeoutHandler, bft.PoolOptions{QueueSize: 1000, ForwardTimeout: time.Hour}, make(chan struct{}, 1))
	defer pool.Close()

	const workers, perWorker = 8, 100
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				id := strconv.Itoa(w*perWorker + i)
				require.NoError(t, pool.Submit(makeTestRequest(id, id, "foo")))
				if i%2 == 0 {
					require.NoError(t, pool.RemoveRequest(types.RequestInfo{ID: id, ClientID: id}))
				}
			}
		})
	}
	wg.Wait()

	require.Equal(t, workers*perWorker/2, pool.Size())
	batch, _ := pool.NextRequests(workers*perWorker, 1<<30, false)
	require.Len(t, batch, pool.Size())
	var bytes uint64
	for _, req := range batch {
		bytes += uint64(len(req))
	}
	require.Equal(t, bytes, pool.SizeBytes())
}

// TestReqPoolReserve covers the reservation of requests that are part of a proposal in flight:
// NextRequests skips reserved requests (and counts only the unreserved ones when checking whether a batch is
// full), a removed request releases its reservation, UnreserveAll releases all of them, and Size counts the
// reserved requests all along since they still occupy the pool.
func TestReqPoolReserve(t *testing.T) {
	basicLog, err := zap.NewDevelopment()
	require.NoError(t, err)
	log := basicLog.Sugar()

	insp := &testRequestInspector{}
	timeoutHandler := &mocks.RequestTimeoutHandler{}
	submittedChan := make(chan struct{}, 1)
	pool := bft.NewPool(log, insp, timeoutHandler, bft.PoolOptions{QueueSize: 20, ForwardTimeout: time.Hour}, submittedChan)
	defer pool.Close()

	const total = 12
	infos := make([]types.RequestInfo, 0, total)
	for i := 1; i <= total; i++ {
		id := strconv.Itoa(i)
		require.NoError(t, pool.Submit(makeTestRequest(id, id, "foo")))
		infos = append(infos, types.RequestInfo{ID: id, ClientID: id})
	}
	require.Equal(t, total, pool.Size())

	ids := func(batch [][]byte) []string {
		res := make([]string, 0, len(batch))
		for _, req := range batch {
			res = append(res, insp.RequestID(req).ID)
		}
		return res
	}

	// Without reservations the first 10 requests make a full batch.
	next, full := pool.NextRequests(10, 10000000, true)
	require.Len(t, next, 10)
	require.True(t, full)

	// Reserve the first 10 (the proposal in flight); reserving unknown or already reserved requests is a no-op.
	pool.Reserve(infos[:10])
	pool.Reserve(infos[:10])
	pool.Reserve([]types.RequestInfo{{ID: "unknown", ClientID: "unknown"}})
	require.Equal(t, total, pool.Size(), "reserved requests still occupy the pool")

	// With check on, only 2 unreserved requests remain, which is less than the batch size.
	next, full = pool.NextRequests(10, 10000000, true)
	require.Nil(t, next)
	require.False(t, full)

	// Without the check the batch holds only the unreserved requests, in FIFO order.
	next, full = pool.NextRequests(10, 10000000, false)
	require.Equal(t, []string{"11", "12"}, ids(next))
	require.False(t, full)

	// Two unreserved requests make a full batch of two.
	next, full = pool.NextRequests(2, 10000000, true)
	require.Equal(t, []string{"11", "12"}, ids(next))
	require.True(t, full)

	// Removing a reserved request (its proposal was delivered) releases its reservation.
	require.NoError(t, pool.RemoveRequest(infos[0]))
	require.Equal(t, total-1, pool.Size())
	next, _ = pool.NextRequests(10, 10000000, false)
	require.Equal(t, []string{"11", "12"}, ids(next))

	// Releasing every reservation makes the remaining requests available again, in FIFO order.
	pool.UnreserveAll()
	next, full = pool.NextRequests(10, 10000000, true)
	require.Equal(t, []string{"2", "3", "4", "5", "6", "7", "8", "9", "10", "11"}, ids(next))
	require.True(t, full)
	require.Equal(t, total-1, pool.Size())

	// A second UnreserveAll with nothing reserved is a no-op, and reserving again works.
	pool.UnreserveAll()
	pool.Reserve(infos[1:3])
	next, _ = pool.NextRequests(10, 10000000, false)
	require.Equal(t, []string{"4", "5", "6", "7", "8", "9", "10", "11", "12"}, ids(next))

	timeoutHandler.AssertNumberOfCalls(t, "OnRequestTimeout", 0)
}
