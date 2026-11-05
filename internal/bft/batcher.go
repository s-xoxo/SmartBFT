// Copyright IBM Corp. All Rights Reserved.
//
// SPDX-License-Identifier: Apache-2.0
//

package bft

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/hyperledger/SmartBFT/pkg/api"
	"github.com/hyperledger/SmartBFT/pkg/metrics/disabled"
)

// BatchParams are the parameters NextBatch reads once per call.
type BatchParams struct {
	// MaxMsgCount is the maximal number of requests in a batch (the same type as RequestBatchMaxCount).
	MaxMsgCount uint64
	// MaxSizeBytes is the maximal total size of the requests in a batch.
	MaxSizeBytes uint64
	// Timeout is the maximal time NextBatch waits for the batch to fill before returning it.
	Timeout time.Duration
}

// BatchBuilder implements Batcher
type BatchBuilder struct {
	pool          RequestPool
	submittedChan chan struct{}
	params        atomic.Pointer[BatchParams]
	closeChan     chan struct{}
	closeLock     sync.Mutex // Reset and Close may be called by different threads
	metrics       *api.MetricsBatcher
}

// NewBatchBuilder creates a new BatchBuilder.
// A nil metrics disables the batcher metrics.
func NewBatchBuilder(pool RequestPool, submittedChan chan struct{}, maxMsgCount uint64, maxSizeBytes uint64, batchTimeout time.Duration, metrics *api.MetricsBatcher) *BatchBuilder {
	if metrics == nil {
		metrics = api.NewMetricsBatcher(&disabled.Provider{})
	}
	b := &BatchBuilder{
		pool:          pool,
		submittedChan: submittedChan,
		closeChan:     make(chan struct{}),
		metrics:       metrics,
	}
	b.publishParams(&BatchParams{MaxMsgCount: maxMsgCount, MaxSizeBytes: maxSizeBytes, Timeout: batchTimeout})
	return b
}

// NextBatch returns the next batch of requests to be proposed.
// The method returns as soon as the batch is full, in terms of request count or total size, or after a timeout.
// The method may block.
// The batch parameters are read once, when the method is called; a SetBatchParams made while the method
// is waiting takes effect on the next call.
func (b *BatchBuilder) NextBatch() [][]byte {
	p := b.params.Load()
	maxCount := int(p.MaxMsgCount)

	currBatch, full := b.pool.NextRequests(maxCount, p.MaxSizeBytes, true)
	if full {
		b.countBatch(currBatch, api.ReasonBatchFull)
		return currBatch
	}

	timeout := time.After(p.Timeout)

	for {
		select {
		case <-b.closeChan:
			return nil
		case <-timeout:
			currBatch, _ = b.pool.NextRequests(maxCount, p.MaxSizeBytes, false)
			b.countBatch(currBatch, api.ReasonBatchTimeout)
			return currBatch
		case <-b.submittedChan:
			// there is a possibility to extend the current batch
			currBatch, full = b.pool.NextRequests(maxCount, p.MaxSizeBytes, true)
			if full {
				b.countBatch(currBatch, api.ReasonBatchFull)
				return currBatch
			}
		}
	}
}

// SetBatchParams replaces the parameters the next NextBatch call reads. A call of NextBatch that is
// already in progress keeps the parameters it loaded when it started. Zero values are normalised:
// MaxMsgCount 0 -> 1, MaxSizeBytes 0 -> 1, Timeout <= 0 -> the previous Timeout.
// The method only publishes what it is given; keeping the parameters within policy bounds is the caller's job.
func (b *BatchBuilder) SetBatchParams(p BatchParams) {
	prev := b.params.Load()
	if p.MaxMsgCount == 0 {
		p.MaxMsgCount = 1
	}
	if p.MaxSizeBytes == 0 {
		p.MaxSizeBytes = 1
	}
	if p.Timeout <= 0 {
		p.Timeout = prev.Timeout
	}
	b.publishParams(&p)
}

// BatchParams returns the parameters the next NextBatch call will read.
func (b *BatchBuilder) BatchParams() BatchParams {
	return *b.params.Load()
}

// publishParams stores a new, immutable parameter set so that a reader always sees the three values as one
// consistent set, and reports the current count and timeout.
func (b *BatchBuilder) publishParams(p *BatchParams) {
	b.params.Store(p)
	b.metrics.MaxCount.Set(float64(p.MaxMsgCount))
	b.metrics.TimeoutSeconds.Set(p.Timeout.Seconds())
}

// countBatch counts a returned batch by the reason it was closed.
// An empty batch, returned when the timeout expires on an idle pool, is not a batch and is not counted.
func (b *BatchBuilder) countBatch(batch [][]byte, reason string) {
	if len(batch) == 0 {
		return
	}
	b.metrics.CountOfBatches.With(
		b.metrics.LabelsForWith("reason", reason)...,
	).Add(1)
}

// Close closes the close channel to stop NextBatch
func (b *BatchBuilder) Close() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	select {
	case <-b.closeChan:
		return
	default:
	}
	close(b.closeChan)
}

// Closed returns true if the batcher is closed. The proposer goroutine asks while the controller may be
// resetting the batcher for a new view, so the channel is read under the same lock Reset replaces it under.
func (b *BatchBuilder) Closed() bool {
	b.closeLock.Lock()
	closeChan := b.closeChan
	b.closeLock.Unlock()
	select {
	case <-closeChan:
		return true
	default:
		return false
	}
}

// Reset reopens the close channel to allow calling NextBatch
func (b *BatchBuilder) Reset() {
	b.closeLock.Lock()
	defer b.closeLock.Unlock()
	b.closeChan = make(chan struct{})
}
