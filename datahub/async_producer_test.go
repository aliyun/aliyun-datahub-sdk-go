package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultParitionFuncWithExtend(t *testing.T) {

	topic := &GetTopicResult{
		ExpandMode: ONLY_EXTEND,
	}

	shards := make([]ShardEntry, 0)
	shards = append(shards, ShardEntry{
		ShardId:      "0",
		BeginHashKey: "00000000000000000000000000000000",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "1",
		BeginHashKey: "55555555555555555555555555555555",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "2",
		BeginHashKey: "99999999999999999999999999999999",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "3",
		BeginHashKey: "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
	})

	record := NewBlobRecord([]byte("test"))
	shardId := DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "")

	record.SetShardId("2")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "2")

	record.SetShardId("")
	record.SetPartitionKey("abcd")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "1")

	record.SetPartitionKey("test1")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "0")
}

func TestDefaultParitionFuncWithSplit(t *testing.T) {
	topic := &GetTopicResult{
		ExpandMode: SPLIT_EXTEND,
	}

	shards := make([]ShardEntry, 0)
	shards = append(shards, ShardEntry{
		ShardId:      "0",
		BeginHashKey: "00000000000000000000000000000000",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "1",
		BeginHashKey: "55555555555555555555555555555555",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "2",
		BeginHashKey: "99999999999999999999999999999999",
	})
	shards = append(shards, ShardEntry{
		ShardId:      "3",
		BeginHashKey: "EEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE",
	})

	record := NewBlobRecord([]byte("test"))
	shardId := DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "")

	record.SetShardId("2")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "2")

	record.SetShardId("")
	record.SetPartitionKey("abcd")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "2")

	record.SetPartitionKey("test1")
	shardId = DefaultPartitionFunc(topic, shards, record)
	assert.Equal(t, shardId, "1")
}

func TestBufferHelper(t *testing.T) {
	buffer := newBufferHelper(3, 2, time.Second*2)

	buffer.input() <- NewBlobRecord(nil)
	buffer.input() <- NewBlobRecord(nil)
	buffer.input() <- NewBlobRecord(nil)

	// wait record flush to batch
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, len(buffer.output()), 1)
	batch := <-buffer.output()
	assert.Equal(t, len(batch), 3)

	buffer.input() <- NewBlobRecord(nil)
	time.Sleep(time.Millisecond * 1000)
	assert.Equal(t, len(buffer.output()), 0)
	time.Sleep(time.Millisecond * 1100)
	assert.Equal(t, len(buffer.output()), 1)
	batch1 := <-buffer.output()
	assert.Equal(t, len(batch1), 1)

	buffer.batchInput() <- batch1
	batch2 := <-buffer.output()
	assert.Equal(t, len(batch2), 1)
}

func TestAsyncProducerFlushWaitsUntilPendingBatchEnqueued(t *testing.T) {
	cfg := NewProducerConfig()
	cfg.Parittioner = nil
	cfg.MaxAsyncBufferNum = 10
	cfg.MaxAsyncFlightingNum = 0
	cfg.MaxAsyncBufferTime = time.Hour

	producer := NewAsyncProducer(cfg)
	ap := producer.(*asyncProducerImpl)
	ap.wg.Add(1)
	go ap.dispatch()

	producer.Input() <- NewBlobRecord([]byte("test"))
	flushDone := make(chan struct{})
	go func() {
		producer.Flush()
		close(flushDone)
	}()

	returnedBeforeEnqueue := false
	select {
	case <-flushDone:
		returnedBeforeEnqueue = true
	case <-time.After(50 * time.Millisecond):
	}

	batch := <-ap.buffer.output()
	assert.Len(t, batch, 1)

	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("Flush did not return after the pending batch was enqueued")
	}
	assert.False(t, returnedBeforeEnqueue, "Flush returned before the pending batch was enqueued")

	close(ap.input)
	ap.wg.Wait()
}

func TestAsyncProducerFlushWaitsUntilPendingShardBatchEnqueued(t *testing.T) {
	cfg := NewProducerConfig()
	cfg.Parittioner = func(*GetTopicResult, []ShardEntry, IRecord) string {
		return "0"
	}
	cfg.MaxAsyncBufferNum = 10
	cfg.MaxAsyncFlightingNum = 0
	cfg.MaxAsyncBufferTime = time.Hour

	producer := NewAsyncProducer(cfg)
	ap := producer.(*asyncProducerImpl)
	writer := newShardWriter(cfg, "0", nil, nil, nil, nil, nil)
	ap.shards = []ShardEntry{{ShardId: "0"}}
	ap.writers = map[string]*shardWriter{"0": writer}
	ap.wg.Add(1)
	go ap.dispatch()

	producer.Input() <- NewBlobRecord([]byte("test"))
	flushDone := make(chan struct{})
	go func() {
		producer.Flush()
		close(flushDone)
	}()

	returnedBeforeEnqueue := false
	select {
	case <-flushDone:
		returnedBeforeEnqueue = true
	case <-time.After(50 * time.Millisecond):
	}

	batch := <-writer.buffer.output()
	assert.Len(t, batch, 1)

	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("Flush did not return after the pending shard batch was enqueued")
	}
	assert.False(t, returnedBeforeEnqueue, "Flush returned before the pending shard batch was enqueued")

	close(ap.input)
	ap.wg.Wait()
	writer.buffer.close()
}

func TestAsyncProducerFlushPendingIncludesRetries(t *testing.T) {
	cfg := NewProducerConfig()
	cfg.Parittioner = nil
	cfg.MaxAsyncBufferNum = 10
	cfg.MaxAsyncBufferTime = time.Hour

	ap := NewAsyncProducer(cfg).(*asyncProducerImpl)
	ap.retries <- []IRecord{NewBlobRecord([]byte("retry"))}
	ap.flushPending()

	select {
	case batch := <-ap.buffer.output():
		assert.Len(t, batch, 1)
	case <-time.After(time.Second):
		t.Fatal("Flush did not trigger records waiting for retry")
	}

	ap.buffer.close()
}

func TestAsyncProducerFlushReturnsWithEmptyBuffers(t *testing.T) {
	cfg := NewProducerConfig()
	cfg.MaxAsyncFlightingNum = 0

	producer := NewAsyncProducer(cfg)
	ap := producer.(*asyncProducerImpl)
	ap.wg.Add(1)
	go ap.dispatch()

	flushDone := make(chan struct{})
	go func() {
		producer.Flush()
		close(flushDone)
	}()

	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("Flush blocked with empty buffers")
	}

	close(ap.input)
	ap.wg.Wait()
}

func TestAsyncProducerConcurrentFlushReturnsAfterPendingBatchEnqueued(t *testing.T) {
	cfg := NewProducerConfig()
	cfg.Parittioner = nil
	cfg.MaxAsyncBufferNum = 10
	cfg.MaxAsyncFlightingNum = 1
	cfg.MaxAsyncBufferTime = time.Hour

	producer := NewAsyncProducer(cfg)
	ap := producer.(*asyncProducerImpl)
	ap.wg.Add(1)
	go ap.dispatch()

	producer.Input() <- NewBlobRecord([]byte("test"))

	const flushCount = 8
	start := make(chan struct{})
	flushDone := make(chan struct{})
	var ready sync.WaitGroup
	var wg sync.WaitGroup
	ready.Add(flushCount)
	wg.Add(flushCount)
	for i := 0; i < flushCount; i++ {
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			producer.Flush()
		}()
	}
	go func() {
		wg.Wait()
		close(flushDone)
	}()

	ready.Wait()
	close(start)
	select {
	case <-flushDone:
	case <-time.After(time.Second):
		t.Fatal("concurrent Flush calls did not all return")
	}

	select {
	case batch := <-ap.buffer.output():
		assert.Len(t, batch, 1)
	case <-time.After(time.Second):
		t.Fatal("concurrent Flush did not enqueue the pending batch")
	}

	close(ap.input)
	ap.wg.Wait()
}
