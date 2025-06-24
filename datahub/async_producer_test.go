package datahub

import (
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
