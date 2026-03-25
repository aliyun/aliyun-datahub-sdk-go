package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type offsetManagerMockClient struct {
	DataHubApi
	mu                 sync.Mutex
	callCount          map[string]int
	openSessionResult  *OpenSubscriptionSessionResult
	commitOffsetResult *CommitSubscriptionOffsetResult
}

func newOffsetManagerMockClient() *offsetManagerMockClient {
	return &offsetManagerMockClient{
		callCount: make(map[string]int),
	}
}

func (m *offsetManagerMockClient) incrementCallCount(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount[method]++
}

func (m *offsetManagerMockClient) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[method]
}

func (m *offsetManagerMockClient) OpenSubscriptionSession(projectName, topicName, subId string, shardIds []string) (*OpenSubscriptionSessionResult, error) {
	m.incrementCallCount("OpenSubscriptionSession")
	if m.openSessionResult != nil {
		return m.openSessionResult, nil
	}
	offsets := make(map[string]SubscriptionOffset)
	for _, shardId := range shardIds {
		offsets[shardId] = SubscriptionOffset{
			Sequence:   0,
			BatchIndex: 0,
			Timestamp:  0,
			VersionId:  -1,
		}
	}
	return &OpenSubscriptionSessionResult{
		Offsets: offsets,
	}, nil
}

func (m *offsetManagerMockClient) CommitSubscriptionOffset(projectName, topicName, subId string, offsets map[string]SubscriptionOffset) (*CommitSubscriptionOffsetResult, error) {
	m.incrementCallCount("CommitSubscriptionOffset")
	if m.commitOffsetResult != nil {
		return m.commitOffsetResult, nil
	}
	return &CommitSubscriptionOffsetResult{}, nil
}

func TestOffsetManager(t *testing.T) {
	mockClient := newOffsetManagerMockClient()

	om := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	om.start()
	defer om.stop()

	// Test addShards
	shardIds := []string{"0", "1"}
	offsets := om.addShards(shardIds)
	assert.NotNil(t, offsets)

	// Verify offset
	offset0 := om.getOffset("0")
	assert.Equal(t, int64(0), offset0.Sequence)

	offset1 := om.getOffset("1")
	assert.Equal(t, int64(0), offset1.Sequence)
}

func TestOffsetManagerRecordKey(t *testing.T) {
	mockClient := newOffsetManagerMockClient()

	om := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	om.start()
	defer om.stop()

	// Add shard first
	om.addShards([]string{"0"})

	// Create record key
	rk := newRecordKey("0", 100, 0, 1000)
	assert.NotNil(t, rk)
	assert.False(t, rk.isAcked())

	// Record the key
	om.appendRecordKey(rk)

	// Ack the key
	rk.Ack()
	assert.True(t, rk.isAcked())
}

func TestOffsetManagerCommit(t *testing.T) {
	mockClient := newOffsetManagerMockClient()

	om := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 100*time.Millisecond)
	om.start()
	defer om.stop()

	// Add shard
	om.addShards([]string{"0"})

	// Create and ack records
	rk1 := newRecordKey("0", 0, 0, 1000)
	om.appendRecordKey(rk1)
	rk1.Ack()

	rk2 := newRecordKey("0", 1, 0, 2000)
	om.appendRecordKey(rk2)
	rk2.Ack()

	// Wait for commit
	time.Sleep(200 * time.Millisecond)

	// Verify commit was called
	assert.GreaterOrEqual(t, mockClient.GetCallCount("CommitSubscriptionOffset"), 1)
}

func TestRecordKeyAck(t *testing.T) {
	rk := newRecordKey("0", 100, 0, 1000)

	assert.False(t, rk.isAcked())

	rk.Ack()
	assert.True(t, rk.isAcked())

	// Ack is idempotent
	rk.Ack()
	assert.True(t, rk.isAcked())
}

func TestCalculateCommitOffset(t *testing.T) {
	mockClient := newOffsetManagerMockClient()
	om := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)

	// Add shard
	om.addShards([]string{"0"})

	info := om.shardInfos["0"]
	info.lastCommit.sequence = 0
	info.lastCommit.batchIndex = 0
	info.lastCommit.timestamp = 0

	// Create record keys
	rk1 := newRecordKey("0", 10, 0, 1000)
	rk2 := newRecordKey("0", 10, 1, 1001)
	rk3 := newRecordKey("0", 11, 0, 2000)

	om.appendRecordKey(rk1)
	om.appendRecordKey(rk2)
	om.appendRecordKey(rk3)

	// No ack, should return lastCommit
	seq, batch, ts := om.calculateCommitOffset(info)
	assert.Equal(t, int64(0), seq)
	assert.Equal(t, uint32(0), batch)
	assert.Equal(t, int64(0), ts)

	// Ack first one
	rk1.Ack()
	seq, batch, ts = om.calculateCommitOffset(info)
	assert.Equal(t, int64(10), seq)
	assert.Equal(t, uint32(0), batch)
	assert.Equal(t, int64(1000), ts)
	assert.Len(t, info.pendingQueue, 2) // rk2, rk3

	// Ack second one
	rk2.Ack()
	seq, batch, ts = om.calculateCommitOffset(info)
	assert.Equal(t, int64(10), seq)
	assert.Equal(t, uint32(1), batch)
	assert.Equal(t, int64(1001), ts)
	assert.Len(t, info.pendingQueue, 1) // rk3

	// Ack third one
	rk3.Ack()
	seq, batch, ts = om.calculateCommitOffset(info)
	assert.Equal(t, int64(11), seq)
	assert.Equal(t, uint32(0), batch)
	assert.Equal(t, int64(2000), ts)
	assert.Len(t, info.pendingQueue, 0)
}

func TestCalculateCommitOffsetOutOfOrder(t *testing.T) {
	mockClient := newOffsetManagerMockClient()
	om := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)

	// Add shard
	om.addShards([]string{"0"})

	info := om.shardInfos["0"]
	info.lastCommit.sequence = 0
	info.lastCommit.batchIndex = 0
	info.lastCommit.timestamp = 0

	// Create record keys
	rk1 := newRecordKey("0", 10, 0, 1000)
	rk2 := newRecordKey("0", 10, 1, 1001)
	rk3 := newRecordKey("0", 11, 0, 2000)

	om.appendRecordKey(rk1)
	om.appendRecordKey(rk2)
	om.appendRecordKey(rk3)

	// Ack out of order - ack rk2 first
	rk2.Ack()
	seq, batch, ts := om.calculateCommitOffset(info)
	assert.Equal(t, int64(0), seq) // rk1 not acked, so no progress
	assert.Equal(t, uint32(0), batch)
	assert.Equal(t, int64(0), ts)
	assert.Len(t, info.pendingQueue, 3) // all remain

	// Ack rk1
	rk1.Ack()
	seq, batch, ts = om.calculateCommitOffset(info)
	assert.Equal(t, int64(10), seq)
	assert.Equal(t, uint32(1), batch) // rk1 and rk2 both acked
	assert.Equal(t, int64(1001), ts)
	assert.Len(t, info.pendingQueue, 1) // rk3

	// Ack rk3
	rk3.Ack()
	seq, batch, ts = om.calculateCommitOffset(info)
	assert.Equal(t, int64(11), seq)
	assert.Equal(t, uint32(0), batch)
	assert.Equal(t, int64(2000), ts)
	assert.Len(t, info.pendingQueue, 0)
}
