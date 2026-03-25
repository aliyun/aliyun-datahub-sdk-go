package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type shardGroupReaderMockClient struct {
	DataHubApi
	mu                 sync.Mutex
	callCount          map[string]int
	getCursorResult    *GetCursorResult
	getRecordsResult   *GetRecordsResult
	openSessionResult  *OpenSubscriptionSessionResult
	commitOffsetResult *CommitSubscriptionOffsetResult
}

func newShardGroupReaderMockClient() *shardGroupReaderMockClient {
	return &shardGroupReaderMockClient{
		callCount: make(map[string]int),
	}
}

func (m *shardGroupReaderMockClient) incrementCallCount(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount[method]++
}

func (m *shardGroupReaderMockClient) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[method]
}

func (m *shardGroupReaderMockClient) GetCursor(projectName, topicName, shardId string, ctype CursorType, param ...int64) (*GetCursorResult, error) {
	m.incrementCallCount("GetCursor")
	if m.getCursorResult != nil {
		return m.getCursorResult, nil
	}
	return &GetCursorResult{
		Cursor:   "test-cursor",
		Sequence: 0,
	}, nil
}

func (m *shardGroupReaderMockClient) GetTupleRecords(projectName, topicName, shardId, cursor string, limit int, recordSchema *RecordSchema) (*GetRecordsResult, error) {
	m.incrementCallCount("GetTupleRecords")
	if m.getRecordsResult != nil {
		return m.getRecordsResult, nil
	}
	return &GetRecordsResult{
		NextCursor:     "next-cursor",
		RecordCount:    0,
		LatestSequence: 0,
		LatestTime:     time.Now().UnixNano() / 1000000,
		Records:        []IRecord{},
	}, nil
}

func (m *shardGroupReaderMockClient) GetBlobRecords(projectName, topicName, shardId, cursor string, limit int) (*GetRecordsResult, error) {
	m.incrementCallCount("GetBlobRecords")
	if m.getRecordsResult != nil {
		return m.getRecordsResult, nil
	}
	return &GetRecordsResult{
		NextCursor:     "next-cursor",
		RecordCount:    0,
		LatestSequence: 0,
		LatestTime:     time.Now().UnixNano() / 1000000,
		Records:        []IRecord{},
	}, nil
}

func (m *shardGroupReaderMockClient) OpenSubscriptionSession(projectName, topicName, subId string, shardIds []string) (*OpenSubscriptionSessionResult, error) {
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

func (m *shardGroupReaderMockClient) CommitSubscriptionOffset(projectName, topicName, subId string, offsets map[string]SubscriptionOffset) (*CommitSubscriptionOffsetResult, error) {
	m.incrementCallCount("CommitSubscriptionOffset")
	if m.commitOffsetResult != nil {
		return m.commitOffsetResult, nil
	}
	return &CommitSubscriptionOffsetResult{}, nil
}

func TestShardGroupReader(t *testing.T) {
	mockClient := newShardGroupReaderMockClient()
	mockClient.getRecordsResult = &GetRecordsResult{
		NextCursor:     "next-cursor",
		RecordCount:    0,
		LatestSequence: 0,
		LatestTime:     time.Now().UnixNano() / 1000000,
		Records:        []IRecord{},
	}

	mockOffsetManager := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	mockOffsetManager.start()
	defer mockOffsetManager.stop()

	cfg := NewConsumerConfig()
	cfg.FetchNumber = 100
	cfg.BufferNumber = 100
	cfg.Protocol = Batch
	cfg.AutoRecordAck = true

	sgr := newShardGroupReader(
		"test-project", "test-topic", mockClient,
		mockOffsetManager, cfg,
	)

	// Test addShards
	sgr.addShards([]string{"0", "1"}, map[string]SubscriptionOffset{
		"0": {Sequence: -1},
		"1": {Sequence: -1},
	})

	holdShards := sgr.getHoldShards()
	assert.Len(t, holdShards, 2)

	// Test removeShards
	sgr.removeShards([]string{"0"})
	holdShards = sgr.getHoldShards()
	assert.Len(t, holdShards, 1)
	assert.Equal(t, "1", holdShards[0])

	sgr.stop()
}

func TestShardGroupReaderRead(t *testing.T) {
	mockClient := newShardGroupReaderMockClient()
	mockClient.getRecordsResult = &GetRecordsResult{
		NextCursor:     "next-cursor",
		RecordCount:    0,
		LatestSequence: 0,
		LatestTime:     time.Now().UnixNano() / 1000000,
		Records:        []IRecord{},
	}

	mockOffsetManager := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	mockOffsetManager.start()
	defer mockOffsetManager.stop()

	cfg := NewConsumerConfig()
	cfg.FetchNumber = 100
	cfg.BufferNumber = 100
	cfg.Protocol = Batch
	cfg.AutoRecordAck = true

	sgr := newShardGroupReader(
		"test-project", "test-topic", mockClient,
		mockOffsetManager, cfg,
	)
	defer sgr.stop()

	// Test read with no data (should return nil)
	record, err := sgr.read(100 * time.Millisecond)
	assert.NoError(t, err)
	assert.Nil(t, record)

	// Test read with timeout=0 (non-blocking)
	record, err = sgr.read(0)
	assert.NoError(t, err)
	assert.Nil(t, record)
}
