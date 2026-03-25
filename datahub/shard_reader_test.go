package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type shardReaderMockClient struct {
	DataHubApi
	mu               sync.Mutex
	callCount        map[string]int
	getCursorResult  *GetCursorResult
	getRecordsResult *GetRecordsResult
}

func newShardReaderMockClient() *shardReaderMockClient {
	return &shardReaderMockClient{
		callCount: make(map[string]int),
	}
}

func (m *shardReaderMockClient) incrementCallCount(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount[method]++
}

func (m *shardReaderMockClient) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[method]
}

func (m *shardReaderMockClient) GetCursor(projectName, topicName, shardId string, ctype CursorType, param ...int64) (*GetCursorResult, error) {
	m.incrementCallCount("GetCursor")
	if m.getCursorResult != nil {
		return m.getCursorResult, nil
	}
	return &GetCursorResult{
		Cursor:   "test-cursor",
		Sequence: 0,
	}, nil
}

func (m *shardReaderMockClient) GetTupleRecords(projectName, topicName, shardId, cursor string, limit int, recordSchema *RecordSchema) (*GetRecordsResult, error) {
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

func (m *shardReaderMockClient) GetBlobRecords(projectName, topicName, shardId, cursor string, limit int) (*GetRecordsResult, error) {
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

func (m *shardReaderMockClient) OpenSubscriptionSession(projectName, topicName, subId string, shardIds []string) (*OpenSubscriptionSessionResult, error) {
	m.incrementCallCount("OpenSubscriptionSession")
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

func (m *shardReaderMockClient) CommitSubscriptionOffset(projectName, topicName, subId string, offsets map[string]SubscriptionOffset) (*CommitSubscriptionOffsetResult, error) {
	m.incrementCallCount("CommitSubscriptionOffset")
	return &CommitSubscriptionOffsetResult{}, nil
}

func TestShardReader(t *testing.T) {
	mockClient := newShardReaderMockClient()
	mockClient.getRecordsResult = &GetRecordsResult{
		NextCursor:     "next-cursor",
		RecordCount:    2,
		LatestSequence: 1,
		LatestTime:     time.Now().UnixNano() / 1000000,
		Records: []IRecord{
			NewTupleRecord(nil),
			NewTupleRecord(nil),
		},
	}

	mockOffsetManager := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	mockOffsetManager.start()
	defer mockOffsetManager.stop()

	cfg := NewConsumerConfig()
	cfg.FetchNumber = 100

	sr := newShardReader(
		"test-project", "test-topic", "0",
		mockClient, mockOffsetManager, cfg,
	)

	// Add shard to offset manager first
	mockOffsetManager.addShards([]string{"0"})

	err := sr.start(SubscriptionOffset{Sequence: -1})
	assert.NoError(t, err)
	defer sr.stop()

	assert.Equal(t, 1, mockClient.GetCallCount("GetCursor"))
}

func TestShardReaderSealed(t *testing.T) {
	mockClient := newShardReaderMockClient()
	// Return a shard sealed error
	mockClient.getRecordsResult = nil

	mockOffsetManager := newOffsetManager("test-project", "test-topic", "test-sub", mockClient, 10*time.Second)
	mockOffsetManager.start()
	defer mockOffsetManager.stop()

	cfg := NewConsumerConfig()
	cfg.FetchNumber = 100

	sr := newShardReader(
		"test-project", "test-topic", "0",
		mockClient, mockOffsetManager, cfg,
	)

	mockOffsetManager.addShards([]string{"0"})

	err := sr.start(SubscriptionOffset{Sequence: -1})
	assert.NoError(t, err)
	defer sr.stop()

	// Initially not sealed
	assert.False(t, sr.isSealed())
}
