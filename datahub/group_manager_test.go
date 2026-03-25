package datahub

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type groupManagerMockClient struct {
	DataHubApi
	mu              sync.Mutex
	callCount       map[string]int
	joinGroupResult *JoinGroupResult
	heartbeatResult *HeartbeatResult
	syncGroupResult *SyncGroupResult
	leaveGroupResult *LeaveGroupResult
}

func newGroupManagerMockClient() *groupManagerMockClient {
	return &groupManagerMockClient{
		callCount: make(map[string]int),
	}
}

func (m *groupManagerMockClient) incrementCallCount(method string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.callCount[method]++
}

func (m *groupManagerMockClient) GetCallCount(method string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.callCount[method]
}

func (m *groupManagerMockClient) JoinGroup(projectName, topicName, consumerGroup string, sessionTimeout int64) (*JoinGroupResult, error) {
	m.incrementCallCount("JoinGroup")
	if m.joinGroupResult != nil {
		return m.joinGroupResult, nil
	}
	return &JoinGroupResult{
		ConsumerId:     "test-consumer-id",
		VersionId:      1,
		SessionTimeout: sessionTimeout,
	}, nil
}

func (m *groupManagerMockClient) Heartbeat(projectName, topicName, consumerGroup, consumerId string, versionId int64, holdShardList, readEndShardList []string) (*HeartbeatResult, error) {
	m.incrementCallCount("Heartbeat")
	if m.heartbeatResult != nil {
		return m.heartbeatResult, nil
	}
	return &HeartbeatResult{
		PlanVersion: 1,
		ShardList:   holdShardList,
	}, nil
}

func (m *groupManagerMockClient) SyncGroup(projectName, topicName, consumerGroup, consumerId string, versionId int64, releaseShardList, readEndShardList []string) (*SyncGroupResult, error) {
	m.incrementCallCount("SyncGroup")
	if m.syncGroupResult != nil {
		return m.syncGroupResult, nil
	}
	return &SyncGroupResult{}, nil
}

func (m *groupManagerMockClient) LeaveGroup(projectName, topicName, consumerGroup, consumerId string, versionId int64) (*LeaveGroupResult, error) {
	m.incrementCallCount("LeaveGroup")
	if m.leaveGroupResult != nil {
		return m.leaveGroupResult, nil
	}
	return &LeaveGroupResult{}, nil
}

func TestGroupManager(t *testing.T) {
	mockClient := newGroupManagerMockClient()

	gm := newGroupManager("test-project", "test-topic", "test-sub", mockClient, 60*time.Second)
	err := gm.start()
	assert.NoError(t, err)
	defer gm.stop()

	assert.Equal(t, 1, mockClient.GetCallCount("JoinGroup"))
	assert.True(t, gm.joined)
}

func TestGroupManagerHeartbeat(t *testing.T) {
	mockClient := newGroupManagerMockClient()

	gm := newGroupManager("test-project", "test-topic", "test-sub", mockClient, 60*time.Second)
	err := gm.start()
	assert.NoError(t, err)
	defer gm.stop()

	result, err := gm.heartbeat([]string{"0", "1"}, nil)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, mockClient.GetCallCount("Heartbeat"))
}

func TestGroupManagerSyncGroup(t *testing.T) {
	mockClient := newGroupManagerMockClient()

	gm := newGroupManager("test-project", "test-topic", "test-sub", mockClient, 60*time.Second)
	err := gm.start()
	assert.NoError(t, err)
	defer gm.stop()

	result, err := gm.syncGroup([]string{"0"}, []string{"1"})
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, mockClient.GetCallCount("SyncGroup"))
}

func TestGroupManagerLeaveGroup(t *testing.T) {
	mockClient := newGroupManagerMockClient()

	gm := newGroupManager("test-project", "test-topic", "test-sub", mockClient, 60*time.Second)
	err := gm.start()
	assert.NoError(t, err)

	err = gm.leaveGroup()
	assert.NoError(t, err)
	assert.Equal(t, 1, mockClient.GetCallCount("LeaveGroup"))
	assert.False(t, gm.joined)
}

func TestGroupManagerShardChanged(t *testing.T) {
	gm := &groupManager{}

	tests := []struct {
		name           string
		newShards      []string
		holdShards     []string
		expectChanged  bool
		expectToAdd    []string
		expectToRemove []string
	}{
		{
			name:           "no change - same shards",
			newShards:      []string{"0", "1", "2"},
			holdShards:     []string{"0", "1", "2"},
			expectChanged:  false,
			expectToAdd:    nil,
			expectToRemove: nil,
		},
		{
			name:           "no change - both empty",
			newShards:      []string{},
			holdShards:     []string{},
			expectChanged:  false,
			expectToAdd:    nil,
			expectToRemove: nil,
		},
		{
			name:           "add shards",
			newShards:      []string{"0", "1", "2"},
			holdShards:     []string{"0"},
			expectChanged:  true,
			expectToAdd:    []string{"1", "2"},
			expectToRemove: nil,
		},
		{
			name:           "remove shards",
			newShards:      []string{"0"},
			holdShards:     []string{"0", "1", "2"},
			expectChanged:  true,
			expectToAdd:    nil,
			expectToRemove: []string{"1", "2"},
		},
		{
			name:           "add and remove shards",
			newShards:      []string{"0", "3", "4"},
			holdShards:     []string{"0", "1", "2"},
			expectChanged:  true,
			expectToAdd:    []string{"3", "4"},
			expectToRemove: []string{"1", "2"},
		},
		{
			name:           "new shards empty",
			newShards:      []string{},
			holdShards:     []string{"0", "1"},
			expectChanged:  true,
			expectToAdd:    nil,
			expectToRemove: []string{"0", "1"},
		},
		{
			name:           "hold shards empty",
			newShards:      []string{"0", "1"},
			holdShards:     []string{},
			expectChanged:  true,
			expectToAdd:    []string{"0", "1"},
			expectToRemove: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			changed, toAdd, toRemove := gm.shardChanged(tt.newShards, tt.holdShards)
			assert.Equal(t, tt.expectChanged, changed)
			assert.ElementsMatch(t, tt.expectToAdd, toAdd)
			assert.ElementsMatch(t, tt.expectToRemove, toRemove)
		})
	}
}
