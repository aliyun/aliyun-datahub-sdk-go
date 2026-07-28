package datahub

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockDataHubApi mocks the DataHubApi interface
type MockDataHubApi struct {
	mock.Mock
	DataHub
}

func (m *MockDataHubApi) GetTopic(project, topic string) (*GetTopicResult, error) {
	args := m.Called(project, topic)
	return args.Get(0).(*GetTopicResult), args.Error(1)
}

func (m *MockDataHubApi) ListTopicSchema(project, topic string) (*ListTopicSchemaResult, error) {
	args := m.Called(project, topic)
	return args.Get(0).(*ListTopicSchemaResult), args.Error(1)
}

func TestSchemaClientSingleton(t *testing.T) {
	client1 := schemaClientInstance()
	client2 := schemaClientInstance()
	assert.Equal(t, client1, client2, "schemaClient should be a singleton")
}

func TestGetTopicKey(t *testing.T) {
	key := getTopicKey("project1", "topic1")
	assert.Equal(t, "project1/topic1", key)
}

func TestNewTopicSchemaCache(t *testing.T) {
	client := &MockDataHubApi{}
	cacheItem := NewTopicSchemaCache("test_project", "test_topic", client)

	assert.NotNil(t, cacheItem)
	assert.NotNil(t, cacheItem.cache)
	assert.WithinDuration(t, time.Now(), cacheItem.accessTime.Load().(time.Time), time.Second)
}

func TestTopicSchemaItem_GetSchemaCache(t *testing.T) {
	client := &MockDataHubApi{}
	cacheItem := NewTopicSchemaCache("test_project", "test_topic", client)

	originalAccessTime := cacheItem.accessTime.Load().(time.Time)
	time.Sleep(100 * time.Millisecond) // Ensure time difference

	schemaCache := cacheItem.getSchemaCache()
	assert.NotNil(t, schemaCache)

	newAccessTime := cacheItem.accessTime.Load().(time.Time)
	assert.True(t, newAccessTime.After(originalAccessTime))
}

func TestSchemaClient_AddTopicSchemaCache(t *testing.T) {
	mockClient := &MockDataHubApi{}

	// Mock GetTopic call
	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: &RecordSchema{}, // An empty RecordSchema
	}, nil).Times(1)

	client := schemaClientInstance()
	cache, err := client.addTopicSchemaCache("test_project", "test_topic", mockClient)

	assert.NoError(t, err)
	assert.NotNil(t, cache)
	maxVersionID, err := cache.getMaxSchemaVersionId()
	assert.NoError(t, err)
	assert.Equal(t, 0, maxVersionID)
	mockClient.AssertExpectations(t)
}

func TestSchemaClient_AddTopicSchemaCacheWithEnableSchema(t *testing.T) {
	mockClient := &MockDataHubApi{}

	// Mock GetTopic call
	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: true,
		RecordSchema: &RecordSchema{}, // An empty RecordSchema
	}, nil).Times(1)

	mockClient.On("ListTopicSchema", "test_project", "test_topic").Return(&ListTopicSchemaResult{
		SchemaInfoList: []RecordSchemaInfo{
			{
				VersionId:    0,
				RecordSchema: RecordSchema{},
			},
			{
				VersionId:    1,
				RecordSchema: RecordSchema{},
			},
		},
	}, nil).Times(1)

	client := schemaClientInstance()
	cache, err := client.addTopicSchemaCache("test_project", "test_topic", mockClient)
	assert.NoError(t, err)
	assert.NotNil(t, cache)
	maxVersionID, err := cache.getMaxSchemaVersionId()
	assert.NoError(t, err)
	assert.Equal(t, 1, maxVersionID)
	mockClient.AssertExpectations(t)
}

func TestSchemaClient_AddTopicSchemaCacheWithBlobTopic(t *testing.T) {
	mockClient := &MockDataHubApi{}

	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   BLOB,
		EnableSchema: false,
	}, nil).Times(1)

	client := schemaClientInstance()
	cache, err := client.addTopicSchemaCache("test_project", "test_topic", mockClient)

	assert.NoError(t, err)
	assert.NotNil(t, cache)
	maxVersionID, err := cache.getMaxSchemaVersionId()
	assert.NoError(t, err)
	assert.Equal(t, -1, maxVersionID)
	mockClient.AssertExpectations(t)
}

func TestSchemaClient_FindTopicSchemaCache(t *testing.T) {
	mockClient := &MockDataHubApi{}

	// Mock GetTopic call
	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: &RecordSchema{},
	}, nil).Times(1)

	client := schemaClientInstance()
	client.clean()

	cache := client.findTopicSchemaCache("test_project", "test_topic")
	assert.Nil(t, cache)

	// Add cache first
	_, err := client.addTopicSchemaCache("test_project", "test_topic", mockClient)
	assert.NoError(t, err)

	// Find cache
	foundCache := client.findTopicSchemaCache("test_project", "test_topic")
	assert.NotNil(t, foundCache)

	// Find non-existent cache
	notFoundCache := client.findTopicSchemaCache("nonexistent_project", "nonexistent_topic")
	assert.Nil(t, notFoundCache)
}

func TestSchemaClient_AutoAddTopicSchemaCache(t *testing.T) {
	mockClient := &MockDataHubApi{}
	schema := NewRecordSchema()
	schema.AddField(*NewField("test_field", INTEGER))

	// Mock GetTopic call
	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: schema,
	}, nil).Times(1)

	client := schemaClientInstance()
	client.clean()

	cache1 := client.findTopicSchemaCache("test_project", "test_topic")
	assert.Nil(t, cache1)

	// First retrieval should create cache
	cache2, err := client.getTopicSchemaCache("test_project", "test_topic", mockClient)
	assert.NoError(t, err)
	assert.NotNil(t, cache2)

	cache3 := client.findTopicSchemaCache("test_project", "test_topic")
	assert.Equal(t, cache2, cache3)

	// Second retrieval should return same cache
	cache4, err := client.getTopicSchemaCache("test_project", "test_topic", mockClient)
	assert.NoError(t, err)
	assert.Equal(t, cache2, cache4)

	mockClient.AssertExpectations(t)
}

func TestTopicSchemaCacheImpl_Init(t *testing.T) {
	mockClient := &MockDataHubApi{}

	schema := NewRecordSchema()
	schema.AddField(*NewField("test_field", INTEGER))

	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: schema,
	}, nil).Times(1)

	var freshTime atomic.Value
	freshTime.Store(time.Now())
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: freshTime,
	}

	assert.NoError(t, cacheImpl.init())

	assert.Equal(t, 0, cacheImpl.maxSchemaVersionId)
	assert.Equal(t, 1, len(cacheImpl.schemaMap))
	assert.Equal(t, 1, len(cacheImpl.versionMap))
	mockClient.AssertExpectations(t)
}

func TestTopicSchemaCacheImpl_InitReturnsRefreshError(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now())
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.init()

	assert.ErrorIs(t, err, expectedErr)
	mockClient.AssertExpectations(t)
}

func TestSchemaClient_DoesNotCacheFailedInitialization(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "failed_project", "failed_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()

	client := schemaClientInstance()
	client.clean()

	cache, err := client.getTopicSchemaCache("failed_project", "failed_topic", mockClient)

	assert.Nil(t, cache)
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, client.findTopicSchemaCache("failed_project", "failed_topic"))
	mockClient.AssertExpectations(t)
}

func TestSchemaClient_RetriesInitializationAfterFailure(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "retry_project", "retry_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()
	mockClient.On("GetTopic", "retry_project", "retry_topic").
		Return(&GetTopicResult{RecordType: BLOB}, nil).
		Once()

	client := schemaClientInstance()
	client.clean()

	firstCache, err := client.getTopicSchemaCache("retry_project", "retry_topic", mockClient)
	assert.Nil(t, firstCache)
	assert.ErrorIs(t, err, expectedErr)

	secondCache, err := client.getTopicSchemaCache("retry_project", "retry_topic", mockClient)
	assert.NoError(t, err)
	assert.NotNil(t, secondCache)
	assert.Equal(t, secondCache, client.findTopicSchemaCache("retry_project", "retry_topic"))
	mockClient.AssertExpectations(t)
}

func TestTopicSchemaCacheImpl_FreshNormalSchema(t *testing.T) {
	mockClient := &MockDataHubApi{}

	// Create a simple RecordSchema for testing
	testSchema := &RecordSchema{}

	result := &GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: testSchema,
	}

	cacheImpl := &topicSchemaCacheImpl{
		client:     mockClient,
		project:    "test_project",
		topic:      "test_topic",
		schemaMap:  make(map[uint32]*SchemaItem),
		versionMap: make(map[int]*SchemaItem),
	}

	err := cacheImpl.freshNomalSchema(result)
	assert.NoError(t, err)
	assert.Equal(t, 0, cacheImpl.maxSchemaVersionId)

	// Verify schemaMap and versionMap are updated correctly
	assert.Len(t, cacheImpl.schemaMap, 1)
	assert.Len(t, cacheImpl.versionMap, 1)

	var schemaHash uint32
	for hash := range cacheImpl.schemaMap {
		schemaHash = hash
		break
	}

	assert.Contains(t, cacheImpl.versionMap, 0)
	assert.Contains(t, cacheImpl.schemaMap, schemaHash)
	assert.Equal(t, testSchema, cacheImpl.versionMap[0].dhSchema)
}

func TestTopicSchemaCacheImpl_FreshMultiSchema(t *testing.T) {
	mockClient := &MockDataHubApi{}

	// Mock ListTopicSchema result
	listResult := &ListTopicSchemaResult{
		SchemaInfoList: []RecordSchemaInfo{
			{
				VersionId:    1,
				RecordSchema: RecordSchema{}, // Simple schema
			},
			{
				VersionId:    2,
				RecordSchema: RecordSchema{}, // Simple schema
			},
		},
	}

	mockClient.On("ListTopicSchema", "test_project", "test_topic").Return(listResult, nil)

	cacheImpl := &topicSchemaCacheImpl{
		client:     mockClient,
		project:    "test_project",
		topic:      "test_topic",
		schemaMap:  make(map[uint32]*SchemaItem),
		versionMap: make(map[int]*SchemaItem),
	}

	err := cacheImpl.freshMultiSchema()
	assert.NoError(t, err)
	assert.Equal(t, 2, cacheImpl.maxSchemaVersionId)

	// Verify maps are populated correctly
	assert.Len(t, cacheImpl.versionMap, 2)
	assert.Contains(t, cacheImpl.versionMap, 1)
	assert.Contains(t, cacheImpl.versionMap, 2)

	mockClient.AssertExpectations(t)
}

func TestTopicSchemaCacheImpl_GetMaxSchemaVersionId(t *testing.T) {
	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))
	cacheImpl := &topicSchemaCacheImpl{
		project:            "test_project",
		topic:              "test_topic",
		schemaMap:          make(map[uint32]*SchemaItem),
		versionMap:         make(map[int]*SchemaItem),
		nextFreshTime:      freshTime,
		maxSchemaVersionId: 5,
		topicResult:        &GetTopicResult{RecordType: TUPLE},
	}

	versionId, err := cacheImpl.getMaxSchemaVersionId()
	assert.NoError(t, err)
	assert.Equal(t, 5, versionId)
}

func TestTopicSchemaCacheImpl_GetSchemaByVersionId(t *testing.T) {
	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))
	cacheImpl := &topicSchemaCacheImpl{
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		nextFreshTime: freshTime,
		topicResult:   &GetTopicResult{RecordType: TUPLE},
		versionMap: map[int]*SchemaItem{
			1: {versionId: 1, dhSchema: testSchema},
		},
	}

	// Test finding existing version
	schema, err := cacheImpl.getSchemaByVersionId(1)
	assert.NoError(t, err)
	assert.Equal(t, testSchema, schema)

	// Test not finding version
	schema, err = cacheImpl.getSchemaByVersionId(999)
	assert.NoError(t, err)
	assert.Nil(t, schema)

	// Test negative version number
	schema, err = cacheImpl.getSchemaByVersionId(-1)
	assert.NoError(t, err)
	assert.Nil(t, schema)
}

func TestTopicSchemaCacheImpl_GetVersionIdBySchema(t *testing.T) {
	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	hash := testSchema.hashCode()

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))

	cacheImpl := &topicSchemaCacheImpl{
		project: "test_project",
		topic:   "test_topic",
		schemaMap: map[uint32]*SchemaItem{
			hash: {versionId: 1, dhSchema: testSchema},
		},
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: freshTime,
		topicResult:   &GetTopicResult{EnableSchema: true},
	}

	// Test finding version for schema
	versionId, err := cacheImpl.getVersionIdBySchema(testSchema)
	assert.NoError(t, err)
	assert.Equal(t, 1, versionId)

	// Test not finding schema
	versionId, err = cacheImpl.getVersionIdBySchema(&RecordSchema{})
	assert.NoError(t, err)
	assert.Equal(t, invalidSchemaVersionId, versionId)

	// Test passing nil schema
	versionId, err = cacheImpl.getVersionIdBySchema(nil)
	assert.NoError(t, err)
	assert.Equal(t, blobSchemaVersionId, versionId)
}

func TestGetVersionIdBySchemaUsesStaleEntryAfterRefreshError(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()

	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now().Add(-time.Minute))
	cacheImpl := &topicSchemaCacheImpl{
		client:      mockClient,
		project:     "test_project",
		topic:       "test_topic",
		topicResult: &GetTopicResult{RecordType: TUPLE, EnableSchema: true},
		schemaMap: map[uint32]*SchemaItem{
			testSchema.hashCode(): {
				versionId: 7,
				dhSchema:  testSchema,
			},
		},
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	versionID, err := cacheImpl.getVersionIdBySchema(testSchema)

	assert.NoError(t, err)
	assert.Equal(t, 7, versionID)
	mockClient.AssertExpectations(t)
}

func TestGetVersionIdBySchemaReturnsRefreshErrorWhenStaleEntryMissing(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()

	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now().Add(-time.Minute))
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		topicResult:   &GetTopicResult{RecordType: TUPLE, EnableSchema: true},
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	versionID, err := cacheImpl.getVersionIdBySchema(testSchema)

	assert.Equal(t, invalidSchemaVersionId, versionID)
	assert.ErrorIs(t, err, expectedErr)
	mockClient.AssertExpectations(t)
}

func TestGetVersionIdBySchemaReturnsUninitializedErrorInsteadOfPanicking(t *testing.T) {
	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now().Add(time.Minute))
	cacheImpl := &topicSchemaCacheImpl{
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	versionID, err := cacheImpl.getVersionIdBySchema(testSchema)

	assert.Equal(t, invalidSchemaVersionId, versionID)
	assert.ErrorContains(t, err, "not initialized")
}

func TestTopicSchemaCacheImpl_GetAvroSchema(t *testing.T) {
	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	hash := testSchema.hashCode()
	testAvroSchema, _ := getAvroSchema(testSchema)

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))

	cacheImpl := &topicSchemaCacheImpl{
		project: "test_project",
		topic:   "test_topic",
		schemaMap: map[uint32]*SchemaItem{
			hash: {versionId: 1, dhSchema: testSchema, avroSchema: testAvroSchema},
		},
		nextFreshTime: freshTime,
		versionMap:    make(map[int]*SchemaItem),
		topicResult:   &GetTopicResult{RecordType: TUPLE},
	}

	// Test finding avro schema for given schema
	avroSchema, err := cacheImpl.getAvroSchema(testSchema)
	assert.NoError(t, err)
	assert.Equal(t, testAvroSchema, avroSchema)

	// Test passing nil schema
	avroSchema, err = cacheImpl.getAvroSchema(nil)
	assert.NoError(t, err)
	assert.Equal(t, getAvroBlobSchema(), avroSchema)
}

func TestTopicSchemaCacheImpl_GetAvroSchemaByVersionId(t *testing.T) {
	testSchema := NewRecordSchema()
	testSchema.AddField(*NewField("test_field", INTEGER))
	testAvroSchema, _ := getAvroSchema(testSchema)

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))

	cacheImpl := &topicSchemaCacheImpl{
		project:       "test_project",
		topic:         "test_topic",
		nextFreshTime: freshTime,
		schemaMap:     make(map[uint32]*SchemaItem),
		topicResult:   &GetTopicResult{RecordType: TUPLE},
		versionMap: map[int]*SchemaItem{
			1: {versionId: 1, avroSchema: testAvroSchema},
		},
	}

	// Test finding avro schema for given version
	avroSchema, err := cacheImpl.getAvroSchemaByVersionId(1)
	assert.NoError(t, err)
	assert.Equal(t, testAvroSchema, avroSchema)

	// Test not finding version
	avroSchema, err = cacheImpl.getAvroSchemaByVersionId(999)
	assert.NoError(t, err)
	assert.Nil(t, avroSchema)

	// Test negative version number
	avroSchema, err = cacheImpl.getAvroSchemaByVersionId(-1)
	assert.NoError(t, err)
	assert.Equal(t, getAvroBlobSchema(), avroSchema)
}

func TestFreshSchemaWithForce(t *testing.T) {
	mockClient := &MockDataHubApi{}

	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: &RecordSchema{},
	}, nil).Times(1)

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))

	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: freshTime,
	}

	err := cacheImpl.freshSchema(true) // Force refresh
	assert.NoError(t, err)

	mockClient.AssertExpectations(t)
}

func TestFreshSchemaWithoutForce(t *testing.T) {
	mockClient := &MockDataHubApi{}

	mockClient.On("GetTopic", "test_project", "test_topic").Return(&GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: false,
		RecordSchema: &RecordSchema{},
	}, nil)

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now().Add(time.Minute)) // Set to future time

	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.freshSchema(false) // No force refresh
	assert.NoError(t, err)

	mockClient.AssertNotCalled(t, "GetTopic", "test_project", "test_topic")
}

func TestFreshSchemaWithError(t *testing.T) {
	mockClient := &MockDataHubApi{}

	expectedErr := errors.New("network error")
	mockClient.On("GetTopic", "test_project", "test_topic").Return((*GetTopicResult)(nil), expectedErr)

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now().Add(-10 * time.Minute)) // Set to past time

	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.freshSchema(true)
	assert.EqualError(t, err, expectedErr.Error())

	mockClient.AssertExpectations(t)
}

func TestFreshSchemaErrorPreservesExistingTopicResult(t *testing.T) {
	expectedErr := errors.New("network error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return((*GetTopicResult)(nil), expectedErr).
		Once()

	oldResult := &GetTopicResult{
		RecordType:   TUPLE,
		EnableSchema: true,
	}
	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now())
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		topicResult:   oldResult,
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.freshSchema(true)

	assert.ErrorIs(t, err, expectedErr)
	assert.Same(t, oldResult, cacheImpl.topicResult)
	assert.Less(t, time.Until(cacheImpl.nextFreshTime.Load().(time.Time)), 5*time.Second)
	mockClient.AssertExpectations(t)
}

func TestFreshSchemaListErrorPreservesExistingSnapshot(t *testing.T) {
	expectedErr := errors.New("list schema error")
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return(&GetTopicResult{RecordType: TUPLE, EnableSchema: true}, nil).
		Once()
	mockClient.On("ListTopicSchema", "test_project", "test_topic").
		Return((*ListTopicSchemaResult)(nil), expectedErr).
		Once()

	oldResult := &GetTopicResult{RecordType: TUPLE, EnableSchema: true}
	oldSchemaMap := make(map[uint32]*SchemaItem)
	oldVersionMap := make(map[int]*SchemaItem)
	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now())
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		topicResult:   oldResult,
		schemaMap:     oldSchemaMap,
		versionMap:    oldVersionMap,
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.freshSchema(true)

	assert.ErrorIs(t, err, expectedErr)
	assert.Same(t, oldResult, cacheImpl.topicResult)
	assert.Equal(t, oldSchemaMap, cacheImpl.schemaMap)
	assert.Equal(t, oldVersionMap, cacheImpl.versionMap)
	mockClient.AssertExpectations(t)
}

func TestFreshSchemaRejectsNilTopicResult(t *testing.T) {
	mockClient := &MockDataHubApi{}
	mockClient.On("GetTopic", "test_project", "test_topic").
		Return((*GetTopicResult)(nil), nil).
		Once()

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now())
	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	err := cacheImpl.freshSchema(true)

	assert.ErrorContains(t, err, "nil topic result")
	mockClient.AssertExpectations(t)
}

func TestFreshSchemaWithAppendField(t *testing.T) {
	oldSchema := NewRecordSchema()
	oldSchema.AddField(*NewField("f1", INTEGER))
	oldSchema.AddField(*NewField("f2", DOUBLE))
	odlAvroSchema, _ := getAvroSchema(oldSchema)

	newSchema := NewRecordSchema()
	newSchema.AddField(*NewField("f1", INTEGER))
	newSchema.AddField(*NewField("f2", DOUBLE))
	newSchema.AddField(*NewField("f3", STRING))
	newAvroSchema, _ := getAvroSchema(newSchema)

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))
	mockClient := &MockDataHubApi{}

	mockClient.
		On("GetTopic", "test_project", "test_topic").
		Return(&GetTopicResult{
			RecordType:   TUPLE,
			EnableSchema: false,
			RecordSchema: oldSchema,
		}, nil).
		Once().
		On("GetTopic", "test_project", "test_topic").
		Return(&GetTopicResult{
			RecordType:   TUPLE,
			EnableSchema: false,
			RecordSchema: newSchema,
		}, nil).
		Once()

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now()) // Set to past time

	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	assert.NoError(t, cacheImpl.init())
	assert.Equal(t, len(cacheImpl.versionMap), 1)
	assert.Equal(t, len(cacheImpl.schemaMap), 1)
	assert.Equal(t, cacheImpl.maxSchemaVersionId, 0)
	versionID, err := cacheImpl.getVersionIdBySchema(oldSchema)
	assert.NoError(t, err)
	assert.Equal(t, versionID, 0)
	avroSchema1, err := cacheImpl.getAvroSchema(oldSchema)
	assert.NoError(t, err)
	assert.Equal(t, avroSchema1, odlAvroSchema)
	avroSchema2, err := cacheImpl.getAvroSchema(newSchema)
	assert.NoError(t, err)
	assert.Nil(t, avroSchema2)
	avroSchema3, err := cacheImpl.getAvroSchemaByVersionId(0)
	assert.NoError(t, err)
	assert.Equal(t, avroSchema3, odlAvroSchema)

	assert.NoError(t, cacheImpl.freshSchema(true))
	assert.Equal(t, len(cacheImpl.versionMap), 1)
	assert.Equal(t, len(cacheImpl.schemaMap), 2)
	assert.Equal(t, cacheImpl.maxSchemaVersionId, 0)
	versionID, err = cacheImpl.getVersionIdBySchema(oldSchema)
	assert.NoError(t, err)
	assert.Equal(t, versionID, 0)
	versionID, err = cacheImpl.getVersionIdBySchema(newSchema)
	assert.NoError(t, err)
	assert.Equal(t, versionID, 0)
	avroSchema11, err := cacheImpl.getAvroSchema(oldSchema)
	assert.NoError(t, err)
	assert.Equal(t, avroSchema11, odlAvroSchema)
	avroSchema12, err := cacheImpl.getAvroSchema(newSchema)
	assert.NoError(t, err)
	assert.Equal(t, avroSchema12, newAvroSchema)
	avroSchema13, err := cacheImpl.getAvroSchemaByVersionId(0)
	assert.NoError(t, err)
	assert.Equal(t, avroSchema13, newAvroSchema)

	mockClient.AssertExpectations(t)
}

func TestFreshSchemaWithAddNewSchema(t *testing.T) {
	schema1 := NewRecordSchema()
	schema1.AddField(*NewField("f1", INTEGER))
	schema1.AddField(*NewField("f2", DOUBLE))
	avroSchema1, _ := getAvroSchema(schema1)

	schema2 := NewRecordSchema()
	schema2.AddField(*NewField("f1", INTEGER))
	schema2.AddField(*NewField("f2", DOUBLE))
	schema2.AddField(*NewField("f3", STRING))
	avroSchema2, _ := getAvroSchema(schema2)

	schema3 := NewRecordSchema()
	schema3.AddField(*NewField("f1", BIGINT))
	schema3.AddField(*NewField("f2", STRING))
	avroSchema3, _ := getAvroSchema(schema3)

	var freshTime atomic.Value
	freshTime.Store(time.Now().Add(time.Minute))
	mockClient := &MockDataHubApi{}

	mockClient.
		On("GetTopic", "test_project", "test_topic").
		Return(&GetTopicResult{
			RecordType:   TUPLE,
			EnableSchema: true,
			RecordSchema: schema1,
		}, nil).
		Once().
		On("GetTopic", "test_project", "test_topic").
		Return(&GetTopicResult{
			RecordType:   TUPLE,
			EnableSchema: true,
			RecordSchema: schema1,
		}, nil).
		Once()

	mockClient.
		On("ListTopicSchema", "test_project", "test_topic").
		Return(&ListTopicSchemaResult{
			SchemaInfoList: []RecordSchemaInfo{
				{
					RecordSchema: *schema1,
					VersionId:    0,
				},
				{
					RecordSchema: *schema2,
					VersionId:    1,
				},
			},
		}, nil).
		Once().
		On("ListTopicSchema", "test_project", "test_topic").
		Return(&ListTopicSchemaResult{
			SchemaInfoList: []RecordSchemaInfo{
				{
					RecordSchema: *schema1,
					VersionId:    0,
				},
				{
					RecordSchema: *schema2,
					VersionId:    1,
				},
				{
					RecordSchema: *schema3,
					VersionId:    2,
				},
			},
		}, nil).
		Once()

	var nextFreshTime atomic.Value
	nextFreshTime.Store(time.Now()) // Set to past time

	cacheImpl := &topicSchemaCacheImpl{
		client:        mockClient,
		project:       "test_project",
		topic:         "test_topic",
		schemaMap:     make(map[uint32]*SchemaItem),
		versionMap:    make(map[int]*SchemaItem),
		nextFreshTime: nextFreshTime,
	}

	assert.NoError(t, cacheImpl.init())
	assert.Equal(t, len(cacheImpl.versionMap), 2)
	assert.Equal(t, len(cacheImpl.schemaMap), 2)
	assert.Equal(t, cacheImpl.maxSchemaVersionId, 1)
	avro1, err := cacheImpl.getAvroSchema(schema1)
	assert.NoError(t, err)
	assert.Equal(t, avro1, avroSchema1)
	avro1, err = cacheImpl.getAvroSchemaByVersionId(0)
	assert.NoError(t, err)
	assert.Equal(t, avro1, avroSchema1)
	avro2, err := cacheImpl.getAvroSchema(schema2)
	assert.NoError(t, err)
	assert.Equal(t, avro2, avroSchema2)
	avro2, err = cacheImpl.getAvroSchemaByVersionId(1)
	assert.NoError(t, err)
	assert.Equal(t, avro2, avroSchema2)
	avro3, err := cacheImpl.getAvroSchema(schema3)
	assert.NoError(t, err)
	assert.Equal(t, avro3, nil)
	avro3, err = cacheImpl.getAvroSchemaByVersionId(2)
	assert.NoError(t, err)
	assert.Equal(t, avro3, nil)

	assert.NoError(t, cacheImpl.freshSchema(true))
	assert.Equal(t, len(cacheImpl.versionMap), 3)
	assert.Equal(t, len(cacheImpl.schemaMap), 3)
	assert.Equal(t, cacheImpl.maxSchemaVersionId, 2)

	avro13, err := cacheImpl.getAvroSchema(schema3)
	assert.NoError(t, err)
	assert.Equal(t, avro13, avroSchema3)
	avro13, err = cacheImpl.getAvroSchemaByVersionId(2)
	assert.NoError(t, err)
	assert.Equal(t, avro13, avroSchema3)

	mockClient.AssertExpectations(t)
}
