package datahub

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/stretchr/testify/assert"
)

func TestPutBlobRecordsPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "pub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody, err := hex.DecodeString("444855426c75b46a000000020800")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	records := make([]IRecord, 0)
	record1 := NewBlobRecord([]byte("AAAA"))
	record1.ShardId = "0"
	record1.SetAttribute("key1", "value1")
	records = append(records, record1)

	record2 := NewBlobRecord([]byte("BBBB"))
	record2.ShardId = "1"
	record2.SetAttribute("key2", "value2")
	records = append(records, record2)

	ret, err := dh.PutRecords("test_project", "test_topic", records)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 0, ret.FailedRecordCount)
	assert.Equal(t, []FailedRecord(nil), ret.FailedRecords)
}

func TestPutBlobRecordsByShardPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "pub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody, err := hex.DecodeString("444855426c75b46a000000020800")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	records := make([]IRecord, 0)
	record1 := NewBlobRecord([]byte("AAAA"))
	record1.SetAttribute("key1", "value1")
	records = append(records, record1)

	record2 := NewBlobRecord([]byte("BBBB"))
	record2.SetAttribute("key2", "value2")
	records = append(records, record2)

	ret, err := dh.PutRecordsByShard("test_project", "test_topic", "0", records)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestPutTupleRecordsPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "pub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody, err := hex.DecodeString("444855426c75b46a000000020800")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	recordSchema := NewRecordSchema()
	recordSchema.AddField(Field{Name: "f1", Type: BIGINT, AllowNull: true})
	recordSchema.AddField(Field{Name: "f2", Type: STRING, AllowNull: true})

	records := make([]IRecord, 0)
	record1 := NewTupleRecord(recordSchema)
	record1.ShardId = "0"
	record1.SetValueByName("f1", 1)
	record1.SetValueByName("f2", "test")
	record1.SetAttribute("key1", "value1")
	records = append(records, record1)

	record2 := NewTupleRecord(recordSchema)
	record2.ShardId = "1"
	record2.SetValueByName("f1", 1)
	records = append(records, record2)

	ret, err := dh.PutRecords("test_project", "test_topic", records)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 0, ret.FailedRecordCount)
	assert.Equal(t, []FailedRecord(nil), ret.FailedRecords)
}

func TestPutTupleRecordsByShardPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "pub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody, err := hex.DecodeString("444855426c75b46a000000020800")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	recordSchema := NewRecordSchema()
	recordSchema.AddField(Field{Name: "f1", Type: BIGINT, AllowNull: true})
	recordSchema.AddField(Field{Name: "f2", Type: STRING, AllowNull: true})

	records := make([]IRecord, 0)
	record1 := NewTupleRecord(recordSchema)
	record1.SetValueByName("f1", 1)
	record1.SetValueByName("f2", "test")
	record1.SetAttribute("key1", "value1")
	records = append(records, record1)

	record2 := NewTupleRecord(recordSchema)
	record2.SetValueByName("f1", 1)
	records = append(records, record2)

	ret, err := dh.PutRecordsByShard("test_project", "test_topic", "0", records)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestGetBlobRecordsPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "sub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)

		respBody, err := hex.DecodeString("44485542ec953a350000009c0a20333030303630343232306164303030303030303030303030303030313030303010011800226b222033303030363034323230616430303030303030303030303030303030303030302a20333030303630343232306164303030303030303030303030303030313030303030003890cdbe92802f42100a0e0a046b657931120676616c7565314a080a060a04414141415000280330a1cdbe92802f")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	ret, err := dh.GetBlobRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "3000604220ad00000000000000010000", ret.NextCursor)
	assert.Equal(t, 1, ret.RecordCount)
	assert.Equal(t, int64(0), ret.StartSequence)
	assert.Nil(t, ret.RecordSchema)
	assert.Equal(t, 1, len(ret.Records))
	data, ok := ret.Records[0].(*BlobRecord)
	assert.True(t, ok)
	assert.Equal(t, "AAAA", string(data.RawData))
	assert.Equal(t, map[string]string(map[string]string{"key1": "value1"}), data.Attributes)
}

func TestGetTupleRecordsPB(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/x-protobuf", request.Header.Get("Content-Type"))
		assert.Equal(t, "sub", request.Header.Get("x-datahub-request-action"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)

		respBody, err := hex.DecodeString("44485542c0c58c45000000a10a203330303036303432316437313030303030303030303030303030303130303030100118002270222033303030363034323164373130303030303030303030303030303030303030302a203330303036303432316437313030303030303030303030303030303130303030300038f8858c92802f42100a0e0a046b657931120676616c7565314a0d0a060a04746573740a030a01315000280530edb28f92802f")
		assert.Nil(t, err)
		_, _ = writer.Write(respBody)
	}))

	defer ts.Close()

	cfg := NewDefaultConfig()
	cfg.Protocol = Protobuf
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	recordSchema := NewRecordSchema()
	recordSchema.AddField(Field{Name: "field1", Type: STRING, AllowNull: true})
	recordSchema.AddField(Field{Name: "field2", Type: BIGINT, AllowNull: false})

	ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, recordSchema)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "300060421d7100000000000000010000", ret.NextCursor)
	assert.Equal(t, 1, ret.RecordCount)
	assert.Equal(t, int64(0), ret.StartSequence)
	assert.NotNil(t, ret.RecordSchema)
	assert.Equal(t, 1, len(ret.Records))
	data, ok := ret.Records[0].(*TupleRecord)
	assert.True(t, ok)
	assert.EqualValues(t, "test", data.Values[0])
	assert.EqualValues(t, 1, data.Values[1])
}
