package ut

import (
    "../../datahub"
    "encoding/hex"
    "fmt"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/shopspring/decimal"
    "github.com/stretchr/Testify/assert"
    "io/ioutil"
    "math"
    "net/http"
    "net/http/httptest"
    "testing"
)

/**********  json client  ****************/

func TestPutBlobRecords(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"pub\",\"Records\":[{\"Data\":\"QUFBQQ==\",\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"}},{\"Data\":\"QkJCQg==\",\"ShardId\":\"1\",\"Attributes\":{\"key2\":\"value2\"}}]}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"FailedRecordCount\": 0,\"FailedRecords\": []}"))
    }))

    defer ts.Close()

    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig(ts.URL, config, account)

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewBlobRecord([]byte("AAAA"), 0)
    record1.ShardId = "0"
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewBlobRecord([]byte("BBBB"), 0)
    record2.ShardId = "1"
    record2.SetAttribute("key2", "value2")
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.FailedRecordCount)
    assert.Equal(t, []datahub.FailedRecord([]datahub.FailedRecord{}), ret.FailedRecords)
}

func TestPutTupleRecords(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        expectedStr := "{\"Action\":\"pub\",\"Records\":[{\"Data\":[\"1\",\"1614915856710\",\"Test1\",\"1.1111\",\"true\",\"13.141593\",\"3\",\"3.12\",\"4\",\"5\"],\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"}},{\"Data\":[\"1\",\"1614915856710\",\"Test2\",\"-1.1111\",\"false\",\"-13.141593\",\"-3\",\"-3.12\",\"-4\",\"-5\"],\"ShardId\":\"1\",\"Attributes\":{\"key1\":\"value1\",\"key2\":\"value2\"}}]}"
        assert.Equal(t, expectedStr, str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"FailedRecordCount\": 0,\"FailedRecords\": []}"))
    }))

    defer ts.Close()

    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig(ts.URL, config, account)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false}).
        AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
        AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN}).
        AddField(datahub.Field{Name: "decimal_field", Type: datahub.DECIMAL}).
        AddField(datahub.Field{Name: "integer_field", Type: datahub.INTEGER}).
        AddField(datahub.Field{Name: "float_field", Type: datahub.FLOAT}).
        AddField(datahub.Field{Name: "smallint_field", Type: datahub.SMALLINT}).
        AddField(datahub.Field{Name: "tinyint_field", Type: datahub.TINYINT})

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewTupleRecord(recordSchema, 0)
    record1.ShardId = "0"
    record1.SetValueByName("bigint_field", 1)
    record1.SetValueByName("timestamp_field", 1614915856710)
    record1.SetValueByName("string_field", "Test1")
    record1.SetValueByName("double_field", 1.1111)
    record1.SetValueByName("boolean_field", true)
    record1.SetValueByName("decimal_field", decimal.NewFromFloat32(13.1415926))
    record1.SetValueByName("integer_field", 3)
    record1.SetValueByName("float_field", float32(3.12))
    record1.SetValueByName("smallint_field", 4)
    record1.SetValueByName("tinyint_field", 5)

    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewTupleRecord(recordSchema, 0)
    record2.ShardId = "1"
    record2.SetValueByName("bigint_field", 1)
    record2.SetValueByName("timestamp_field", 1614915856710)
    record2.SetValueByName("string_field", "Test2")
    record2.SetValueByName("double_field", -1.1111)
    record2.SetValueByName("boolean_field", false)
    record2.SetValueByName("decimal_field", decimal.NewFromFloat32(-13.1415926))
    record2.SetValueByName("integer_field", -3)
    record2.SetValueByName("float_field", float32(-3.12))
    record2.SetValueByName("smallint_field", -4)
    record2.SetValueByName("tinyint_field", -5)

    record2.SetAttribute("key1", "value1")
    record2.SetAttribute("key2", "value2")
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.FailedRecordCount)
    assert.Equal(t, []datahub.FailedRecord([]datahub.FailedRecord{}), ret.FailedRecords)
}

func TestPutRecordsWithFailedReturn(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)

        assert.Equal(t, "{\"Action\":\"pub\",\"Records\":[{\"Data\":\"QUFBQQ==\",\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"}},{\"Data\":\"QkJCQg==\",\"ShardId\":\"1\",\"Attributes\":{\"key2\":\"value2\"}}]}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"FailedRecordCount\": 2,\"FailedRecords\": [{\"ErrorCode\": \"MalformedRecord\",\"ErrorMessage\": \"Record field size not match\",\"Index\": 0}," +
            "{\"ErrorCode\": \"InvalidShardId\",\"ErrorMessage\": \"Invalid shard id\",\"Index\": 1}]}"
        _, _ = writer.Write([]byte(respBody))
    }))

    defer ts.Close()

    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig(ts.URL, config, account)

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewBlobRecord([]byte("AAAA"), 0)
    record1.ShardId = "0"
    record1.Attributes["key1"] = "value1"
    records = append(records, record1)

    record2 := datahub.NewBlobRecord([]byte("BBBB"), 0)
    record2.ShardId = "1"
    record2.Attributes["key2"] = "value2"
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 2, ret.FailedRecordCount)
    assert.Equal(t, 0, ret.FailedRecords[0].Index)
    assert.Equal(t, "MalformedRecord", ret.FailedRecords[0].ErrorCode)
    assert.Equal(t, "Record field size not match", ret.FailedRecords[0].ErrorMessage)
    assert.Equal(t, 1, ret.FailedRecords[1].Index)
    assert.Equal(t, "InvalidShardId", ret.FailedRecords[1].ErrorCode)
    assert.Equal(t, "Invalid shard id", ret.FailedRecords[1].ErrorMessage)
}

func TestPutRecordsWithNullRecord(t *testing.T) {
    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig("a", config, account)

    ret, err := dh.PutRecords("test_project", "test_topic", nil)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ret)
}

func TestPutRecordsWithNullColumn(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        expectedStr := "{\"Action\":\"pub\",\"Records\":[{\"Data\":[\"1\",\"test\"],\"ShardId\":\"0\",\"Attributes\":{\"key1\":\"value1\"}},{\"Data\":[\"1\",null],\"ShardId\":\"1\"}]}"
        assert.Equal(t, expectedStr, str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"FailedRecordCount\": 0,\"FailedRecords\": []}"))
    }))

    defer ts.Close()

    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig(ts.URL, config, account)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "f1", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f2", Type: datahub.STRING, AllowNull: true})

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewTupleRecord(recordSchema, 0)
    record1.ShardId = "0"
    record1.SetValueByName("f1", 1)
    record1.SetValueByName("f2", "test")
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewTupleRecord(recordSchema, 0)
    record2.ShardId = "1"
    record2.SetValueByName("f1", 1)
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.FailedRecordCount)
    assert.Equal(t, []datahub.FailedRecord([]datahub.FailedRecord{}), ret.FailedRecords)
}

func TestPutRecordsByShard(t *testing.T) {
    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig("a", config, account)

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewBlobRecord([]byte("AAAA"), 0)
    record1.ShardId = "0"
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewBlobRecord([]byte("BBBB"), 0)
    record2.ShardId = "1"
    record2.SetAttribute("key2", "value2")
    records = append(records, record2)

    ret, err := dh.PutRecordsByShard("test_project", "test_topic", "0", records)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ret)
}

func TestSetTupleValueOutOfRange(t *testing.T) {
    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "f1", Type: datahub.INTEGER, AllowNull: true}).
        AddField(datahub.Field{Name: "f2", Type: datahub.SMALLINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f3", Type: datahub.TINYINT, AllowNull: true})

    record1 := datahub.NewTupleRecord(recordSchema, 0)

    assert.Panics(t, func() {
        record1.SetValueByName("f1", math.MaxInt32+3)
    })
    assert.Panics(t, func() {
        record1.SetValueByName("f1", math.MinInt32-3)
    })

    assert.Panics(t, func() {
        record1.SetValueByName("f2", math.MaxInt16+3)
    })
    assert.Panics(t, func() {
        record1.SetValueByName("f2", math.MinInt32-3)
    })

    assert.Panics(t, func() {
        record1.SetValueByName("f3", math.MaxInt8+3)
    })
    assert.Panics(t, func() {
        record1.SetValueByName("f3", math.MinInt8-3)
    })
}

/**********  pb client  ****************/

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

    dh := datahub.New("a", "a", ts.URL)

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewBlobRecord([]byte("AAAA"), 0)
    record1.ShardId = "0"
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewBlobRecord([]byte("BBBB"), 0)
    record2.ShardId = "1"
    record2.SetAttribute("key2", "value2")
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.FailedRecordCount)
    assert.Equal(t, []datahub.FailedRecord(nil), ret.FailedRecords)
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

    dh := datahub.New("a", "a", ts.URL)

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewBlobRecord([]byte("AAAA"), 0)
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewBlobRecord([]byte("BBBB"), 0)
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

    dh := datahub.New("a", "a", ts.URL)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "f1", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f2", Type: datahub.STRING, AllowNull: true})

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewTupleRecord(recordSchema, 0)
    record1.ShardId = "0"
    record1.SetValueByName("f1", 1)
    record1.SetValueByName("f2", "test")
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewTupleRecord(recordSchema, 0)
    record2.ShardId = "1"
    record2.SetValueByName("f1", 1)
    records = append(records, record2)

    ret, err := dh.PutRecords("test_project", "test_topic", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.FailedRecordCount)
    assert.Equal(t, []datahub.FailedRecord(nil), ret.FailedRecords)
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

    dh := datahub.New("a", "a", ts.URL)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "f1", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f2", Type: datahub.STRING, AllowNull: true})

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewTupleRecord(recordSchema, 0)
    record1.SetValueByName("f1", 1)
    record1.SetValueByName("f2", "test")
    record1.SetAttribute("key1", "value1")
    records = append(records, record1)

    record2 := datahub.NewTupleRecord(recordSchema, 0)
    record2.SetValueByName("f1", 1)
    records = append(records, record2)

    ret, err := dh.PutRecordsByShard("test_project", "test_topic", "0", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
}
