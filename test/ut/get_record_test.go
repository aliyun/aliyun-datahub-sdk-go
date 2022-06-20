package ut

import (
    "../../datahub"
    "encoding/hex"
    "fmt"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/shopspring/decimal"
    "github.com/stretchr/testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "testing"
)

/**********  json client  ****************/

func TestGetBlobRecords(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"LatestSeq\":2,\"LatestTime\":3,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":1,\"Serial\":1,\"Data\":\"QUFBQQ==\",\"Attributes\":{\"key1\":\"value1\", \"key2\":\"value2\"}}]}"
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

    ret, err := dh.GetBlobRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, "30005af19b3800000000000000090001", ret.NextCursor)
    assert.Equal(t, 1, ret.RecordCount)
    assert.Equal(t, int64(1), ret.StartSequence)
    assert.Nil(t, ret.RecordSchema)
    assert.Equal(t, 1, len(ret.Records))
    data, ok := ret.Records[0].(*datahub.BlobRecord)
    assert.True(t, ok)
    assert.Equal(t, "AAAA", string(data.RawData))
    assert.Equal(t, map[string]interface{}(map[string]interface{}{"key1": "value1", "key2": "value2"}), data.Attributes)
}

func TestGetTupleRecords(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[" +
            "{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\"," +
            "\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0," +
            "\"Data\":[\"AAAA\",\"100\",\"1000\",\"1.1\",\"2.2\",\"3.3\",\"1234567890123456\",\"false\",\"10\",\"100\"]," +
            "\"Attributes\":{\"key1\":\"value1\", \"key2\":\"value2\"}}]}"
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

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.INTEGER, AllowNull: false}).
        AddField(datahub.Field{Name: "field3", Type: datahub.BIGINT}).
        AddField(datahub.Field{Name: "field4", Type: datahub.FLOAT}).
        AddField(datahub.Field{Name: "field5", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "field6", Type: datahub.DECIMAL}).
        AddField(datahub.Field{Name: "field7", Type: datahub.TIMESTAMP}).
        AddField(datahub.Field{Name: "field8", Type: datahub.BOOLEAN}).
        AddField(datahub.Field{Name: "field9", Type: datahub.TINYINT}).
        AddField(datahub.Field{Name: "field10", Type: datahub.SMALLINT})

    ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, recordSchema)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, "30005af19b3800000000000000090001", ret.NextCursor)
    assert.Equal(t, 1, ret.RecordCount)
    assert.Equal(t, int64(1), ret.StartSequence)
    assert.NotNil(t, ret.RecordSchema)
    assert.Equal(t, 1, len(ret.Records))
    data, ok := ret.Records[0].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, "AAAA", data.Values[0])
    assert.EqualValues(t, 100, data.Values[1])
    assert.EqualValues(t, 1000, data.Values[2])
    assert.EqualValues(t, 1.1, data.Values[3])
    assert.EqualValues(t, 2.2, data.Values[4])
    assert.EqualValues(t, decimal.NewFromFloat(3.3), data.Values[5])
    assert.EqualValues(t, 1234567890123456, data.Values[6])
    assert.EqualValues(t, false, data.Values[7])
    assert.EqualValues(t, 10, data.Values[8])
    assert.EqualValues(t, 100, data.Values[9])
    assert.EqualValues(t, map[string]interface{}(map[string]interface{}{"key1": "value1", "key2": "value2"}), data.Attributes)
}

func TestGetTupleRecordsWithoutSchema(t *testing.T) {
    account := datahub.NewAliyunAccount("a", "a")
    config := &datahub.Config{
        CompressorType: datahub.NOCOMPRESS,
        EnableBinary:   false,
        HttpClient:     datahub.DefaultHttpClient(),
    }
    dh := datahub.NewClientWithConfig("a", config, account)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.INTEGER, AllowNull: false}).
        AddField(datahub.Field{Name: "field3", Type: datahub.BIGINT}).
        AddField(datahub.Field{Name: "field4", Type: datahub.FLOAT}).
        AddField(datahub.Field{Name: "field5", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "field6", Type: datahub.DECIMAL}).
        AddField(datahub.Field{Name: "field7", Type: datahub.TIMESTAMP}).
        AddField(datahub.Field{Name: "field8", Type: datahub.BOOLEAN}).
        AddField(datahub.Field{Name: "field9", Type: datahub.TINYINT}).
        AddField(datahub.Field{Name: "field10", Type: datahub.SMALLINT})

    ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, nil)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ret)
}

// record is not empty array, but data is null, e.g.
/*{
    "Records":[
        {
        "SystemTime":1525783352873,
        "NextCursor":"30005af19b3800000000000000010000",
        "Cursor":"30005af19b3800000000000000000000",
        "Sequence":0
        }
    ]
}*/
func TestGetTupleRecordsWithNullRecordReturn(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0}]}"
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

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false})

    ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, recordSchema)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ret)
}

// record is empty array, e.g. {"Records":[]}
func TestGetTupleRecordsWithEmptyRecordReturn(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"NextCursor\":\"30005af19b3800000000000000000000\",\"RecordCount\":0,\"StartSeq\":-1,\"LatestSeq\":4566380,\"LatestTime\":1608887046022,\"Records\":[]}"
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

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false})

    ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, recordSchema)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, "30005af19b3800000000000000000000", ret.NextCursor)
    assert.Equal(t, 0, len(ret.Records))
}

func TestGetTupleWithBlobRecordDataReturn(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"sub\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Limit\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"NextCursor\":\"30005af19b3800000000000000090001\",\"RecordCount\":1,\"StartSeq\":1,\"Records\":[{\"SystemTime\":1525783352873,\"NextCursor\":\"30005af19b3800000000000000010000\",\"Cursor\":\"30005af19b3800000000000000000000\",\"Sequence\":0,\"Data\":\"QUFBQQ==\"}]}"
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

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false})

    ret, err := dh.GetTupleRecords("test_project", "test_topic", "0", "30005af19b3800000000000000000000", 1, recordSchema)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ret)
}

/**********  pb client  ****************/

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

    dh := datahub.New("a", "a", ts.URL)

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
    data, ok := ret.Records[0].(*datahub.BlobRecord)
    assert.True(t, ok)
    assert.Equal(t, "AAAA", string(data.RawData))
    assert.Equal(t, map[string]interface {}(map[string]interface {}{"key1":"value1"}), data.Attributes)
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

    dh := datahub.New("a", "a", ts.URL)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false})

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
    data, ok := ret.Records[0].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, "test", data.Values[0])
    assert.EqualValues(t, 1, data.Values[1])
}
