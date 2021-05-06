package ut

import (
    "../../datahub"
    "fmt"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/stretchr/Testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestListTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "/projects/test_project/topics", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"TopicNames\": [\"topic1\", \"topic2\"]}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    lt, err := dh.ListTopic("test_project")
    assert.Nil(t, err)
    assert.NotNil(t, lt)
    assert.Equal(t, http.StatusOK, lt.StatusCode)
    assert.Equal(t, "request_id", lt.RequestId)
    assert.Equal(t, "topic1", lt.TopicNames[0])
    assert.Equal(t, "topic2", lt.TopicNames[1])
}

func TestListTopicWithFilter(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "/projects/test_project/topics", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "topic", request.URL.Query().Get("filter"))

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"TopicNames\": [\"topic1\", \"topic2\"]}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    lt, err := dh.ListTopicWithFilter("test_project", "topic")
    assert.Nil(t, err)
    assert.NotNil(t, lt)
    assert.Equal(t, http.StatusOK, lt.StatusCode)
    assert.Equal(t, "request_id", lt.RequestId)
    assert.Equal(t, "topic1", lt.TopicNames[0])
    assert.Equal(t, "topic2", lt.TopicNames[1])
}

func TestCreateBlobTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":3,\"RecordType\":\"BLOB\",\"Comment\":\"test\",\"ExpandMode\":\"\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusCreated)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    cb, err := dh.CreateBlobTopic("test_project", "test_topic", "test", 1, 3)
    assert.Nil(t, err)
    assert.NotNil(t, cb)
    assert.Equal(t, http.StatusCreated, cb.StatusCode)
    assert.Equal(t, "request_id", cb.RequestId)
}

func TestCreateTupleTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        expectedStr := "{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":1,\"RecordType\":\"TUPLE\"," +
            "\"RecordSchema\":\"{\\\"fields\\\":" +
            "[" +
            "{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"INTEGER\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field3\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field4\\\",\\\"type\\\":\\\"FLOAT\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field5\\\",\\\"type\\\":\\\"DOUBLE\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field6\\\",\\\"type\\\":\\\"DECIMAL\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field7\\\",\\\"type\\\":\\\"TIMESTAMP\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field8\\\",\\\"type\\\":\\\"BOOLEAN\\\",\\\"comment\\\":\\\"test\\\"}," +
            "{\\\"name\\\":\\\"field9\\\",\\\"type\\\":\\\"SMALLINT\\\",\\\"notnull\\\":true,\\\"comment\\\":\\\"test9\\\"}," +
            "{\\\"name\\\":\\\"field10\\\",\\\"type\\\":\\\"TINYINT\\\",\\\"notnull\\\":true,\\\"comment\\\":\\\"test10\\\"}" +
            "]}\"," +
            "\"Comment\":\"test comment\",\"ExpandMode\":\"\"}"
        assert.Equal(t, expectedStr, str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusCreated)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    fields := []datahub.Field{
        {"field1", datahub.STRING, true, "test"},
        {"field2", datahub.INTEGER, true, "test"},
        {"field3", datahub.BIGINT, true, "test"},
        {"field4", datahub.FLOAT, true, "test"},
        {"field5", datahub.DOUBLE, true, "test"},
        {"field6", datahub.DECIMAL, true, "test"},
        {"field7", datahub.TIMESTAMP, true, "test"},
        {"field8", datahub.BOOLEAN, true, "test"},
        {"field9", datahub.SMALLINT, false, "test9"},
        {"field10", datahub.TINYINT, false, "test10"},
    }
    schema := &datahub.RecordSchema{
        Fields: fields,
    }

    cb, err := dh.CreateTupleTopic("test_project", "test_topic", "test comment", 1, 1, schema)
    assert.Nil(t, err)
    assert.NotNil(t, cb)
    assert.Equal(t, http.StatusCreated, cb.StatusCode)
    assert.Equal(t, "request_id", cb.RequestId)
}

func TestCreateTopicWithExpandMode(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":3,\"RecordType\":\"BLOB\",\"Comment\":\"test\",\"ExpandMode\":\"extend\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusCreated)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    tp := &datahub.CreateTopicParameter{
        ShardCount:   1,
        LifeCycle:    3,
        Comment:      "test",
        RecordType:   datahub.BLOB,
        RecordSchema: nil,
        ExpandMode:   datahub.ONLY_EXTEND,
    }

    ct, err := dh.CreateTopicWithPara("test_project", "test_topic", tp)
    assert.Nil(t, err)
    assert.NotNil(t, ct)
    assert.Equal(t, http.StatusCreated, ct.StatusCode)
    assert.Equal(t, "request_id", ct.RequestId)
}

func TestCreateTopicWithInvalidName(t *testing.T) {
    dh := datahub.New("a", "a", "a")

    tp := &datahub.CreateTopicParameter{
        ShardCount:   1,
        LifeCycle:    3,
        Comment:      "test",
        RecordType:   datahub.BLOB,
        RecordSchema: nil,
        ExpandMode:   datahub.ONLY_EXTEND,
    }

    ct, err := dh.CreateTopicWithPara("test_project", "test_topic--", tp)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ct)
}

func TestCreateTopicWithNullSchema(t *testing.T) {
    dh := datahub.New("a", "a", "a")

    tp := &datahub.CreateTopicParameter{
        ShardCount:   1,
        LifeCycle:    3,
        Comment:      "test",
        RecordType:   datahub.TUPLE,
        RecordSchema: nil,
        ExpandMode:   datahub.ONLY_EXTEND,
    }

    ct, err := dh.CreateTopicWithPara("test_project", "test_topic", tp)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ct)
}

func TestUpdateTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.PUT, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Comment\":\"test update\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ut, err := dh.UpdateTopic("test_project", "test_topic", "test update")
    assert.Nil(t, err)
    assert.NotNil(t, ut)
    assert.Equal(t, http.StatusOK, ut.StatusCode)
    assert.Equal(t, "request_id", ut.RequestId)
}

func TestUpdateTopicLifecycle(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.PUT, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Comment\":\"test\",\"Lifecycle\":3}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    para := &datahub.UpdateTopicParameter{
        LifeCycle: 3,
        Comment:   "test",
    }

    ut, err := dh.UpdateTopicWithPara("test_project", "test_topic", para)
    assert.Nil(t, err)
    assert.NotNil(t, ut)
    assert.Equal(t, http.StatusOK, ut.StatusCode)
    assert.Equal(t, "request_id", ut.RequestId)
}

func TestDeleteTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.DELETE, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    dt, err := dh.DeleteTopic("test_project", "test_topic")
    assert.Nil(t, err)
    assert.NotNil(t, dt)
    assert.Equal(t, http.StatusOK, dt.StatusCode)
    assert.Equal(t, "request_id", dt.RequestId)
}

func TestGetBlobTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"Comment\": \"test topic blob\",\"CreateTime\": 1525763481,\"LastModifyTime\": 1525763481,\"Lifecycle\": 1,\"RecordSchema\": \"\",\"RecordType\": \"BLOB\",\"ShardCount\": 4,\"ExpandMode\":\"extend\", \"Status\": \"on\"}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gt, err := dh.GetTopic("test_project", "test_topic")
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    assert.Equal(t, http.StatusOK, gt.StatusCode)
    assert.Equal(t, "request_id", gt.RequestId)
    assert.Equal(t, 4, gt.ShardCount)
    assert.Equal(t, 1, gt.LifeCycle)
    assert.Equal(t, datahub.BLOB, gt.RecordType)
    assert.Nil(t, gt.RecordSchema)
    assert.Equal(t, "test topic blob", gt.Comment)
    assert.Equal(t, int64(1525763481), gt.CreateTime)
    assert.Equal(t, int64(1525763481), gt.LastModifyTime)
    assert.Equal(t, datahub.TOPIC_ON, gt.TopicStatus)
    assert.Equal(t, datahub.ONLY_EXTEND, gt.ExpandMode)
}

func TestGetTupleTopic(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        body := "{\"Comment\": \"test topic tuple\",\"CreateTime\": 1525763481,\"LastModifyTime\": 1525763481,\"Lifecycle\": 1," +
            "\"RecordSchema\": \"{\\\"fields\\\":[" +
            "{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"}," +
            "{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"INTEGER\\\"}," +
            "{\\\"name\\\":\\\"field3\\\",\\\"type\\\":\\\"BIGINT\\\"}," +
            "{\\\"name\\\":\\\"field4\\\",\\\"type\\\":\\\"FLOAT\\\"}," +
            "{\\\"name\\\":\\\"field5\\\",\\\"type\\\":\\\"DOUBLE\\\"}," +
            "{\\\"name\\\":\\\"field6\\\",\\\"type\\\":\\\"DECIMAL\\\"}," +
            "{\\\"name\\\":\\\"field7\\\",\\\"type\\\":\\\"TIMESTAMP\\\",\\\"notnull\\\":false}," +
            "{\\\"name\\\":\\\"field8\\\",\\\"type\\\":\\\"BOOLEAN\\\",\\\"notnull\\\":true}," +
            "{\\\"name\\\":\\\"field9\\\",\\\"type\\\":\\\"SMALLINT\\\"}," +
            "{\\\"name\\\":\\\"field10\\\",\\\"type\\\":\\\"TINYINT\\\",\\\"Comment\\\":\\\"test\\\"}]}\"," +
            "\"RecordType\": \"TUPLE\",\"ShardCount\": 4,\"ExpandMode\":\"split\",\"Status\": \"off\"}"
        _, _ = writer.Write([]byte(body))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gt, err := dh.GetTopic("test_project", "test_topic")
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    assert.Equal(t, http.StatusOK, gt.StatusCode)
    assert.Equal(t, "request_id", gt.RequestId)
    assert.Equal(t, 4, gt.ShardCount)
    assert.Equal(t, 1, gt.LifeCycle)
    assert.Equal(t, "test topic tuple", gt.Comment)
    assert.Equal(t, int64(1525763481), gt.CreateTime)
    assert.Equal(t, int64(1525763481), gt.LastModifyTime)
    assert.Equal(t, datahub.TOPIC_OFF, gt.TopicStatus)
    assert.Equal(t, datahub.ONLY_SPLIT, gt.ExpandMode)

    assert.Equal(t, datahub.TUPLE, gt.RecordType)
    assert.NotNil(t, gt.RecordSchema)
    assert.Equal(t, 10, gt.RecordSchema.Size())
    assert.Equal(t, "field1", gt.RecordSchema.Fields[0].Name)
    assert.Equal(t, datahub.STRING, gt.RecordSchema.Fields[0].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[0].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[0].Comment)

    assert.Equal(t, "field2", gt.RecordSchema.Fields[1].Name)
    assert.Equal(t, datahub.INTEGER, gt.RecordSchema.Fields[1].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[1].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[1].Comment)

    assert.Equal(t, "field3", gt.RecordSchema.Fields[2].Name)
    assert.Equal(t, datahub.BIGINT, gt.RecordSchema.Fields[2].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[2].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[2].Comment)

    assert.Equal(t, "field4", gt.RecordSchema.Fields[3].Name)
    assert.Equal(t, datahub.FLOAT, gt.RecordSchema.Fields[3].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[3].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[3].Comment)

    assert.Equal(t, "field5", gt.RecordSchema.Fields[4].Name)
    assert.Equal(t, datahub.DOUBLE, gt.RecordSchema.Fields[4].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[4].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[4].Comment)

    assert.Equal(t, "field6", gt.RecordSchema.Fields[5].Name)
    assert.Equal(t, datahub.DECIMAL, gt.RecordSchema.Fields[5].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[5].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[5].Comment)

    assert.Equal(t, "field7", gt.RecordSchema.Fields[6].Name)
    assert.Equal(t, datahub.TIMESTAMP, gt.RecordSchema.Fields[6].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[6].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[6].Comment)

    assert.Equal(t, "field8", gt.RecordSchema.Fields[7].Name)
    assert.Equal(t, datahub.BOOLEAN, gt.RecordSchema.Fields[7].Type)
    assert.Equal(t, false, gt.RecordSchema.Fields[7].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[7].Comment)

    assert.Equal(t, "field9", gt.RecordSchema.Fields[8].Name)
    assert.Equal(t, datahub.SMALLINT, gt.RecordSchema.Fields[8].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[8].AllowNull)
    assert.Equal(t, "", gt.RecordSchema.Fields[8].Comment)

    assert.Equal(t, "field10", gt.RecordSchema.Fields[9].Name)
    assert.Equal(t, datahub.TINYINT, gt.RecordSchema.Fields[9].Type)
    assert.Equal(t, true, gt.RecordSchema.Fields[9].AllowNull)
    assert.Equal(t, "test", gt.RecordSchema.Fields[9].Comment)
}
