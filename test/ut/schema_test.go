package ut

import (
    "../../datahub"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/stretchr/testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestListTopicSchema(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"ListSchema\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"RecordSchemaList\":[{\"RecordSchema\":\"{\\\"fields\\\": [{\\\"name\\\": \\\"field1\\\",\\\"type\\\": \\\"STRING\\\"},{\\\"name\\\": \\\"field2\\\",\\\"notnull\\\": true,\\\"type\\\": \\\"BIGINT\\\"}]}\",\"VersionId\":0},{\"RecordSchema\":\"{\\\"fields\\\": [{\\\"name\\\": \\\"field1\\\",\\\"type\\\": \\\"STRING\\\"},{\\\"name\\\": \\\"field2\\\",\\\"notnull\\\":true,\\\"type\\\": \\\"BIGINT\\\"},{\\\"name\\\":\\\"field3\\\",\\\"notnull\\\":true,\\\"type\\\": \\\"BIGINT\\\"}]}\",\"VersionId\":1}]}"
        _, _ = writer.Write([]byte(respBody))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.ListTopicSchema("test_project", "test_topic")
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 0, ret.SchemaInfoList[0].VersionId)
    assert.Equal(t, datahub.Field(datahub.Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.SchemaInfoList[0].RecordSchema.Fields[0])
    assert.Equal(t, datahub.Field(datahub.Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[0].RecordSchema.Fields[1])
    assert.Equal(t, 1, ret.SchemaInfoList[1].VersionId)
    assert.Equal(t, datahub.Field(datahub.Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[0])
    assert.Equal(t, datahub.Field(datahub.Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[1])
    assert.Equal(t, datahub.Field(datahub.Field{Name: "field3", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[2])
}

func TestRegisterTopicSchema(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        expectedStr := "{\"Action\":\"RegisterSchema\",\"RecordSchema\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"notnull\\\":true},{\\\"name\\\":\\\"field3\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"notnull\\\":true}]}\"}"
        assert.Equal(t, expectedStr, str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"VersionId\":1}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false}).
        AddField(datahub.Field{Name: "field3", Type: datahub.BIGINT, AllowNull: false})

    ret, err := dh.RegisterTopicSchema("test_project", "test_topic", recordSchema)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
}

func TestDeleteTopicSchema(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"DeleteSchema\",\"VersionId\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.DeleteTopicSchema("test_project", "test_topic", 1)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
}

func TestGetTopicSchemaByVersion(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"GetSchema\",\"VersionId\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"RecordSchema\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"notnull\\\":true,\\\"type\\\":\\\"BIGINT\\\"},{\\\"name\\\":\\\"field3\\\",\\\"notnull\\\":true,\\\"type\\\":\\\"BIGINT\\\"}]}\",\"VersionId\":1}"
        _, _ = writer.Write([]byte(respBody))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.GetTopicSchemaByVersion("test_project", "test_topic", 1)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 1, ret.VersionId)
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field1", Type:"STRING", AllowNull:true, Comment:""}), ret.RecordSchema.Fields[0])
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field2", Type:"BIGINT", AllowNull:false, Comment:""}), ret.RecordSchema.Fields[1])
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field3", Type:"BIGINT", AllowNull:false, Comment:""}), ret.RecordSchema.Fields[2])
}

func TestGetTopicSchemaBySchema(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        expectedStr := "{\"Action\":\"GetSchema\",\"VersionId\":-1,\"RecordSchema\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"notnull\\\":true},{\\\"name\\\":\\\"field3\\\",\\\"type\\\":\\\"BIGINT\\\",\\\"notnull\\\":true}]}\"}"
        assert.Equal(t, expectedStr, str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        respBody := "{\"RecordSchema\":\"{\\\"fields\\\":[{\\\"name\\\":\\\"field1\\\",\\\"type\\\":\\\"STRING\\\"},{\\\"name\\\":\\\"field2\\\",\\\"notnull\\\":true,\\\"type\\\":\\\"BIGINT\\\"},{\\\"name\\\":\\\"field3\\\",\\\"notnull\\\":true,\\\"type\\\":\\\"BIGINT\\\"}]}\",\"VersionId\":1}"
        _, _ = writer.Write([]byte(respBody))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false}).
        AddField(datahub.Field{Name: "field3", Type: datahub.BIGINT, AllowNull: false})

    ret, err := dh.GetTopicSchemaBySchema("test_project", "test_topic", recordSchema)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, 1, ret.VersionId)
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field1", Type:"STRING", AllowNull:true, Comment:""}), ret.RecordSchema.Fields[0])
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field2", Type:"BIGINT", AllowNull:false, Comment:""}), ret.RecordSchema.Fields[1])
    assert.Equal(t, datahub.Field(datahub.Field{Name:"field3", Type:"BIGINT", AllowNull:false, Comment:""}), ret.RecordSchema.Fields[2])
}
