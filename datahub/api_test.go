package datahub

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/stretchr/testify/assert"
)

func TestListProject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, request.URL.EscapedPath(), "/projects")

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ProjectNames\": [\"project1\", \"project2\"]}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	lp, err := dh.ListProject()
	assert.Nil(t, err)
	assert.NotNil(t, lp)
	assert.Equal(t, http.StatusOK, lp.StatusCode)
	assert.Equal(t, "request_id", lp.RequestId)
	assert.Equal(t, "project1", lp.ProjectNames[0])
	assert.Equal(t, "project2", lp.ProjectNames[1])
}

func TestListProjectWithFilter(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects", request.URL.EscapedPath())
		assert.Equal(t, "project.*", request.URL.Query().Get("filter"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ProjectNames\": [\"project1\", \"project2\"]}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	lp, err := dh.ListProjectWithFilter("project.*")
	assert.Nil(t, err)
	assert.NotNil(t, lp)
	assert.Equal(t, http.StatusOK, lp.StatusCode)
	assert.Equal(t, "request_id", lp.RequestId)
	assert.Equal(t, "project1", lp.ProjectNames[0])
	assert.Equal(t, "project2", lp.ProjectNames[1])
}

func TestGetProject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"CreateTime\": 1, \"LastModifyTime\":2, \"Comment\":\"test\"}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	gp, err := dh.GetProject("test_project")
	assert.Nil(t, err)
	assert.NotNil(t, gp)
	assert.Equal(t, http.StatusOK, gp.StatusCode)
	assert.Equal(t, "request_id", gp.RequestId)
	assert.Equal(t, int64(1), gp.CreateTime)
	assert.Equal(t, int64(2), gp.LastModifyTime)
	assert.Equal(t, "test", gp.Comment)
	assert.Equal(t, "test_project", gp.ProjectName)
}

func TestGetProjectWithInvalidName(t *testing.T) {
	dh := New("a", "a", "a")

	gp, err := dh.GetProject("test-")
	assert.NotNil(t, err)
	assert.Nil(t, gp)
}

func TestCreateProject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"test_comment\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusCreated)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	cp, err := dh.CreateProject("test_project", "test_comment")
	assert.Nil(t, err)
	assert.NotNil(t, cp)
	assert.Equal(t, http.StatusCreated, cp.StatusCode)
	assert.Equal(t, "request_id", cp.RequestId)
}

func TestCreateProjectWithInvalidName(t *testing.T) {
	dh := New("a", "a", "a")

	cp, err := dh.CreateProject("test_project-", "test_comment")
	assert.NotNil(t, err)
	assert.Nil(t, cp)
}

func TestUpdateProject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"update_comment\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	up, err := dh.UpdateProject("test_project", "update_comment")
	assert.Nil(t, err)
	assert.NotNil(t, up)
	assert.Equal(t, http.StatusOK, up.StatusCode)
	assert.Equal(t, "request_id", up.RequestId)
}

func TestDeleteProject(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.DELETE, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	dp, err := dh.DeleteProject("test_project")
	assert.Nil(t, err)
	assert.NotNil(t, dp)
	assert.Equal(t, http.StatusOK, dp.StatusCode)
	assert.Equal(t, "request_id", dp.RequestId)
}

func TestUpdateProjectVpcWhiteList(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"VpcIds\":\"111,2222\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	up, err := dh.UpdateProjectVpcWhitelist("test_project", "111,2222")
	assert.Nil(t, err)
	assert.NotNil(t, up)
	assert.Equal(t, http.StatusOK, up.StatusCode)
	assert.Equal(t, "request_id", up.RequestId)
}

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
	dh := New("a", "a", ts.URL)

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
	dh := New("a", "a", ts.URL)

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
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":3,\"RecordType\":\"BLOB\",\"Comment\":\"test\",\"ExpandMode\":\"\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusCreated)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

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
		body, err := io.ReadAll(request.Body)
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
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	fields := []Field{
		{"field1", STRING, true, "test"},
		{"field2", INTEGER, true, "test"},
		{"field3", BIGINT, true, "test"},
		{"field4", FLOAT, true, "test"},
		{"field5", DOUBLE, true, "test"},
		{"field6", DECIMAL, true, "test"},
		{"field7", TIMESTAMP, true, "test"},
		{"field8", BOOLEAN, true, "test"},
		{"field9", SMALLINT, false, "test9"},
		{"field10", TINYINT, false, "test10"},
	}
	schema := &RecordSchema{
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
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"create\",\"ShardCount\":1,\"Lifecycle\":3,\"RecordType\":\"BLOB\",\"Comment\":\"test\",\"ExpandMode\":\"extend\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusCreated)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	tp := &CreateTopicParameter{
		ShardCount:   1,
		LifeCycle:    3,
		Comment:      "test",
		RecordType:   BLOB,
		RecordSchema: nil,
		ExpandMode:   ONLY_EXTEND,
	}

	ct, err := dh.CreateTopicWithPara("test_project", "test_topic", tp)
	assert.Nil(t, err)
	assert.NotNil(t, ct)
	assert.Equal(t, http.StatusCreated, ct.StatusCode)
	assert.Equal(t, "request_id", ct.RequestId)
}

func TestCreateTopicWithInvalidName(t *testing.T) {
	dh := New("a", "a", "a")

	tp := &CreateTopicParameter{
		ShardCount:   1,
		LifeCycle:    3,
		Comment:      "test",
		RecordType:   BLOB,
		RecordSchema: nil,
		ExpandMode:   ONLY_EXTEND,
	}

	ct, err := dh.CreateTopicWithPara("test_project", "test_topic--", tp)
	assert.NotNil(t, err)
	assert.Nil(t, ct)
}

func TestCreateTopicWithNullSchema(t *testing.T) {
	dh := New("a", "a", "a")

	tp := &CreateTopicParameter{
		ShardCount:   1,
		LifeCycle:    3,
		Comment:      "test",
		RecordType:   TUPLE,
		RecordSchema: nil,
		ExpandMode:   ONLY_EXTEND,
	}

	ct, err := dh.CreateTopicWithPara("test_project", "test_topic", tp)
	assert.NotNil(t, err)
	assert.Nil(t, ct)
}

func TestUpdateTopic(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"test update\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

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
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"test\",\"Lifecycle\":3}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	para := &UpdateTopicParameter{
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
	dh := New("a", "a", ts.URL)

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
	dh := New("a", "a", ts.URL)

	gt, err := dh.GetTopic("test_project", "test_topic")
	assert.Nil(t, err)
	assert.NotNil(t, gt)
	assert.Equal(t, http.StatusOK, gt.StatusCode)
	assert.Equal(t, "request_id", gt.RequestId)
	assert.Equal(t, 4, gt.ShardCount)
	assert.Equal(t, 1, gt.LifeCycle)
	assert.Equal(t, BLOB, gt.RecordType)
	assert.Nil(t, gt.RecordSchema)
	assert.Equal(t, "test topic blob", gt.Comment)
	assert.Equal(t, int64(1525763481), gt.CreateTime)
	assert.Equal(t, int64(1525763481), gt.LastModifyTime)
	assert.Equal(t, TOPIC_ON, gt.TopicStatus)
	assert.Equal(t, ONLY_EXTEND, gt.ExpandMode)
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
	dh := New("a", "a", ts.URL)

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
	assert.Equal(t, TOPIC_OFF, gt.TopicStatus)
	assert.Equal(t, ONLY_SPLIT, gt.ExpandMode)

	assert.Equal(t, TUPLE, gt.RecordType)
	assert.NotNil(t, gt.RecordSchema)
	assert.Equal(t, 10, gt.RecordSchema.Size())
	assert.Equal(t, "field1", gt.RecordSchema.Fields[0].Name)
	assert.Equal(t, STRING, gt.RecordSchema.Fields[0].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[0].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[0].Comment)

	assert.Equal(t, "field2", gt.RecordSchema.Fields[1].Name)
	assert.Equal(t, INTEGER, gt.RecordSchema.Fields[1].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[1].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[1].Comment)

	assert.Equal(t, "field3", gt.RecordSchema.Fields[2].Name)
	assert.Equal(t, BIGINT, gt.RecordSchema.Fields[2].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[2].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[2].Comment)

	assert.Equal(t, "field4", gt.RecordSchema.Fields[3].Name)
	assert.Equal(t, FLOAT, gt.RecordSchema.Fields[3].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[3].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[3].Comment)

	assert.Equal(t, "field5", gt.RecordSchema.Fields[4].Name)
	assert.Equal(t, DOUBLE, gt.RecordSchema.Fields[4].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[4].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[4].Comment)

	assert.Equal(t, "field6", gt.RecordSchema.Fields[5].Name)
	assert.Equal(t, DECIMAL, gt.RecordSchema.Fields[5].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[5].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[5].Comment)

	assert.Equal(t, "field7", gt.RecordSchema.Fields[6].Name)
	assert.Equal(t, TIMESTAMP, gt.RecordSchema.Fields[6].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[6].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[6].Comment)

	assert.Equal(t, "field8", gt.RecordSchema.Fields[7].Name)
	assert.Equal(t, BOOLEAN, gt.RecordSchema.Fields[7].Type)
	assert.Equal(t, false, gt.RecordSchema.Fields[7].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[7].Comment)

	assert.Equal(t, "field9", gt.RecordSchema.Fields[8].Name)
	assert.Equal(t, SMALLINT, gt.RecordSchema.Fields[8].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[8].AllowNull)
	assert.Equal(t, "", gt.RecordSchema.Fields[8].Comment)

	assert.Equal(t, "field10", gt.RecordSchema.Fields[9].Name)
	assert.Equal(t, TINYINT, gt.RecordSchema.Fields[9].Type)
	assert.Equal(t, true, gt.RecordSchema.Fields[9].AllowNull)
	assert.Equal(t, "test", gt.RecordSchema.Fields[9].Comment)

	assert.Equal(t, 10, gt.RecordSchema.Size())
	assert.Equal(t, 0, gt.RecordSchema.GetFieldIndex("field1"))
	assert.Equal(t, 1, gt.RecordSchema.GetFieldIndex("field2"))
	assert.Equal(t, 2, gt.RecordSchema.GetFieldIndex("field3"))
	assert.Equal(t, 3, gt.RecordSchema.GetFieldIndex("field4"))
	assert.Equal(t, 4, gt.RecordSchema.GetFieldIndex("field5"))
	assert.Equal(t, 5, gt.RecordSchema.GetFieldIndex("field6"))
	assert.Equal(t, 6, gt.RecordSchema.GetFieldIndex("field7"))
	assert.Equal(t, 7, gt.RecordSchema.GetFieldIndex("field8"))
	assert.Equal(t, 8, gt.RecordSchema.GetFieldIndex("field9"))
	assert.Equal(t, 9, gt.RecordSchema.GetFieldIndex("field10"))
}

func TestListTopicSchema(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
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
	dh := New("a", "a", ts.URL)

	ret, err := dh.ListTopicSchema("test_project", "test_topic")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 0, ret.SchemaInfoList[0].VersionId)
	assert.Equal(t, Field(Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.SchemaInfoList[0].RecordSchema.Fields[0])
	assert.Equal(t, Field(Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[0].RecordSchema.Fields[1])
	assert.Equal(t, 1, ret.SchemaInfoList[1].VersionId)
	assert.Equal(t, Field(Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[0])
	assert.Equal(t, Field(Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[1])
	assert.Equal(t, Field(Field{Name: "field3", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.SchemaInfoList[1].RecordSchema.Fields[2])
}

func TestRegisterTopicSchema(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
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
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	recordSchema := NewRecordSchema()
	recordSchema.AddField(Field{Name: "field1", Type: STRING, AllowNull: true})
	recordSchema.AddField(Field{Name: "field2", Type: BIGINT, AllowNull: false})
	recordSchema.AddField(Field{Name: "field3", Type: BIGINT, AllowNull: false})

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
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"DeleteSchema\",\"VersionId\":1}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

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
		body, err := io.ReadAll(request.Body)
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
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetTopicSchemaByVersion("test_project", "test_topic", 1)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 1, ret.VersionId)
	assert.Equal(t, Field(Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.RecordSchema.Fields[0])
	assert.Equal(t, Field(Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.RecordSchema.Fields[1])
	assert.Equal(t, Field(Field{Name: "field3", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.RecordSchema.Fields[2])
}

func TestGetTopicSchemaBySchema(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, "/projects/test_project/topics/test_topic", request.URL.EscapedPath())

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
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
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	recordSchema := NewRecordSchema()
	recordSchema.AddField(Field{Name: "field1", Type: STRING, AllowNull: true})
	recordSchema.AddField(Field{Name: "field2", Type: BIGINT, AllowNull: false})
	recordSchema.AddField(Field{Name: "field3", Type: BIGINT, AllowNull: false})

	ret, err := dh.GetTopicSchemaBySchema("test_project", "test_topic", recordSchema)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 1, ret.VersionId)
	assert.Equal(t, Field(Field{Name: "field1", Type: "STRING", AllowNull: true, Comment: ""}), ret.RecordSchema.Fields[0])
	assert.Equal(t, Field(Field{Name: "field2", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.RecordSchema.Fields[1])
	assert.Equal(t, Field(Field{Name: "field3", Type: "BIGINT", AllowNull: false, Comment: ""}), ret.RecordSchema.Fields[2])
}

func TestListShard(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		body := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}," +
			"{\"ShardId\":\"1\", \"State\":\"CLOSED\",\"ClosedTime\":100, \"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\", \"LeftShardId\":\"0\",\"RightShardId\":\"1\"}],\"Protocol\":\"http1.1\",\"Interval\":500}"
		_, _ = writer.Write([]byte(body))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ls, err := dh.ListShard("test_project", "test_topic")
	assert.Nil(t, err)
	assert.NotNil(t, ls)
	assert.Equal(t, http.StatusOK, ls.StatusCode)
	assert.Equal(t, "request_id", ls.RequestId)
	assert.Equal(t, 2, len(ls.Shards))
	assert.Equal(t, "http1.1", ls.Protocol)
	assert.Equal(t, int64(500), ls.IntervalMs)
	assert.Equal(t, "0", ls.Shards[0].ShardId)
	assert.Equal(t, ACTIVE, ls.Shards[0].State)
	assert.Equal(t, "00000000000000000000000000000000", ls.Shards[0].BeginHashKey)
	assert.Equal(t, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ls.Shards[0].EndHashKey)
	assert.Equal(t, int64(100), ls.Shards[0].ClosedTime)
	assert.Equal(t, 0, len(ls.Shards[0].ParentShardIds))
	assert.Equal(t, "0", ls.Shards[0].LeftShardId)
	assert.Equal(t, "4294967295", ls.Shards[0].RightShardId)
	assert.Equal(t, "", ls.Shards[0].Address)

	assert.Equal(t, "1", ls.Shards[1].ShardId)
	assert.Equal(t, CLOSED, ls.Shards[1].State)
	assert.Equal(t, "00000000000000000000000000000000", ls.Shards[1].BeginHashKey)
	assert.Equal(t, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ls.Shards[1].EndHashKey)
	assert.Equal(t, int64(100), ls.Shards[1].ClosedTime)
	assert.Equal(t, 0, len(ls.Shards[1].ParentShardIds))
	assert.Equal(t, "0", ls.Shards[1].LeftShardId)
	assert.Equal(t, "1", ls.Shards[1].RightShardId)
	assert.Equal(t, "", ls.Shards[1].Address)
}

func TestWaitShard(t *testing.T) {
	cnt := 0
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)

		body1 := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"OPENING\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}"
		body2 := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}"

		if cnt == 0 {
			_, _ = writer.Write([]byte(body1))
			cnt = cnt + 1
		} else {
			_, _ = writer.Write([]byte(body2))
		}
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	assert.Equal(t, 0, cnt)
	ret := dh.WaitAllShardsReadyWithTime("test_project", "test_topic", 60)
	assert.True(t, ret)
	assert.Equal(t, 1, cnt)
}

func TestWaitShardTimeout(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)

		body := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"OPENING\",\"ClosedTime\":100,\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ParentShardIds\":[],\"LeftShardId\":\"0\",\"RightShardId\":\"4294967295\"}]}"
		_, _ = writer.Write([]byte(body))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret := dh.WaitAllShardsReadyWithTime("test_project", "test_topic", 3)
	assert.False(t, ret)
}

func TestSplitShard(t *testing.T) {
	cnt := 0
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if cnt == 0 {
			assert.Equal(t, requests.GET, request.Method)
		} else {
			assert.Equal(t, requests.POST, request.Method)
		}
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		if cnt == 1 {
			defer request.Body.Close()
			body, err := io.ReadAll(request.Body)
			assert.Nil(t, err)
			assert.NotNil(t, body)
			str := string(body)
			assert.Equal(t, "{\"Action\":\"split\",\"ShardId\":\"0\",\"SplitKey\":\"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}", str)
		}

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody1 := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"BeginHashKey\":\"00000000000000000000000000000000\", \"EndHashKey\":\"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}]}"
		respBody2 := "{\"NewShards\": [{\"BeginHashKey\": \"000\",\"EndHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"1\"}, {\"BeginHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"EndHashKey\": \"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"2\"}]}"
		if cnt == 0 {
			_, _ = writer.Write([]byte(respBody1))
			cnt = cnt + 1
		} else {
			_, _ = writer.Write([]byte(respBody2))
		}

	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	assert.Equal(t, cnt, 0)
	ss, err := dh.SplitShard("test_project", "test_topic", "0")
	assert.Equal(t, cnt, 1)
	assert.Nil(t, err)
	assert.NotNil(t, ss)
	assert.Equal(t, http.StatusOK, ss.StatusCode)
	assert.Equal(t, "request_id", ss.RequestId)
	assert.Equal(t, 2, len(ss.NewShards))
	assert.Equal(t, "1", ss.NewShards[0].ShardId)
	assert.Equal(t, "000", ss.NewShards[0].BeginHashKey)
	assert.Equal(t, "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[0].EndHashKey)
	assert.Equal(t, "2", ss.NewShards[1].ShardId)
	assert.Equal(t, "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[1].BeginHashKey)
	assert.Equal(t, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[1].EndHashKey)
}

func TestSplitShardWithHashKey(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"split\",\"ShardId\":\"0\",\"SplitKey\":\"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"NewShards\": [{\"BeginHashKey\": \"000\",\"EndHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"1\"}, {\"BeginHashKey\": \"7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"EndHashKey\": \"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF\",\"ShardId\": \"2\"}]}"

		_, _ = writer.Write([]byte(respBody))

	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	ss, err := dh.SplitShardBySplitKey("test_project", "test_topic", "0", "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
	assert.Nil(t, err)
	assert.NotNil(t, ss)
	assert.Equal(t, http.StatusOK, ss.StatusCode)
	assert.Equal(t, "request_id", ss.RequestId)
	assert.Equal(t, 2, len(ss.NewShards))
	assert.Equal(t, "1", ss.NewShards[0].ShardId)
	assert.Equal(t, "000", ss.NewShards[0].BeginHashKey)
	assert.Equal(t, "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[0].EndHashKey)
	assert.Equal(t, "2", ss.NewShards[1].ShardId)
	assert.Equal(t, "7FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[1].BeginHashKey)
	assert.Equal(t, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ss.NewShards[1].EndHashKey)
}

func TestSplitShardWithInvalidShardId(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"Shards\":[{\"ShardId\":\"0\",\"State\":\"ACTIVE\",\"BeginHashKey\":\"FFF\", \"EndHashKey\":\"000\"}]}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ss, err := dh.SplitShard("test_project", "test_topic", "1")
	assert.NotNil(t, err)
	assert.Nil(t, ss)
}

func TestSplitShardWithInvalidShardIdFormat(t *testing.T) {
	dh := New("a", "a", "a")

	ss, err := dh.SplitShard("test_project", "test_topic", "aa")
	assert.NotNil(t, err)
	assert.Nil(t, ss)
}

func TestMergeShard(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"merge\",\"ShardId\":\"0\",\"AdjacentShardId\":\"1\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"BeginHashKey\": \"000\",\"EndHashKey\": \"FFF\",\"ShardId\": \"2\"}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ms, err := dh.MergeShard("test_project", "test_topic", "0", "1")
	assert.Nil(t, err)
	assert.NotNil(t, ms)
	assert.Equal(t, http.StatusOK, ms.StatusCode)
	assert.Equal(t, "request_id", ms.RequestId)
	assert.Equal(t, "2", ms.ShardId)
	assert.Equal(t, "000", ms.BeginHashKey)
	assert.Equal(t, "FFF", ms.EndHashKey)
}

func TestExtendShard(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"extend\",\"ExtendMode\":\"TO\",\"ShardNumber\":3}", str)
		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	es, err := dh.ExtendShard("test_project", "test_topic", 3)
	assert.Nil(t, err)
	assert.NotNil(t, es)
	assert.Equal(t, http.StatusOK, es.StatusCode)
	assert.Equal(t, "request_id", es.RequestId)
}

func TestGetCursor(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"cursor\",\"Type\":\"OLDEST\"}", str)
		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Cursor\": \"30005af19b3800000000000000000000\",\"RecordTime\": 1525783352873,\"Sequence\": 1}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	gc, err := dh.GetCursor("test_project", "test_topic", "0", OLDEST)
	assert.Nil(t, err)
	assert.NotNil(t, gc)
	assert.Equal(t, http.StatusOK, gc.StatusCode)
	assert.Equal(t, "request_id", gc.RequestId)
	assert.Equal(t, int64(1), gc.Sequence)
	assert.Equal(t, int64(1525783352873), gc.RecordTime)
	assert.Equal(t, "30005af19b3800000000000000000000", gc.Cursor)
}

func TestGetCursor2(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"cursor\",\"Type\":\"SEQUENCE\",\"Sequence\":10}", str)
		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Cursor\": \"30005af19b3800000000000000000000\",\"RecordTime\": 1525783352873,\"Sequence\": 1}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	gc, err := dh.GetCursor("test_project", "test_topic", "0", SEQUENCE, 10)
	assert.Nil(t, err)
	assert.NotNil(t, gc)
	assert.Equal(t, http.StatusOK, gc.StatusCode)
	assert.Equal(t, "request_id", gc.RequestId)
	assert.Equal(t, int64(1), gc.Sequence)
	assert.Equal(t, int64(1525783352873), gc.RecordTime)
	assert.Equal(t, "30005af19b3800000000000000000000", gc.Cursor)
}

func TestGetCursorWithInvalidParameter(t *testing.T) {
	dh := New("a", "a", "")

	gc, err := dh.GetCursor("test_project", "test_topic", "0", OLDEST, 10)
	assert.NotNil(t, err)
	assert.Nil(t, gc)

	gc, err = dh.GetCursor("test_project", "test_topic", "0", LATEST, 10)
	assert.NotNil(t, err)
	assert.Nil(t, gc)

	gc, err = dh.GetCursor("test_project", "test_topic", "0", SYSTEM_TIME)
	assert.NotNil(t, err)
	assert.Nil(t, gc)

	gc, err = dh.GetCursor("test_project", "test_topic", "0", SEQUENCE)
	assert.NotNil(t, err)
	assert.Nil(t, gc)

	gc, err = dh.GetCursor("test_project", "test_topic", "0", SEQUENCE, 1, 2)
	assert.NotNil(t, err)
	assert.Nil(t, gc)
}

func TestGetMeterInfo(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"meter\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ActiveTime\": 20,\"Storage\": 10}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	gm, err := dh.GetMeterInfo("test_project", "test_topic", "0")
	assert.Nil(t, err)
	assert.NotNil(t, gm)
	assert.Equal(t, http.StatusOK, gm.StatusCode)
	assert.Equal(t, "request_id", gm.RequestId)
	assert.Equal(t, int64(20), gm.ActiveTime)
	assert.Equal(t, int64(10), gm.Storage)
}

func TestListSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"TotalCount\": 3,\"Subscriptions\":[{\"Comment\":\"testsubscription\",\"CreateTime\":1525835229,\"IsOwner\":true,\"LastModifyTime\":1525835229,\"State\":1,\"SubId\":\"test_subId\",\"TopicName\":\"test_topic\",\"Type\":0}]}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ls, err := dh.ListSubscription("test_project", "test_topic", 1, 1)
	assert.Nil(t, err)
	assert.NotNil(t, ls)
	assert.Equal(t, http.StatusOK, ls.StatusCode)
	assert.Equal(t, "request_id", ls.RequestId)
	assert.Equal(t, int64(3), ls.TotalCount)
	assert.Equal(t, 1, len(ls.Subscriptions))
	assert.Equal(t, "test_subId", ls.Subscriptions[0].SubId)
	assert.Equal(t, "test_topic", ls.Subscriptions[0].TopicName)
	assert.Equal(t, true, ls.Subscriptions[0].IsOwner)
	assert.Equal(t, SUBTYPE_USER, ls.Subscriptions[0].Type)
	assert.Equal(t, SUB_ONLINE, ls.Subscriptions[0].State)
	assert.Equal(t, "testsubscription", ls.Subscriptions[0].Comment)
	assert.Equal(t, int64(1525835229), ls.Subscriptions[0].CreateTime)
	assert.Equal(t, int64(1525835229), ls.Subscriptions[0].LastModifyTime)
}

func TestCreateSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"create\",\"Comment\":\"test subscription\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("{\"SubId\": \"1525835229905vJHtz\"}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	cs, err := dh.CreateSubscription("test_project", "test_topic", "test subscription")
	assert.Nil(t, err)
	assert.NotNil(t, cs)
	assert.Equal(t, http.StatusCreated, cs.StatusCode)
	assert.Equal(t, "request_id", cs.RequestId)
	assert.Equal(t, "1525835229905vJHtz", cs.SubId)
}

func TestUpdateSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"update comment\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	cs, err := dh.UpdateSubscription("test_project", "test_topic", "test_subId", "update comment")
	assert.Nil(t, err)
	assert.NotNil(t, cs)
	assert.Equal(t, http.StatusOK, cs.StatusCode)
	assert.Equal(t, "request_id", cs.RequestId)
}

func TestDeleteSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.DELETE, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	cs, err := dh.DeleteSubscription("test_project", "test_topic", "test_subId")
	assert.Nil(t, err)
	assert.NotNil(t, cs)
	assert.Equal(t, http.StatusOK, cs.StatusCode)
	assert.Equal(t, "request_id", cs.RequestId)
}

func TestGetSubscription(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Comment\":\"testsubscription\",\"CreateTime\":1525835229,\"IsOwner\":true,\"LastModifyTime\":1525835229,\"State\":1,\"SubId\":\"test_subId\",\"TopicName\":\"test_topic\",\"Type\":0}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	gs, err := dh.GetSubscription("test_project", "test_topic", "test_subId")
	assert.Nil(t, err)
	assert.NotNil(t, gs)
	assert.Equal(t, http.StatusOK, gs.StatusCode)
	assert.Equal(t, "request_id", gs.RequestId)
	assert.Equal(t, "test_subId", gs.SubId)
	assert.Equal(t, "test_topic", gs.TopicName)
	assert.Equal(t, true, gs.IsOwner)
	assert.Equal(t, SUBTYPE_USER, gs.Type)
	assert.Equal(t, SUB_ONLINE, gs.State)
	assert.Equal(t, "testsubscription", gs.Comment)
	assert.Equal(t, int64(1525835229), gs.CreateTime)
	assert.Equal(t, int64(1525835229), gs.LastModifyTime)
}

func TestUpdateSubscriptionState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"State\":0}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	uss, err := dh.UpdateSubscriptionState("test_project", "test_topic", "test_subId", SUB_OFFLINE)
	assert.Nil(t, err)
	assert.NotNil(t, uss)
	assert.Equal(t, http.StatusOK, uss.StatusCode)
	assert.Equal(t, "request_id", uss.RequestId)
}

func TestOpenSubscriptionSession(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"open\",\"ShardIds\":[\"0\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Offsets\":{\"0\":{\"Sequence\":1,\"SessionId\":2,\"Timestamp\":3,\"Version\":4}}}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	shardIds := []string{"0"}
	oss, err := dh.OpenSubscriptionSession("test_project", "test_topic", "test_subId", shardIds)
	assert.Nil(t, err)
	assert.NotNil(t, oss)
	assert.Equal(t, http.StatusOK, oss.StatusCode)
	assert.Equal(t, "request_id", oss.RequestId)
	assert.Equal(t, int64(1), oss.Offsets["0"].Sequence)
	assert.Equal(t, int64(3), oss.Offsets["0"].Timestamp)
	assert.Equal(t, int64(2), *oss.Offsets["0"].SessionId)
	assert.Equal(t, int64(4), oss.Offsets["0"].VersionId)
}

func TestGetSubscriptionOffset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"get\",\"ShardIds\":[\"0\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Offsets\":{\"0\":{\"Sequence\":1,\"Timestamp\":2,\"Version\":3}}}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	shardIds := []string{"0"}
	oss, err := dh.GetSubscriptionOffset("test_project", "test_topic", "test_subId", shardIds)
	assert.Nil(t, err)
	assert.NotNil(t, oss)
	assert.Equal(t, http.StatusOK, oss.StatusCode)
	assert.Equal(t, "request_id", oss.RequestId)
	assert.Equal(t, int64(1), oss.Offsets["0"].Sequence)
	assert.Equal(t, int64(2), oss.Offsets["0"].Timestamp)
	assert.Nil(t, oss.Offsets["0"].SessionId)
	assert.Equal(t, int64(3), oss.Offsets["0"].VersionId)
}

func TestCommitSubscriptionOffset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"commit\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":1,\"Version\":0,\"SessionId\":1}}}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	sessionId := int64(1)
	offset := SubscriptionOffset{
		Timestamp: 100,
		Sequence:  1,
		SessionId: &sessionId,
	}
	offsetMap := map[string]SubscriptionOffset{"0": offset}
	css, err := dh.CommitSubscriptionOffset("test_project", "test_topic", "test_subId", offsetMap)
	assert.Nil(t, err)
	assert.NotNil(t, css)
	assert.Equal(t, http.StatusOK, css.StatusCode)
	assert.Equal(t, "request_id", css.RequestId)
}

func TestResetSubscriptionOffset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.PUT, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId/offsets", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"reset\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":200,\"Version\":0,\"SessionId\":null}}}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	offset := SubscriptionOffset{
		Timestamp: 100,
		Sequence:  200,
	}
	offsetMap := map[string]SubscriptionOffset{"0": offset}
	css, err := dh.ResetSubscriptionOffset("test_project", "test_topic", "test_subId", offsetMap)
	assert.Nil(t, err)
	assert.NotNil(t, css)
	assert.Equal(t, http.StatusOK, css.StatusCode)
	assert.Equal(t, "request_id", css.RequestId)
}

func TestListConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, url.Values(url.Values{"mode": []string{"id"}}), request.URL.Query())

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Connectors\": [\"sink_odps\", \"sink_oss\"]}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.ListConnector("test_project", "test_topic")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, []string([]string{"sink_odps", "sink_oss"}), ret.ConnectorIds)
}

func TestCreateOdpsConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_odps", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_odps\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"OdpsEndpoint\":\"OdpsEndpoint\",\"Project\":\"test_project\",\"Table\":\"test_table\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"TimeRange\":15,\"PartitionMode\":\"SYSTEM_TIME\",\"PartitionConfig\":{\"ds\":\"%Y%m%d\",\"hh\":\"%H\",\"mm\":\"%M\"},\"TunnelEndpoint\":\"TunnelEndpoint\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}

	odpsPartitionConfig := NewPartitionConfig()
	odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
	odpsPartitionConfig.AddConfig("hh", "%H")
	odpsPartitionConfig.AddConfig("mm", "%M")

	sinkOdpsConfig := &SinkOdpsConfig{
		Endpoint:        "OdpsEndpoint",
		TunnelEndpoint:  "TunnelEndpoint",
		Project:         "test_project",
		Table:           "test_table",
		AccessId:        "test_ak",
		AccessKey:       "test_sk",
		TimeRange:       15,
		PartitionMode:   SystemTimeMode,
		PartitionConfig: *odpsPartitionConfig,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkOdps, columnFields, *sinkOdpsConfig)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateOdpsConnectorWithSomePara(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_odps", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_odps\",\"SinkStartTime\":123456,\"ColumnFields\":[\"field1\",\"field2\"],\"ColumnNameMap\":{\"field1\":\"c1\",\"field2\":\"c2\"},\"Config\":{\"TimestampUnit\":\"SECOND\",\"OdpsEndpoint\":\"OdpsEndpoint\",\"Project\":\"test_project\",\"Table\":\"test_table\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"TimeRange\":15,\"PartitionMode\":\"EVENT_TIME\",\"PartitionConfig\":{\"ds\":\"%Y%m%d\",\"hh\":\"%H\",\"mm\":\"%M\"},\"TunnelEndpoint\":\"TunnelEndpoint\",\"SplitKey\":\"split_key\",\"Base64Encode\":true}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}

	columnNameMap := map[string]string{"field1": "c1", "field2": "c2"}

	odpsPartitionConfig := NewPartitionConfig()
	odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
	odpsPartitionConfig.AddConfig("hh", "%H")
	odpsPartitionConfig.AddConfig("mm", "%M")

	sinkOdpsConfig := &SinkOdpsConfig{
		Endpoint:        "OdpsEndpoint",
		TunnelEndpoint:  "TunnelEndpoint",
		Project:         "test_project",
		Table:           "test_table",
		AccessId:        "test_ak",
		AccessKey:       "test_sk",
		TimeRange:       15,
		PartitionMode:   EventTimeMode,
		PartitionConfig: *odpsPartitionConfig,
		SplitKey:        "split_key",
		Base64Encode:    true,
		ConnectorConfig: ConnectorConfig{
			TimestampUnit: ConnectorSecond,
		},
	}

	para := &CreateConnectorParameter{
		SinkStartTime: 123456,
		ConnectorType: SinkOdps,
		ColumnFields:  columnFields,
		ColumnNameMap: columnNameMap,
		Config:        *sinkOdpsConfig,
	}

	ret, err := dh.CreateConnectorWithPara("test_project", "test_topic", para)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateDataBaseConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_mysql", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_mysql\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Host\":\"test_host\",\"Port\":\"1234\",\"Database\":\"test_database\",\"Table\":\"test_table\",\"User\":\"test_user\",\"Password\":\"test_password\",\"Ignore\":\"true\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkMysqlConfig{
		Host:     "test_host",
		Port:     "1234",
		Database: "test_database",
		Table:    "test_table",
		User:     "test_user",
		Password: "test_password",
		Ignore:   IGNORE,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkMysql, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateFcConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_fc", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_fc\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Endpoint\":\"test_endpoint\",\"Service\":\"test_service\",\"Function\":\"test_function\",\"AuthMode\":\"sts\",\"InvokeType\":\"async\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkFcConfig{
		Endpoint:   "test_endpoint",
		Service:    "test_service",
		Function:   "test_function",
		AuthMode:   STS,
		InvokeType: FcAsync,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkFc, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateOssConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_oss", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_oss\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Endpoint\":\"test_endpoint\",\"Bucket\":\"test_bucket\",\"Prefix\":\"test_prefix\",\"TimeFormat\":\"%d%M\",\"TimeRange\":100,\"AuthMode\":\"sts\",\"MaxFileSize\":1024}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkOssConfig{
		Endpoint:    "test_endpoint",
		Bucket:      "test_bucket",
		Prefix:      "test_prefix",
		TimeFormat:  "%d%M",
		TimeRange:   100,
		AuthMode:    STS,
		MaxFileSize: 1024,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkOss, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateOtsConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_ots", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_ots\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Endpoint\":\"test_endpoint\",\"InstanceName\":\"test_instance\",\"TableName\":\"test_table\",\"AuthMode\":\"ak\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"WriteMode\":\"UPDATE\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkOtsConfig{
		Endpoint:     "test_endpoint",
		InstanceName: "test_instance",
		TableName:    "test_table",
		AuthMode:     AK,
		AccessId:     "test_ak",
		AccessKey:    "test_sk",
		WriteMode:    OtsUpdate,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkOts, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateEsConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_es", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_es\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Index\":\"test_index\",\"Endpoint\":\"test_endpoint\",\"User\":\"test_user\",\"Password\":\"test_password\",\"IDFields\":[\"id1\",\"id2\"],\"TypeFields\":[\"type1\",\"type2\"],\"RouterFields\":[\"router1\",\"router2\"],\"ProxyMode\":\"true\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkEsConfig{
		Index:        "test_index",
		Endpoint:     "test_endpoint",
		User:         "test_user",
		Password:     "test_password",
		IDFields:     []string{"id1", "id2"},
		TypeFields:   []string{"type1", "type2"},
		RouterFields: []string{"router1", "router2"},
		ProxyMode:    true,
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkEs, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateDataHubConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_datahub", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_datahub\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Endpoint\":\"test_endpoint\",\"Project\":\"test_project1\",\"Topic\":\"test_topic1\",\"AuthMode\":\"ak\",\"AccessId\":\"AccessId\",\"AccessKey\":\"AccessKey\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkDatahubConfig{
		Endpoint:  "test_endpoint",
		Project:   "test_project1",
		Topic:     "test_topic1",
		AuthMode:  AK,
		AccessId:  "AccessId",
		AccessKey: "AccessKey",
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkDatahub, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestCreateHologresConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/sink_hologres", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"create\",\"Type\":\"sink_hologres\",\"SinkStartTime\":-1,\"ColumnFields\":[\"field1\",\"field2\"],\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"Endpoint\":\"endpoint\",\"Project\":\"project\",\"Topic\":\"topic\",\"AuthMode\":\"ak\",\"AccessId\":\"accessId\",\"AccessKey\":\"accessKey\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	columnFields := []string{"field1", "field2"}
	conf := SinkHologresConfig{
		SinkDatahubConfig: SinkDatahubConfig{
			Endpoint:  "endpoint",
			Project:   "project",
			Topic:     "topic",
			AuthMode:  AK,
			AccessId:  "accessId",
			AccessKey: "accessKey",
		},
	}

	ret, err := dh.CreateConnector("test_project", "test_topic", SinkHologres, columnFields, conf)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
}

func TestGetOdpsConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"ClusterAddress\":\"address\",\"ColumnFields\":[\"char_test\",\"varchar_test\",\"num\"],\"ColumnNameMap\":{},\"Config\":{\"OdpsEndpoint\":\"endpoint\",\"PartitionConfig\":\"[{\\\"key\\\":\\\"ds\\\",\\\"value\\\":\\\"%Y%m%d\\\"},{\\\"key\\\":\\\"hh\\\",\\\"value\\\":\\\"%H\\\"},{\\\"key\\\":\\\"mm\\\",\\\"value\\\":\\\"%M\\\"}]\",\"PartitionField\":\"[\\\"ds:STRING\\\",\\\"hh:STRING\\\",\\\"mm:STRING\\\"]\",\"PartitionMode\":\"SYSTEM_TIME\",\"Project\":\"odps_project\",\"Table\":\"odps_table\",\"TableCreateTime\":\"1610095420\",\"TimeRange\":\"120\",\"TimeZone\":\"Asia/Shanghai\",\"TimestampUnit\":\"MICROSECOND\",\"TunnelEndpoint\":\"tunnel_endpoint\"},\"ConnectorId\":\"connector_id\",\"CreateTime\":1610095524,\"Creator\":\"1324\",\"ExtraInfo\":{\"SubscriptionId\":\"1610095525500LZHF8\"},\"LastModifyTime\":1610095524,\"Owner\":\"5678\",\"ShardContexts\":[{\"CurrentSequence\":3,\"CurrentTimestamp\":1610096918736,\"DiscardCount\":1,\"DoneTime\":1610092800,\"LastErrorMessage\":\"Access denied by project ip white list: sourceIP:'11.11.11.11' is not in white list. project: odps_test_project\",\"ShardId\":\"0\",\"State\":\"CONTEXT_HANG\",\"UpdateTime\":1614765291,\"WorkerAddress\":\"worker\"}],\"State\":\"CONNECTOR_RUNNING\",\"Type\":\"sink_odps\"}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnector("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(1610095524), ret.CreateTime)
	assert.Equal(t, int64(1610095524), ret.LastModifyTime)
	assert.Equal(t, "connector_id", ret.ConnectorId)
	assert.Equal(t, "address", ret.ClusterAddress)
	assert.Equal(t, SinkOdps, ret.Type)
	assert.Equal(t, ConnectorRunning, ret.State)
	assert.Equal(t, []string{"char_test", "varchar_test", "num"}, ret.ColumnFields)
	assert.Equal(t, map[string]string{"SubscriptionId": "1610095525500LZHF8"}, ret.ExtraConfig)
	assert.Equal(t, "1324", ret.Creator)
	assert.Equal(t, "5678", ret.Owner)

	conf, ok := ret.Config.(SinkOdpsConfig)
	assert.True(t, ok)
	assert.Equal(t, "endpoint", conf.Endpoint)
	assert.Equal(t, "odps_project", conf.Project)
	assert.Equal(t, "odps_table", conf.Table)
	assert.Equal(t, 120, conf.TimeRange)
	assert.Equal(t, "Asia/Shanghai", conf.TimeZone)
	assert.Equal(t, SystemTimeMode, conf.PartitionMode)
	assert.Equal(t, PartitionConfig(PartitionConfig{ConfigMap: []map[string]string{{"ds": "%Y%m%d"}, {"hh": "%H"}, {"mm": "%M"}}}), conf.PartitionConfig)
	assert.Equal(t, "tunnel_endpoint", conf.TunnelEndpoint)
	assert.Equal(t, "", conf.SplitKey)
	assert.Equal(t, false, conf.Base64Encode)
	assert.Equal(t, ConnectorMicrosecond, conf.TimestampUnit)
}

func TestGetFcConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"ClusterAddress\":\"address\",\"ColumnFields\":[\"c1\",\"c2\",\"c3\",\"c4\",\"c5\",\"c6\",\"c7\"],\"ColumnNameMap\":{},\"Config\":{\"AccessId\":\"access_id\",\"AccessKey\":\"access_key\",\"AuthMode\":\"ak\",\"Endpoint\":\"endpoint\",\"Function\":\"test_linus\",\"InvokeType\":\"async\",\"Service\":\"datahub_sink_fc\",\"TimestampUnit\":\"MICROSECOND\"},\"ConnectorId\":\"connector_id\",\"CreateTime\":1612170910,\"Creator\":\"1234\",\"ExtraInfo\":{\"SubscriptionId\":\"161217091056258PJB\"},\"LastModifyTime\":1612170910,\"Owner\":\"5678\",\"ShardContexts\":[{\"CurrentSequence\":42,\"CurrentTimestamp\":1614666568095,\"DiscardCount\":0,\"DoneTime\":0,\"LastErrorMessage\":\"\",\"ShardId\":\"0\",\"State\":\"CONTEXT_EXECUTING\",\"UpdateTime\":1614859881,\"WorkerAddress\":\"worker\"}],\"State\":\"CONNECTOR_RUNNING\",\"Type\":\"sink_fc\"}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnector("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(1612170910), ret.CreateTime)
	assert.Equal(t, int64(1612170910), ret.LastModifyTime)
	assert.Equal(t, "connector_id", ret.ConnectorId)
	assert.Equal(t, "address", ret.ClusterAddress)
	assert.Equal(t, SinkFc, ret.Type)
	assert.Equal(t, ConnectorRunning, ret.State)
	assert.Equal(t, []string([]string{"c1", "c2", "c3", "c4", "c5", "c6", "c7"}), ret.ColumnFields)
	assert.Equal(t, map[string]string{"SubscriptionId": "161217091056258PJB"}, ret.ExtraConfig)
	assert.Equal(t, "1234", ret.Creator)
	assert.Equal(t, "5678", ret.Owner)

	conf, ok := ret.Config.(SinkFcConfig)
	assert.True(t, ok)
	assert.Equal(t, "endpoint", conf.Endpoint)
	assert.Equal(t, "datahub_sink_fc", conf.Service)
	assert.Equal(t, "test_linus", conf.Function)
	assert.Equal(t, AK, conf.AuthMode)
	assert.Equal(t, FcAsync, conf.InvokeType)
	assert.Equal(t, ConnectorMicrosecond, conf.TimestampUnit)
}

func TestGetEsConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/test_connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"ColumnFields\":[\"c1\",\"c2\"],\"ColumnNameMap\":{},\"Config\":{\"Endpoint\":\"endpoint:9200\",\"IDFields\":\"[\\\"c1\\\"]\",\"Index\":\"my_index\",\"ProxyMode\":\"true\",\"TimeZone\":\"Asia/Shanghai\",\"TimestampUnit\":\"MICROSECOND\",\"TypeFields\":\"[]\",\"Version\":\"7.4.0\"},\"ConnectorId\":\"test_connector_id\",\"CreateTime\":1614839000,\"Creator\":\"1234\",\"ExtraInfo\":{\"SubscriptionId\":\"16148390006786VM28\"},\"LastModifyTime\":1614839000,\"Owner\":\"5678\",\"ShardContexts\":[{\"CurrentSequence\":-1,\"CurrentTimestamp\":-1,\"DiscardCount\":0,\"DoneTime\":0,\"LastErrorMessage\":\"\",\"ShardId\":\"0\",\"State\":\"CONTEXT_EXECUTING\",\"UpdateTime\":1614849194,\"WorkerAddress\":\"worker\"}],\"State\":\"CONNECTOR_RUNNING\",\"Type\":\"sink_es\"}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnector("test_project", "test_topic", "test_connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(1614839000), ret.CreateTime)
	assert.Equal(t, int64(1614839000), ret.LastModifyTime)
	assert.Equal(t, "test_connector_id", ret.ConnectorId)
	assert.Equal(t, "", ret.ClusterAddress)
	assert.Equal(t, SinkEs, ret.Type)
	assert.Equal(t, ConnectorRunning, ret.State)
	assert.Equal(t, 2, len(ret.ColumnFields))
	assert.Equal(t, "c1", ret.ColumnFields[0])
	assert.Equal(t, "c2", ret.ColumnFields[1])
	assert.Equal(t, map[string]string(map[string]string{"SubscriptionId": "16148390006786VM28"}), ret.ExtraConfig)
	assert.Equal(t, "1234", ret.Creator)
	assert.Equal(t, "5678", ret.Owner)

	conf, ok := ret.Config.(SinkEsConfig)
	assert.True(t, ok)
	assert.Equal(t, "my_index", conf.Index)
	assert.Equal(t, "endpoint:9200", conf.Endpoint)
	assert.Equal(t, []string([]string{"c1"}), conf.IDFields)
	assert.Equal(t, []string([]string{}), conf.TypeFields)
	assert.Equal(t, []string{}, conf.RouterFields)
	assert.Equal(t, true, conf.ProxyMode)
	assert.Equal(t, ConnectorMicrosecond, conf.TimestampUnit)
}

func TestUpdateConnectorConfig(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"updateconfig\",\"Config\":{\"TimestampUnit\":\"MICROSECOND\",\"OdpsEndpoint\":\"OdpsEndpoint\",\"Project\":\"test_project\",\"Table\":\"test_table\",\"AccessId\":\"test_ak\",\"AccessKey\":\"test_sk\",\"TimeRange\":15,\"PartitionMode\":\"SYSTEM_TIME\",\"PartitionConfig\":{\"ds\":\"%Y%m%d\",\"hh\":\"%H\",\"mm\":\"%M\"},\"TunnelEndpoint\":\"TunnelEndpoint\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	odpsPartitionConfig := NewPartitionConfig()
	odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
	odpsPartitionConfig.AddConfig("hh", "%H")
	odpsPartitionConfig.AddConfig("mm", "%M")
	sinkOdpsConfig := SinkOdpsConfig{
		Endpoint:        "OdpsEndpoint",
		TunnelEndpoint:  "TunnelEndpoint",
		Project:         "test_project",
		Table:           "test_table",
		AccessId:        "test_ak",
		AccessKey:       "test_sk",
		TimeRange:       15,
		PartitionMode:   SystemTimeMode,
		PartitionConfig: *odpsPartitionConfig,
	}

	ret, err := dh.UpdateConnector("test_project", "test_topic", "connector_id", sinkOdpsConfig)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestUpdateConnectorColumnFields(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"updateconfig\",\"ColumnFields\":[\"f1\",\"f2\"]}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	para := &UpdateConnectorParameter{
		ColumnFields:  []string{"f1", "f2"},
		ColumnNameMap: nil,
		Config:        nil,
	}

	ret, err := dh.UpdateConnectorWithPara("test_project", "test_topic", "connector_id", para)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestUpdateConnectorColumnMap(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		expectedStr := "{\"Action\":\"updateconfig\",\"ColumnNameMap\":{\"c1\":\"f1\",\"c2\":\"f2\"}}"
		assert.Equal(t, expectedStr, str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConnectorId\": \"test_connector_id\"}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	para := &UpdateConnectorParameter{
		ColumnFields:  nil,
		ColumnNameMap: map[string]string{"c1": "f1", "c2": "f2"},
		Config:        nil,
	}

	ret, err := dh.UpdateConnectorWithPara("test_project", "test_topic", "connector_id", para)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestDeleteConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.DELETE, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.DeleteConnector("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestGetConnectorDoneTime(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.GET, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
		assert.Equal(t, url.Values(url.Values{"donetime": []string{""}}), request.URL.Query())

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"DoneTime\":1610092800,\"TimeWindow\":7200,\"TimeZone\":\"Asia/Shanghai\"}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnectorDoneTime("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(1610092800), ret.DoneTime)
	assert.Equal(t, "Asia/Shanghai", ret.TimeZone)
	assert.Equal(t, 7200, ret.TimeWindow)
}

func TestUpdateReloadConnector(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"Reload\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.ReloadConnector("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestUpdateReloadConnectorByShard(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"Reload\",\"ShardId\":\"0\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.ReloadConnectorByShard("test_project", "test_topic", "connector_id", "0")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestGetConnectorShardStatus(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"Status\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"ShardStatusInfos\":{\"0\":{\"CurrentSequence\":3,\"CurrentTimestamp\":1610096918736,\"DiscardCount\":10,\"DoneTime\":1610092800,\"LastErrorMessage\":\"\",\"ShardId\":\"0\",\"State\":\"CONTEXT_EXECUTING\",\"UpdateTime\":1614765291,\"WorkerAddress\":\"worker1\"},\"1\":{\"CurrentSequence\":30,\"CurrentTimestamp\":1610096918736,\"DiscardCount\":1,\"DoneTime\":1610092800,\"LastErrorMessage\":\"\",\"ShardId\":\"0\",\"State\":\"CONTEXT_HANG\",\"UpdateTime\":1614765291,\"WorkerAddress\":\"worker2\"}}}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnectorShardStatus("test_project", "test_topic", "connector_id")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, 2, len(ret.ShardStatus))
	assert.Equal(t, int64(0), ret.ShardStatus["0"].StartSequence)
	assert.Equal(t, int64(0), ret.ShardStatus["0"].EndSequence)
	assert.Equal(t, int64(3), ret.ShardStatus["0"].CurrentSequence)
	assert.Equal(t, int64(1610096918736), ret.ShardStatus["0"].CurrentTimestamp)
	assert.Equal(t, int64(1614765291), ret.ShardStatus["0"].UpdateTime)
	assert.Equal(t, ConnectorShardExecuting, ret.ShardStatus["0"].State)
	assert.Equal(t, "", ret.ShardStatus["0"].LastErrorMessage)
	assert.Equal(t, int64(10), ret.ShardStatus["0"].DiscardCount)
	assert.Equal(t, int64(1610092800), ret.ShardStatus["0"].DoneTime)
	assert.Equal(t, "worker1", ret.ShardStatus["0"].WorkerAddress)

	assert.Equal(t, int64(0), ret.ShardStatus["1"].StartSequence)
	assert.Equal(t, int64(0), ret.ShardStatus["1"].EndSequence)
	assert.Equal(t, int64(30), ret.ShardStatus["1"].CurrentSequence)
	assert.Equal(t, int64(1610096918736), ret.ShardStatus["1"].CurrentTimestamp)
	assert.Equal(t, int64(1614765291), ret.ShardStatus["1"].UpdateTime)
	assert.Equal(t, ConnectorShardHang, ret.ShardStatus["1"].State)
	assert.Equal(t, "", ret.ShardStatus["1"].LastErrorMessage)
	assert.Equal(t, int64(1), ret.ShardStatus["1"].DiscardCount)
	assert.Equal(t, int64(1610092800), ret.ShardStatus["1"].DoneTime)
	assert.Equal(t, "worker2", ret.ShardStatus["1"].WorkerAddress)
}

func TestGetConnectorShardStatusByShard(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"Status\",\"ShardId\":\"0\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		respBody := "{\"CurrentSequence\":3,\"CurrentTimestamp\":1610096918736,\"DiscardCount\":1,\"DoneTime\":1610092800,\"LastErrorMessage\":\"error\",\"ShardId\":\"0\",\"State\":\"CONTEXT_HANG\",\"UpdateTime\":1614765291,\"WorkerAddress\":\"worker1\"}"
		_, _ = writer.Write([]byte(respBody))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.GetConnectorShardStatusByShard("test_project", "test_topic", "connector_id", "0")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(0), ret.StartSequence)
	assert.Equal(t, int64(0), ret.EndSequence)
	assert.Equal(t, int64(3), ret.CurrentSequence)
	assert.Equal(t, int64(1610096918736), ret.CurrentTimestamp)
	assert.Equal(t, int64(1614765291), ret.UpdateTime)
	assert.Equal(t, ConnectorShardHang, ret.State)
	assert.Equal(t, "error", ret.LastErrorMessage)
	assert.Equal(t, int64(1), ret.DiscardCount)
	assert.Equal(t, int64(1610092800), ret.DoneTime)
	assert.Equal(t, "worker1", ret.WorkerAddress)
}

func TestAppendConnectorField(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"appendfield\",\"FieldName\":\"field3\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.AppendConnectorField("test_project", "test_topic", "connector_id", "field3")
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestUpdateConnectorState(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"updatestate\",\"State\":\"CONNECTOR_STOPPED\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.UpdateConnectorState("test_project", "test_topic", "connector_id", ConnectorStopped)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestUpdateConnectorOffset(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"updateshardcontext\",\"ShardId\":\"0\",\"CurrentTime\":100,\"CurrentSequence\":10}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	offset := ConnectorOffset{
		Timestamp: 100,
		Sequence:  10,
	}

	ret, err := dh.UpdateConnectorOffset("test_project", "test_topic", "connector_id", "0", offset)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestJoinGroup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"joinGroup\",\"SessionTimeout\":60000}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ConsumerId\": \"test_sub_id-1\",\"VersionId\":1,\"SessionTimeout\": 60000}"))
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	jg, err := dh.JoinGroup("test_project", "test_topic", "test_subId", 60000)
	assert.Nil(t, err)
	assert.NotNil(t, jg)
	assert.Equal(t, http.StatusOK, jg.StatusCode)
	assert.Equal(t, "request_id", jg.RequestId)
	assert.Equal(t, "test_sub_id-1", jg.ConsumerId)
	assert.Equal(t, int64(1), jg.VersionId)
	assert.Equal(t, int64(60000), jg.SessionTimeout)
}

func TestHeartbeat(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"heartbeat\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"HoldShardList\":[\"0\",\"1\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"ShardList\": [\"0\", \"1\"], \"TotalPlan\": \"xxx\", \"PlanVersion\": 1}"))
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	holdShardIds := []string{"0", "1"}
	ret, err := dh.Heartbeat("test_project", "test_topic", "test_subId", "test_consumer_id", 1, holdShardIds, nil)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
	assert.Equal(t, int64(1), ret.PlanVersion)
	assert.Equal(t, "0", ret.ShardList[0])
	assert.Equal(t, "1", ret.ShardList[1])
	assert.Equal(t, "xxx", ret.TotalPlan)
}

func TestSyncGroup(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"syncGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"ReleaseShardList\":[\"0\",\"1\"],\"ReadEndShardList\":[\"2\",\"3\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	cfg := NewDefaultConfig()
	cfg.CompressorType = NOCOMPRESS
	dh := NewClientWithConfig(ts.URL, cfg, NewAliyunAccount("a", "a"))

	releaseShardList := []string{"0", "1"}
	readEndShardList := []string{"2", "3"}
	ret, err := dh.SyncGroup("test_project", "test_topic", "test_subId", "test_consumer_id", 1, releaseShardList, readEndShardList)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}

func TestLeaveGroups(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		assert.Equal(t, requests.POST, request.Method)
		assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
		assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

		defer request.Body.Close()
		body, err := io.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"leaveGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := New("a", "a", ts.URL)

	ret, err := dh.LeaveGroup("test_project", "test_topic", "test_subId", "test_consumer_id", 1)
	assert.Nil(t, err)
	assert.NotNil(t, ret)
	assert.Equal(t, http.StatusOK, ret.StatusCode)
	assert.Equal(t, "request_id", ret.RequestId)
}
