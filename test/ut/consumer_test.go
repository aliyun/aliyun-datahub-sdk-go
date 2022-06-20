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

func TestJoinGroup(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/subscriptions/test_subId", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"joinGroup\",\"SessionTimeout\":60000}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"ConsumerId\": \"test_sub_id-1\",\"VersionId\":1,\"SessionTimeout\": 60000}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"heartbeat\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"HoldShardList\":[\"0\",\"1\"]}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"ShardList\": [\"0\", \"1\"], \"TotalPlan\": \"xxx\", \"PlanVersion\": 1}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"syncGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1,\"ReleaseShardList\":[\"0\",\"1\"],\"ReadEndShardList\":[\"2\",\"3\"]}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"leaveGroup\",\"ConsumerId\":\"test_consumer_id\",\"VersionId\":1}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.LeaveGroup("test_project", "test_topic", "test_subId", "test_consumer_id", 1)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
}
