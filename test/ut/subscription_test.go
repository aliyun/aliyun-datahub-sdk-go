package ut

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/stretchr/testify/assert"
)

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
	dh := datahub.New("a", "a", ts.URL)

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
	assert.Equal(t, datahub.SUBTYPE_USER, ls.Subscriptions[0].Type)
	assert.Equal(t, datahub.SUB_ONLINE, ls.Subscriptions[0].State)
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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"create\",\"Comment\":\"test subscription\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusCreated)
		_, _ = writer.Write([]byte("{\"SubId\": \"1525835229905vJHtz\"}"))
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Comment\":\"update comment\"}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

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
	dh := datahub.New("a", "a", ts.URL)

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
	dh := datahub.New("a", "a", ts.URL)

	gs, err := dh.GetSubscription("test_project", "test_topic", "test_subId")
	assert.Nil(t, err)
	assert.NotNil(t, gs)
	assert.Equal(t, http.StatusOK, gs.StatusCode)
	assert.Equal(t, "request_id", gs.RequestId)
	assert.Equal(t, "test_subId", gs.SubId)
	assert.Equal(t, "test_topic", gs.TopicName)
	assert.Equal(t, true, gs.IsOwner)
	assert.Equal(t, datahub.SUBTYPE_USER, gs.Type)
	assert.Equal(t, datahub.SUB_ONLINE, gs.State)
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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"State\":0}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

	uss, err := dh.UpdateSubscriptionState("test_project", "test_topic", "test_subId", datahub.SUB_OFFLINE)
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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"open\",\"ShardIds\":[\"0\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Offsets\":{\"0\":{\"Sequence\":1,\"SessionId\":2,\"Timestamp\":3,\"Version\":4}}}"))
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"get\",\"ShardIds\":[\"0\"]}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte("{\"Offsets\":{\"0\":{\"Sequence\":1,\"Timestamp\":2,\"Version\":3}}}"))
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"commit\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":1,\"Version\":0,\"SessionId\":1}}}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

	sessionId := int64(1)
	offset := datahub.SubscriptionOffset{
		Timestamp: 100,
		Sequence:  1,
		SessionId: &sessionId,
	}
	offsetMap := map[string]datahub.SubscriptionOffset{"0": offset}
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
		body, err := ioutil.ReadAll(request.Body)
		assert.Nil(t, err)
		assert.NotNil(t, body)
		str := string(body)
		assert.Equal(t, "{\"Action\":\"reset\",\"Offsets\":{\"0\":{\"Timestamp\":100,\"Sequence\":200,\"Version\":0,\"SessionId\":null}}}", str)

		writer.Header().Set("x-datahub-request-id", "request_id")
		writer.WriteHeader(http.StatusOK)
	}))

	defer ts.Close()
	dh := datahub.New("a", "a", ts.URL)

	offset := datahub.SubscriptionOffset{
		Timestamp: 100,
		Sequence:  200,
	}
	offsetMap := map[string]datahub.SubscriptionOffset{"0": offset}
	css, err := dh.ResetSubscriptionOffset("test_project", "test_topic", "test_subId", offsetMap)
	assert.Nil(t, err)
	assert.NotNil(t, css)
	assert.Equal(t, http.StatusOK, css.StatusCode)
	assert.Equal(t, "request_id", css.RequestId)
}
