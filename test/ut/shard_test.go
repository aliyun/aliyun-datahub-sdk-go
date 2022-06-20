package ut

import (
    "../../datahub"
    "fmt"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/stretchr/testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "testing"
)

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
   dh := datahub.New("a", "a", ts.URL)

   ls, err := dh.ListShard("test_project", "test_topic")
   assert.Nil(t, err)
   assert.NotNil(t, ls)
   assert.Equal(t, http.StatusOK, ls.StatusCode)
   assert.Equal(t, "request_id", ls.RequestId)
   assert.Equal(t, 2, len(ls.Shards))
   assert.Equal(t, "http1.1", ls.Protocol)
   assert.Equal(t, int64(500), ls.IntervalMs)
   assert.Equal(t, "0", ls.Shards[0].ShardId)
   assert.Equal(t, datahub.ACTIVE, ls.Shards[0].State)
   assert.Equal(t, "00000000000000000000000000000000", ls.Shards[0].BeginHashKey)
   assert.Equal(t, "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF", ls.Shards[0].EndHashKey)
   assert.Equal(t, int64(100), ls.Shards[0].ClosedTime)
   assert.Equal(t, 0, len(ls.Shards[0].ParentShardIds))
   assert.Equal(t, "0", ls.Shards[0].LeftShardId)
   assert.Equal(t, "4294967295", ls.Shards[0].RightShardId)
   assert.Equal(t, "", ls.Shards[0].Address)

   assert.Equal(t, "1", ls.Shards[1].ShardId)
   assert.Equal(t, datahub.CLOSED, ls.Shards[1].State)
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
   dh := datahub.New("a", "a", ts.URL)

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
   dh := datahub.New("a", "a", ts.URL)

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
            body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

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
    dh := datahub.New("a", "a", ts.URL)

    ss, err := dh.SplitShard("test_project", "test_topic", "1")
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ss)
}

func TestSplitShardWithInvalidShardIdFormat(t *testing.T) {
    dh := datahub.New("a", "a", "a")

    ss, err := dh.SplitShard("test_project", "test_topic", "aa")
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, ss)
}

func TestMergeShard(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"extend\",\"ExtendMode\":\"TO\",\"ShardNumber\":3}", str)
        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"cursor\",\"Type\":\"OLDEST\"}", str)
        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"Cursor\": \"30005af19b3800000000000000000000\",\"RecordTime\": 1525783352873,\"Sequence\": 1}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gc, err := dh.GetCursor("test_project", "test_topic", "0", datahub.OLDEST)
    assert.Nil(t, err)
    assert.NotNil(t, gc)
    assert.Equal(t, http.StatusOK, gc.StatusCode)
    assert.Equal(t, "request_id", gc.RequestId)
    assert.Equal(t, int64(1), gc.Sequence)
    assert.Equal(t, int64(1525783352873) , gc.RecordTime)
    assert.Equal(t, "30005af19b3800000000000000000000", gc.Cursor)
}

func TestGetCursor2(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"cursor\",\"Type\":\"SEQUENCE\",\"Sequence\":10}", str)
        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"Cursor\": \"30005af19b3800000000000000000000\",\"RecordTime\": 1525783352873,\"Sequence\": 1}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gc, err := dh.GetCursor("test_project", "test_topic", "0", datahub.SEQUENCE, 10)
    assert.Nil(t, err)
    assert.NotNil(t, gc)
    assert.Equal(t, http.StatusOK, gc.StatusCode)
    assert.Equal(t, "request_id", gc.RequestId)
    assert.Equal(t, int64(1), gc.Sequence)
    assert.Equal(t, int64(1525783352873) , gc.RecordTime)
    assert.Equal(t, "30005af19b3800000000000000000000", gc.Cursor)
}

func TestGetCursorWithInvalidParameter(t *testing.T) {
    dh := datahub.New("a", "a", "")

    gc, err := dh.GetCursor("test_project", "test_topic", "0", datahub.OLDEST, 10)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gc)

    gc, err = dh.GetCursor("test_project", "test_topic", "0", datahub.LATEST, 10)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gc)

    gc, err = dh.GetCursor("test_project", "test_topic", "0", datahub.SYSTEM_TIME)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gc)

    gc, err = dh.GetCursor("test_project", "test_topic", "0", datahub.SEQUENCE)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gc)

    gc, err = dh.GetCursor("test_project", "test_topic", "0", datahub.SEQUENCE, 1, 2)
    assert.NotNil(t, err)
    fmt.Println(err)
    assert.Nil(t, gc)
}

func TestGetMeterInfo(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/shards/0", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"meter\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"ActiveTime\": 20,\"Storage\": 10}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    gm, err := dh.GetMeterInfo("test_project", "test_topic", "0")
    assert.Nil(t, err)
    assert.NotNil(t, gm)
    assert.Equal(t, http.StatusOK, gm.StatusCode)
    assert.Equal(t, "request_id", gm.RequestId)
    assert.Equal(t, int64(20), gm.ActiveTime)
    assert.Equal(t, int64(10) , gm.Storage)
}
