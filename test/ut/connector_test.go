package ut

import (
    "../../datahub"
    "github.com/aliyun/alibaba-cloud-sdk-go/sdk/requests"
    "github.com/stretchr/Testify/assert"
    "io/ioutil"
    "net/http"
    "net/http/httptest"
    "net/url"
    "testing"
)

func TestListConnector(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.GET, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/connectors", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))
        assert.Equal(t, url.Values(url.Values{"mode":[]string{"id"}}), request.URL.Query())

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
        _, _ = writer.Write([]byte("{\"Connectors\": [\"sink_odps\", \"sink_oss\"]}"))
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}

    odpsPartitionConfig := datahub.NewPartitionConfig()
    odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
    odpsPartitionConfig.AddConfig("hh", "%H")
    odpsPartitionConfig.AddConfig("mm", "%M")

    sinkOdpsConfig := &datahub.SinkOdpsConfig{
        Endpoint:        "OdpsEndpoint",
        TunnelEndpoint:  "TunnelEndpoint",
        Project:         "test_project",
        Table:           "test_table",
        AccessId:        "test_ak",
        AccessKey:       "test_sk",
        TimeRange:       15,
        PartitionMode:   datahub.SystemTimeMode,
        PartitionConfig: *odpsPartitionConfig,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkOdps, columnFields, *sinkOdpsConfig)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}

    columnNameMap := map[string]string{"field1": "c1", "field2": "c2"}

    odpsPartitionConfig := datahub.NewPartitionConfig()
    odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
    odpsPartitionConfig.AddConfig("hh", "%H")
    odpsPartitionConfig.AddConfig("mm", "%M")

    sinkOdpsConfig := &datahub.SinkOdpsConfig{
        Endpoint:        "OdpsEndpoint",
        TunnelEndpoint:  "TunnelEndpoint",
        Project:         "test_project",
        Table:           "test_table",
        AccessId:        "test_ak",
        AccessKey:       "test_sk",
        TimeRange:       15,
        PartitionMode:   datahub.EventTimeMode,
        PartitionConfig: *odpsPartitionConfig,
        SplitKey:        "split_key",
        Base64Encode:    true,
        ConnectorConfig: datahub.ConnectorConfig{
            TimestampUnit: datahub.ConnectorSecond,
        },
    }

    para := &datahub.CreateConnectorParameter{
        SinkStartTime: 123456,
        ConnectorType: datahub.SinkOdps,
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkMysqlConfig{
        Host:     "test_host",
        Port:     "1234",
        Database: "test_database",
        Table:    "test_table",
        User:     "test_user",
        Password: "test_password",
        Ignore:   datahub.IGNORE,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkMysql, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkFcConfig{
        Endpoint:   "test_endpoint",
        Service:    "test_service",
        Function:   "test_function",
        AuthMode:   datahub.STS,
        InvokeType: datahub.FcAsync,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkFc, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkOssConfig{
        Endpoint:    "test_endpoint",
        Bucket:      "test_bucket",
        Prefix:      "test_prefix",
        TimeFormat:  "%d%M",
        TimeRange:   100,
        AuthMode:    datahub.STS,
        MaxFileSize: 1024,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkOss, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkOtsConfig{
        Endpoint:     "test_endpoint",
        InstanceName: "test_instance",
        TableName:    "test_table",
        AuthMode:     datahub.AK,
        AccessId:     "test_ak",
        AccessKey:    "test_sk",
        WriteMode:    datahub.OtsUpdate,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkOts, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkEsConfig{
        Index:        "test_index",
        Endpoint:     "test_endpoint",
        User:         "test_user",
        Password:     "test_password",
        IDFields:     []string{"id1", "id2"},
        TypeFields:   []string{"type1", "type2"},
        RouterFields: []string{"router1", "router2"},
        ProxyMode:    true,
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkEs, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkDatahubConfig{
        Endpoint:  "test_endpoint",
        Project:   "test_project1",
        Topic:     "test_topic1",
        AuthMode:  datahub.AK,
        AccessId:  "AccessId",
        AccessKey: "AccessKey",
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkDatahub, columnFields, conf)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    columnFields := []string{"field1", "field2"}
    conf := datahub.SinkHologresConfig{
        SinkDatahubConfig: datahub.SinkDatahubConfig{
            Endpoint:  "endpoint",
            Project:   "project",
            Topic:     "topic",
            AuthMode:  datahub.AK,
            AccessId:  "accessId",
            AccessKey: "accessKey",
        },
    }

    ret, err := dh.CreateConnector("test_project", "test_topic", datahub.SinkHologres, columnFields, conf)
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
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.GetConnector("test_project", "test_topic", "connector_id")
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, int64(1610095524), ret.CreateTime)
    assert.Equal(t, int64(1610095524), ret.LastModifyTime)
    assert.Equal(t, "connector_id", ret.ConnectorId)
    assert.Equal(t, "address", ret.ClusterAddress)
    assert.Equal(t, datahub.SinkOdps, ret.Type)
    assert.Equal(t, datahub.ConnectorRunning, ret.State)
    assert.Equal(t, []string{"char_test", "varchar_test", "num"}, ret.ColumnFields)
    assert.Equal(t, map[string]string{"SubscriptionId": "1610095525500LZHF8"}, ret.ExtraConfig)
    assert.Equal(t, "1324", ret.Creator)
    assert.Equal(t, "5678", ret.Owner)

    conf, ok := ret.Config.(datahub.SinkOdpsConfig)
    assert.True(t, ok)
    assert.Equal(t, "endpoint", conf.Endpoint)
    assert.Equal(t, "odps_project", conf.Project)
    assert.Equal(t, "odps_table", conf.Table)
    assert.Equal(t, 120, conf.TimeRange)
    assert.Equal(t, "Asia/Shanghai", conf.TimeZone)
    assert.Equal(t, datahub.SystemTimeMode, conf.PartitionMode)
    assert.Equal(t, datahub.PartitionConfig(datahub.PartitionConfig{ConfigMap: []map[string]string{{"ds": "%Y%m%d"}, {"hh": "%H"}, {"mm": "%M"}}}), conf.PartitionConfig)
    assert.Equal(t, "tunnel_endpoint", conf.TunnelEndpoint)
    assert.Equal(t, "", conf.SplitKey)
    assert.Equal(t, false, conf.Base64Encode)
    assert.Equal(t, datahub.ConnectorMicrosecond, conf.TimestampUnit)
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
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.GetConnector("test_project", "test_topic", "connector_id")
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, int64(1612170910), ret.CreateTime)
    assert.Equal(t, int64(1612170910), ret.LastModifyTime)
    assert.Equal(t, "connector_id", ret.ConnectorId)
    assert.Equal(t, "address", ret.ClusterAddress)
    assert.Equal(t, datahub.SinkFc, ret.Type)
    assert.Equal(t, datahub.ConnectorRunning, ret.State)
    assert.Equal(t, []string([]string{"c1", "c2", "c3", "c4", "c5", "c6", "c7"}), ret.ColumnFields)
    assert.Equal(t, map[string]string{"SubscriptionId": "161217091056258PJB"}, ret.ExtraConfig)
    assert.Equal(t, "1234", ret.Creator)
    assert.Equal(t, "5678", ret.Owner)

    conf, ok := ret.Config.(datahub.SinkFcConfig)
    assert.True(t, ok)
    assert.Equal(t, "endpoint", conf.Endpoint)
    assert.Equal(t, "datahub_sink_fc", conf.Service)
    assert.Equal(t, "test_linus", conf.Function)
    assert.Equal(t, datahub.AK, conf.AuthMode)
    assert.Equal(t, datahub.FcAsync, conf.InvokeType)
    assert.Equal(t, datahub.ConnectorMicrosecond, conf.TimestampUnit)
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
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.GetConnector("test_project", "test_topic", "test_connector_id")
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
    assert.Equal(t, int64(1614839000), ret.CreateTime)
    assert.Equal(t, int64(1614839000), ret.LastModifyTime)
    assert.Equal(t, "test_connector_id", ret.ConnectorId)
    assert.Equal(t, "", ret.ClusterAddress)
    assert.Equal(t, datahub.SinkEs, ret.Type)
    assert.Equal(t, datahub.ConnectorRunning, ret.State)
    assert.Equal(t, 2, len(ret.ColumnFields))
    assert.Equal(t, "c1", ret.ColumnFields[0])
    assert.Equal(t, "c2", ret.ColumnFields[1])
    assert.Equal(t, map[string]string(map[string]string{"SubscriptionId": "16148390006786VM28"}), ret.ExtraConfig)
    assert.Equal(t, "1234", ret.Creator)
    assert.Equal(t, "5678", ret.Owner)

    conf, ok := ret.Config.(datahub.SinkEsConfig)
    assert.True(t, ok)
    assert.Equal(t, "my_index", conf.Index)
    assert.Equal(t, "endpoint:9200", conf.Endpoint)
    assert.Equal(t, []string([]string{"c1"}), conf.IDFields)
    assert.Equal(t, []string([]string{}), conf.TypeFields)
    assert.Equal(t, []string{}, conf.RouterFields)
    assert.Equal(t, true, conf.ProxyMode)
    assert.Equal(t, datahub.ConnectorMicrosecond, conf.TimestampUnit)
}

func TestUpdateConnectorConfig(t *testing.T) {
    ts := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
        assert.Equal(t, requests.POST, request.Method)
        assert.Equal(t, "/projects/test_project/topics/test_topic/connectors/connector_id", request.URL.EscapedPath())
        assert.Equal(t, "application/json", request.Header.Get("Content-Type"))

        defer request.Body.Close()
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    odpsPartitionConfig := datahub.NewPartitionConfig()
    odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
    odpsPartitionConfig.AddConfig("hh", "%H")
    odpsPartitionConfig.AddConfig("mm", "%M")
    sinkOdpsConfig := datahub.SinkOdpsConfig{
        Endpoint:        "OdpsEndpoint",
        TunnelEndpoint:  "TunnelEndpoint",
        Project:         "test_project",
        Table:           "test_table",
        AccessId:        "test_ak",
        AccessKey:       "test_sk",
        TimeRange:       15,
        PartitionMode:   datahub.SystemTimeMode,
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    para := &datahub.UpdateConnectorParameter{
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

    para := &datahub.UpdateConnectorParameter{
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
    dh := datahub.New("a", "a", ts.URL)

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
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"Reload\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"Reload\",\"ShardId\":\"0\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

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
    assert.Equal(t, datahub.ConnectorShardExecuting, ret.ShardStatus["0"].State)
    assert.Equal(t, "", ret.ShardStatus["0"].LastErrorMessage)
    assert.Equal(t, int64(10), ret.ShardStatus["0"].DiscardCount)
    assert.Equal(t, int64(1610092800), ret.ShardStatus["0"].DoneTime)
    assert.Equal(t, "worker1", ret.ShardStatus["0"].WorkerAddress)

    assert.Equal(t, int64(0), ret.ShardStatus["1"].StartSequence)
    assert.Equal(t, int64(0), ret.ShardStatus["1"].EndSequence)
    assert.Equal(t, int64(30), ret.ShardStatus["1"].CurrentSequence)
    assert.Equal(t, int64(1610096918736), ret.ShardStatus["1"].CurrentTimestamp)
    assert.Equal(t, int64(1614765291), ret.ShardStatus["1"].UpdateTime)
    assert.Equal(t, datahub.ConnectorShardHang, ret.ShardStatus["1"].State)
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
        body, err := ioutil.ReadAll(request.Body)
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
    dh := datahub.New("a", "a", ts.URL)

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
    assert.Equal(t, datahub.ConnectorShardHang, ret.State)
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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"appendfield\",\"FieldName\":\"field3\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"updatestate\",\"State\":\"CONNECTOR_STOPPED\"}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    ret, err := dh.UpdateConnectorState("test_project", "test_topic", "connector_id", datahub.ConnectorStopped)
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
        body, err := ioutil.ReadAll(request.Body)
        assert.Nil(t, err)
        assert.NotNil(t, body)
        str := string(body)
        assert.Equal(t, "{\"Action\":\"updateshardcontext\",\"ShardId\":\"0\",\"CurrentTime\":100,\"CurrentSequence\":10}", str)

        writer.Header().Set("x-datahub-request-id", "request_id")
        writer.WriteHeader(http.StatusOK)
    }))

    defer ts.Close()
    dh := datahub.New("a", "a", ts.URL)

    offset := datahub.ConnectorOffset{
        Timestamp: 100,
        Sequence:  10,
    }

    ret, err := dh.UpdateConnectorOffset("test_project", "test_topic", "connector_id", "0", offset)
    assert.Nil(t, err)
    assert.NotNil(t, ret)
    assert.Equal(t, http.StatusOK, ret.StatusCode)
    assert.Equal(t, "request_id", ret.RequestId)
}
