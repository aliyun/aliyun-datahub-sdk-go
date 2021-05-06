package e2e

import (
    "fmt"
    "github.com/stretchr/Testify/assert"
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
    "testing"
)

var _accessId string = ""
var _accessKey string = ""
var _endpoint string = ""
var test_connector_project string = ""
var test_topic string = ""

var ODPS_ENDPOINT string = ""
var ODPS_PROJECT string = ""
var ODPS_TABLE string = ""
var ODPS_ACCESSID string = ""
var ODPS_ACCESSKEY string = ""

var ADS_HOST string = ""
var ADS_PORT string = ""
var ADS_DATABASE string = ""
var ADS_TABLE_GROUP string = ""
var ADS_TABLE string = ""
var ADS_USER string = ""
var ADS_PASSWORD string = ""

var MYSQL_HOST string = ""
var MYSQL_PORT string = ""
var MYSQL_DATABASE string = ""
var MYSQL_TABLE string = ""
var MYSQL_USER string = ""
var MYSQL_PASSWORD string = ""

var OSS_ENDPOINT string = ""
var OSS_BUCKET string = ""
var OSS_PREFIX string = ""
var OSS_ACCESSID string = ""
var OSS_ACCESSKEY string = ""

var OTS_ENDPOINT string = ""
var OTS_INSTANCE string = ""
var OTS_TABLE string = ""
var OTS_ACCESSID string = ""
var OTS_ACCESSKEY string = ""

var FC_ENDPOINT string = ""
var FC_SERVICE string = ""
var FC_FUNCTION string = ""
var FC_ACCESSID string = ""
var FC_ACCESSKEY string = ""

var DATAHUB_ENDPOINT string = ""
var DATAHUB_PROJECT string = ""
var DATAHUB_TOPIC string = ""
var DATAHUB_ACCESSID string = ""
var DATAHUB_ACCESSKEY string = ""



// disable
func testConnector(t *testing.T) {

    _, err := client.CreateProject(test_connector_project, "comment")
    assert.Nil(t, err)
    defer client.DeleteProject(test_connector_project)

    test_odps(t)

    test_ads(t)

    test_mysql(t)

    test_oss(t)

    test_fc(t)

    test_ots(t)

    test_datahub(t)
}

func test_odps(t *testing.T) {

    var connectorId string

    topicName := "odps_test"
    odpsTimeRange := 60
    odpsPartitionMode := datahub.SystemTimeMode
    connectorType := datahub.SinkOdps

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test", 3, 7, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    odpsPartitionConfig := datahub.NewPartitionConfig()
    odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
    odpsPartitionConfig.AddConfig("hh", "%H")
    odpsPartitionConfig.AddConfig("mm", "%M")

    sinkOdpsConfig := &datahub.SinkOdpsConfig{
        Endpoint:        ODPS_ENDPOINT,
        Project:         ODPS_PROJECT,
        Table:           ODPS_TABLE,
        AccessId:        ODPS_ACCESSID,
        AccessKey:       ODPS_ACCESSKEY,
        TimeRange:       odpsTimeRange,
        PartitionMode:   odpsPartitionMode,
        PartitionConfig: *odpsPartitionConfig,
    }

    fileds := []string{fieldName1}

    fmt.Println(fileds)
    /* create connector config */
    ccr, err := client.CreateConnector(test_connector_project, topicName, connectorType, fileds, *sinkOdpsConfig)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)

    fmt.Println(*ccr)
    fmt.Println()
    connectorId = ccr.ConnectorId

    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    dt, err := client.GetConnectorDoneTime(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, dt)
    fmt.Println(*dt)
    fmt.Println()

    /* append connector config */
    _, err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    assert.Nil(t, err)

    /* update connector config */
    gc, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gc)
    config, ok := gc.Config.(datahub.SinkOdpsConfig)
    assert.True(t, ok)
    config.TimeRange = 100
    _, err = client.UpdateConnector(test_connector_project, topicName, connectorId, config)
    assert.Nil(t, err)
    gc, err = client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gc)
    config, ok = gc.Config.(datahub.SinkOdpsConfig)
    assert.True(t, ok)
    assert.Equal(t, 100, config.TimeRange)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)

}

/**
 * CREATE TABLE IF NOT EXISTS `test_datahub_ads_connector`(`test` varchar(2048) NOT NULL, `test1` bigint NULL, PRIMARY KEY(test)) PARTITION BY HASH KEY(test) PARTITION NUM 32 TABLEGROUP datahub_dailyrun options (updateType='realtime');
 */
func test_ads(t *testing.T) {
    var connectorId string
    topicName := "test_ods"

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test ods", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)
    conf := datahub.SinkAdsConfig{
        SinkMysqlConfig: datahub.SinkMysqlConfig{
            Host:     ADS_HOST,
            Port:     ADS_PORT,
            Database: ADS_DATABASE,
            Table:    ADS_TABLE,
            User:     ADS_USER,
            Password: ADS_PASSWORD,
            Ignore:   datahub.IGNORE,
        },
    }

    fileds := []string{fieldName1, fieldName2}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkAds, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}

/**
 * CREATE TABLE IF NOT EXISTS `test_datahub_mysql_connector` (`test` varchar(2048) NOT NULL, `test1` bigint(20) NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8
 */
func test_mysql(t *testing.T) {
    var connectorId string
    topicName := "test_mysql"

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test mysql", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)
    conf := datahub.SinkMysqlConfig{
        Host:     MYSQL_HOST,
        Port:     MYSQL_PORT,
        Database: MYSQL_DATABASE,
        Table:    MYSQL_TABLE,
        User:     MYSQL_USER,
        Password: MYSQL_PASSWORD,
        Ignore:   datahub.IGNORE,
    }

    fileds := []string{fieldName1, fieldName2}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkMysql, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}

func test_oss(t *testing.T) {
    var connectorId string
    topicName := "test_oss"

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test mysql", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)

    conf := datahub.SinkOssConfig{
        Endpoint:   OSS_ENDPOINT,
        Bucket:     OSS_BUCKET,
        Prefix:     OSS_PREFIX,
        TimeFormat: "%Y%m%d%H%M",
        TimeRange:  100,
        AuthMode:   datahub.AK,
        AccessId:   OSS_ACCESSID,
        AccessKey:  OSS_ACCESSKEY,
    }

    fileds := []string{fieldName1, fieldName2}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkOss, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}

func test_fc(t *testing.T) {
    var connectorId string
    topicName := "test_fc"

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test mysql", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)

    conf := datahub.SinkFcConfig{
        Endpoint:  FC_ENDPOINT,
        Service:   FC_SERVICE,
        Function:  FC_FUNCTION,
        AuthMode:  datahub.AK,
        AccessId:  FC_ACCESSID,
        AccessKey: FC_ACCESSKEY,
    }

    fileds := []string{fieldName1, fieldName2}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkFc, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}

func test_ots(t *testing.T) {
    var connectorId string
    topicName := "test_ots"

    fieldName := "pk1"
    fieldName1 := "val1"
    fieldName2 := "pk2"
    fieldName3 := "val2"
    fieldName4 := "pk3"
    fieldName5 := "val3"

    field := datahub.Field{
        Name:      fieldName,
        Type:      datahub.STRING,
        AllowNull: true,
    }

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field3 := datahub.Field{
        Name:      fieldName3,
        Type:      datahub.STRING,
        AllowNull: true,
    }

    field4 := datahub.Field{
        Name:      fieldName4,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    field5 := datahub.Field{
        Name:      fieldName5,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }

    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field,
            field1,
            field2,
            field3,
            field4,
            field5,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test mysql", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)

    conf := datahub.SinkOtsConfig{
        Endpoint:     OTS_ENDPOINT,
        InstanceName: OTS_INSTANCE,
        TableName:    OTS_TABLE,
        AuthMode:     datahub.AK,
        AccessId:     OTS_ACCESSID,
        AccessKey:    OTS_ACCESSKEY,
    }

    fileds := []string{fieldName, fieldName1, fieldName2, fieldName3, fieldName4, fieldName5}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkOts, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}



func test_datahub(t *testing.T) {
    var connectorId string
    topicName := "test_datahub"

    fieldName1 := "test"
    fieldName2 := "test1"

    field1 := datahub.Field{
        Name:      fieldName1,
        Type:      datahub.STRING,
        AllowNull: true,
    }
    field2 := datahub.Field{
        Name:      fieldName2,
        Type:      datahub.BIGINT,
        AllowNull: true,
    }
    rs := &datahub.RecordSchema{
        Fields: []datahub.Field{
            field1,
            field2,
        },
    }

    _, err := client.CreateTupleTopic(test_connector_project, topicName, "test mysql", 7, 3, rs)
    assert.Nil(t, err)
    defer client.DeleteTopic(test_connector_project, topicName)

    //port, err := strconv.Atoi(ADS_PORT)
    //assert.Nil(t, err)

    _, err = client.CreateProject(DATAHUB_PROJECT,"test")
    assert.Nil(t,err)
    defer client.DeleteProject(DATAHUB_PROJECT)
    _, err = client.CreateTupleTopic(DATAHUB_PROJECT,DATAHUB_TOPIC,"test",7,3,rs)
    assert.Nil(t,err)
    defer client.DeleteTopic(DATAHUB_PROJECT,DATAHUB_TOPIC)

    conf := datahub.SinkDatahubConfig{
        Endpoint:_endpoint,
        Project:DATAHUB_PROJECT,
        Topic:DATAHUB_TOPIC,
        AuthMode:datahub.AK,
        AccessId:_accessId,
        AccessKey:_accessKey,
    }

    fileds := []string{fieldName1, fieldName2}
    ccr, err := client.CreateConnector(test_connector_project, topicName, datahub.SinkDatahub, fileds, conf)
    assert.Nil(t, err)
    assert.NotNil(t, ccr)
    connectorId = ccr.ConnectorId
    defer client.DeleteConnector(test_connector_project, topicName, connectorId)

    /* get connector */
    gcr, err := client.GetConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcr)
    fmt.Println(*gcr)
    fmt.Println()

    /* list connector config */
    lc, err := client.ListConnector(test_connector_project, topicName)
    assert.Nil(t, err)
    assert.NotNil(t, lc)
    fmt.Println(*lc)
    fmt.Println()

    /* get connector done time */
    //Only SINK_ODPS has doneTime information

    /* append connector config */
    // ads 在创建 connector  时必须要链接所有列，所以无法实时测试appendField
    //err = client.AppendConnectorField(test_connector_project, topicName, connectorId, fieldName2)
    //assert.Nil(t, err)

    //get connector shard status
    gcs, err := client.GetConnectorShardStatus(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    assert.NotNil(t, gcs)
    fmt.Println("****** list connector status *****")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }
    fmt.Println()
    shardId := "0"
    gs, err := client.GetConnectorShardStatusByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    fmt.Println(*gs)
    fmt.Println()

    /* update connector state */
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)
    shardId = "0"
    ot := datahub.ConnectorOffset{
        Timestamp: 1000,
        Sequence:  104,
    }
    // should update the state to stopped before update connector offset
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorStopped)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorOffset(test_connector_project, topicName, connectorId, shardId, ot)
    assert.Nil(t, err)
    _, err = client.UpdateConnectorState(test_connector_project, topicName, connectorId, datahub.ConnectorRunning)
    assert.Nil(t, err)

    /* reload connector */
    _, err = client.ReloadConnector(test_connector_project, topicName, connectorId)
    assert.Nil(t, err)
    _, err = client.ReloadConnectorByShard(test_connector_project, topicName, connectorId, shardId)
    assert.Nil(t, err)
}
