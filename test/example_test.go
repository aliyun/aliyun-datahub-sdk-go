package Test

import (
    "../datahub"
    "fmt"
    "github.com/stretchr/Testify/assert"
    "testing"
    "time"
)

var accessId = ""
var accessKey = ""
var endpoint = ""
var projectName = "_project_Test_"
var tupleTopicName = "_tuple_topic_Test_"
var blobTopicName = "_blob_topic_Test_"

var subId string
var connectorId string
var dh datahub.DataHub = datahub.New(accessId, accessKey, endpoint)


func ExampleNew() {
    //var dh DataHub
    dh = datahub.New(accessId, accessKey, endpoint)
}

func TestRun(t *testing.T) {
    fmt.Println("######################### project ###################################")
    /* create project */
    err := dh.CreateProject(projectName, "project comment")
    assert.Nil(t, err)
    defer dh.DeleteProject(projectName)
    gp, err := dh.GetProject(projectName)
    assert.Nil(t, err)
    assert.Equal(t, "project comment", gp.Comment)

    /* list project */
    lp, err := dh.ListProject()
    assert.Nil(t, err)
    assert.NotNil(t, lp)
    fmt.Println("****** list project ****")
    for _, projectName := range lp.ProjectNames {
        fmt.Println(projectName)
    }
    fmt.Println()

    /* update project */
    time.Sleep(1 * time.Second)
    err = dh.UpdateProject(projectName, "new project comment")
    assert.Nil(t, err)
    time.Sleep(1 * time.Second)
    gp, err = dh.GetProject(projectName)
    assert.Nil(t, err)
    assert.NotNil(t, gp)
    assert.Equal(t, "new project comment", gp.Comment)

    fmt.Println("######################### topic ###################################")
    /* create tuple topic */
    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false}).
        AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
        AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN})
    err = dh.CreateTupleTopic(projectName, tupleTopicName, "topic comment", 5, 7, recordSchema)
    assert.Nil(t, err)
    defer dh.DeleteTopic(projectName, tupleTopicName)
    gt, err := dh.GetTopic(projectName, tupleTopicName)
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    assert.Equal(t, "topic comment", gt.Comment)

    /* create blob topic */
    err = dh.CreateBlobTopic(projectName, blobTopicName, "topic comment", 5, 7)
    assert.Nil(t, err)
    defer dh.DeleteTopic(projectName, blobTopicName)
    gt, err = dh.GetTopic(projectName, blobTopicName)
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    assert.Equal(t, "topic comment", gt.Comment)

    /* list topic */
    lt, err := dh.ListTopic(projectName)
    assert.Nil(t, err)
    assert.NotNil(t, lt)
    fmt.Println("****** list topic *****")
    for _, top := range lt.TopicNames {
        fmt.Println(top)
    }
    fmt.Println()

    /* update topic */
    time.Sleep(1 * time.Second)
    err = dh.UpdateTopic(projectName, tupleTopicName, "new topic comment")
    time.Sleep(1 * time.Second)
    gt, err = dh.GetTopic(projectName, tupleTopicName)
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    assert.Equal(t, "new topic comment", gt.Comment)

    fmt.Println("######################### shard ###################################")
    /* list shard */
    ls, err := dh.ListShard(projectName, tupleTopicName)
    assert.Nil(t, err)
    fmt.Println("****** list shard *****")
    for _, shard := range ls.Shards {
        fmt.Println(shard)
    }
    fmt.Println()

    /* split shard */
    time.Sleep(3 * time.Second)
    shardId := "2"
    ss, err := dh.SplitShard(projectName, tupleTopicName, shardId)
    assert.Nil(t, err)
    assert.NotNil(t, gt)
    fmt.Println(*ss)

    /* merge shard */
    time.Sleep(3 * time.Second)
    shardId = "3"
    adjacentShardId := "4"
    ms, err := dh.MergeShard(projectName, tupleTopicName, shardId, adjacentShardId)
    assert.Nil(t, err)
    assert.NotNil(t, ms)
    fmt.Println(*ms)

    fmt.Println("######################### put&get records ###################################")
    PutRecords(t)
    PutRecords2(t)
    GetTupleRecords(t)
    GetBlobRecords(t)

    //fmt.Println("######################### meter ###################################")
    //time.Sleep(5 * time.Second)
    //shardId = "0"
    //gmi, err := dh.GetMeterInfo(projectName, tupleTopicName, shardId)
    //assert.Nil(t, err)
    //assert.NotNil(t, gmi)
    //fmt.Println(*gmi)

    fmt.Println("######################### subscription ###################################")
    /* create subscription */
    err = dh.CreateSubscription(projectName, tupleTopicName, "sub comment")
    assert.Nil(t, err)

    /* list subscription */
    pageIndex := 1
    pageSize := 5
    lss, err := dh.ListSubscription(projectName, tupleTopicName, pageIndex, pageSize)
    assert.Nil(t, err)
    assert.NotNil(t, lss)
    fmt.Println("****** list subscription *****")
    for _, sub := range lss.Subscriptions {
        fmt.Println(sub)
        subId = sub.SubId
    }

    /* get subscription */
    gs, err := dh.GetSubscription(projectName, tupleTopicName, subId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    assert.Equal(t, "sub comment", gs.Comment)

    /* update subscription */
    err = dh.UpdateSubscription(projectName, tupleTopicName, subId, "new sub comment")
    assert.Nil(t, err)
    gs, err = dh.GetSubscription(projectName, tupleTopicName, subId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    assert.Equal(t, "new sub comment", gs.Comment)

    /* update subscription state */
    err = dh.UpdateSubscriptionState(projectName, tupleTopicName, subId, datahub.SUB_OFFLINE)
    assert.Nil(t, err)
    gs, err = dh.GetSubscription(projectName, tupleTopicName, subId)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    assert.Equal(t, datahub.SUB_OFFLINE, gs.State)
    err = dh.UpdateSubscriptionState(projectName, tupleTopicName, subId, datahub.SUB_ONLINE)
    assert.Nil(t, err)
    fmt.Println()

    fmt.Println("######################### offset ###################################")
    /* open session offset */
    shardIds := []string{"0", "1", "2"}
    oss, err := dh.OpenSubscriptionSession(projectName, tupleTopicName, subId, shardIds)
    assert.Nil(t, err)
    assert.NotNil(t, oss)
    fmt.Println(*oss)

    /* get offset */
    gss, err := dh.GetSubscriptionOffset(projectName, tupleTopicName, subId, shardIds)
    assert.Nil(t, err)
    assert.NotNil(t, gss)
    fmt.Println(*gss)

    /* commit offset */
    shardId = "1"
    offset := oss.Offsets[shardId]
    // set offset message
    offset.Sequence = 309
    offset.Timestamp = 1565593166690
    offsetMap := map[string]datahub.SubscriptionOffset{
        shardId: offset,
    }
    err = dh.CommitSubscriptionOffset(projectName, tupleTopicName, subId, offsetMap)
    assert.Nil(t, err)
    gss, err = dh.GetSubscriptionOffset(projectName, tupleTopicName, subId, shardIds)
    assert.Nil(t, err)
    assert.NotNil(t, gss)
    assert.Equal(t, offset.Sequence, gss.Offsets[shardId].Sequence)
    assert.Equal(t, offset.Timestamp, gss.Offsets[shardId].Timestamp)

    /* reset offset */
    offset = datahub.SubscriptionOffset{
        Timestamp: 1565593166690,
    }
    offsetMap = map[string]datahub.SubscriptionOffset{
        shardId: offset,
    }
    err = dh.ResetSubscriptionOffset(projectName, tupleTopicName, subId, offsetMap)
    assert.Nil(t, err)
    gss, err = dh.GetSubscriptionOffset(projectName, tupleTopicName, subId, shardIds)
    assert.Nil(t, err)
    assert.NotNil(t, gss)
    assert.Equal(t, offset.Timestamp, gss.Offsets[shardId].Timestamp)

    defer dh.DeleteSubscription(projectName, tupleTopicName, subId)
    fmt.Println()
}

func PutRecords(t *testing.T) {
    /* put tuple data */
    fmt.Println("************** put tuple data **************")
    topic, err := dh.GetTopic(projectName, tupleTopicName)
    assert.Nil(t, err)
    assert.NotNil(t, topic)
    fmt.Println(*topic.RecordSchema)

    putNum := 100

    records := make([]datahub.IRecord, putNum)
    record1 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record1.ShardId = "0"
    record1.SetValueByName("bigint_field", 1)
    record1.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
    record1.SetValueByName("string_field", "Test1")
    record1.SetValueByName("double_field", 1.1111)
    record1.SetValueByName("boolean_field", true)

    // you can add some attributes when put record
    record1.SetAttribute("attribute", "Test attribute")
    //records[0] = record1

    record2 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record2.ShardId = "1"
    record2.SetValueByName("bigint_field", 2)
    record2.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
    record2.SetValueByName("string_field", "Test2")
    record2.SetValueByName("double_field", 2.2222)
    record2.SetValueByName("boolean_field", true)
    //records[1] = record2

    record3 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record3.ShardId = "2"
    record3.SetValueByName("bigint_field", 3)
    record3.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
    record3.SetValueByName("string_field", "Test3")
    record3.SetValueByName("double_field", 3.3333)
    record3.SetValueByName("boolean_field", true)
    //records[2] = record3

    for i := 0; i < putNum; i++ {
        switch i % 3 {
        case 0:
            records[i] = record1
        case 1:
            records[i] = record2
        case 2:
            records[i] = record3
        }
    }

    result, err := dh.PutRecords(projectName, tupleTopicName, records)
    assert.Nil(t, err)
    assert.NotNil(t, result)

    fmt.Println("****** put result ****")
    fmt.Printf("putRecord failed num is %d\n", result.FailedRecordCount)
    for _, v := range result.FailedRecords {
        fmt.Println(v)
    }
    fmt.Println()

}

func PutRecords2(t *testing.T) {
    /* put blob data */
    putNum := 100
    fmt.Println("************** put blob data **************")
    records := make([]datahub.IRecord, putNum)
    record1 := datahub.NewBlobRecord([]byte("blob Test1"), 0)
    record1.ShardId = "0"
    records[0] = record1

    record2 := datahub.NewBlobRecord([]byte("blob Test2"), 0)
    record2.ShardId = "1"
    records[1] = record2

    record3 := datahub.NewBlobRecord([]byte("blob Test3"), 0)
    record3.ShardId = "2"
    records[2] = record3

    for i := 0; i < putNum; i++ {
        switch i % 3 {
        case 0:
            records[i] = record1
        case 1:
            records[i] = record2
        case 2:
            records[i] = record3
        }
    }

    result, err := dh.PutRecords(projectName, blobTopicName, records)
    assert.Nil(t, err)
    assert.NotNil(t, result)

    fmt.Println("****** put result ****")
    fmt.Printf("putRecord failed num is %d\n", result.FailedRecordCount)
    for _, v := range result.FailedRecords {
        fmt.Println(v)
    }
    fmt.Println()
}

func GetTupleRecords(t *testing.T) {
    fmt.Println("************** get tuple data **************")
    shardId := "0"
    topic, err := dh.GetTopic(projectName, tupleTopicName)
    assert.Nil(t, err)
    assert.NotNil(t, topic)

    cursor, err := dh.GetCursor(projectName, tupleTopicName, shardId, datahub.OLDEST)
    assert.Nil(t, err)
    assert.NotNil(t, cursor)

    limitNum := 100
    gr, err := dh.GetTupleRecords(projectName, tupleTopicName, shardId, cursor.Cursor, limitNum, topic.RecordSchema)
    assert.Nil(t, err)
    assert.NotNil(t, gr)

    fmt.Println("****** get result ****")
    fmt.Printf("get record num is %d\n", gr.RecordCount)
    for _, record := range gr.Records {
        data, ok := record.(*datahub.TupleRecord)
        assert.True(t, ok)
        fmt.Println(data.Values)
    }
    fmt.Println()
}

func GetBlobRecords(t *testing.T) {
    fmt.Println("************** get blob data **************")
    shardId := "1"

    cursor, err := dh.GetCursor(projectName, blobTopicName, shardId, datahub.OLDEST)
    assert.Nil(t, err)
    assert.NotNil(t, cursor)

    limitNum := 100
    gr, err := dh.GetBlobRecords(projectName, blobTopicName, shardId, cursor.Cursor, limitNum)
    assert.Nil(t, err)
    assert.NotNil(t, gr)

    fmt.Println("****** get result ****")
    fmt.Printf("get record num is %d\n", gr.RecordCount)
    for _, record := range gr.Records {
        data, ok := record.(*datahub.BlobRecord)
        assert.True(t, ok)
        fmt.Println(data.StoreData)
    }
    fmt.Println()
}
