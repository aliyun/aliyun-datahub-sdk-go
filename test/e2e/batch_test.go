package e2e

import (

    "github.com/shopspring/decimal"
    "github.com/stretchr/Testify/assert"
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
    "testing"
)

func TestBatch(t *testing.T) {
    recordSchema1 := datahub.NewRecordSchema()
    recordSchema1.AddField(datahub.Field{Name: "f1", Type: datahub.TINYINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f2", Type: datahub.SMALLINT, AllowNull: true}).
        AddField(datahub.Field{Name: "f3", Type: datahub.INTEGER, AllowNull: true}).
        AddField(datahub.Field{Name: "f4", Type: datahub.BIGINT, AllowNull: false}).
        AddField(datahub.Field{Name: "f5", Type: datahub.TIMESTAMP, AllowNull: true}).
        AddField(datahub.Field{Name: "f6", Type: datahub.FLOAT, AllowNull: true}).
        AddField(datahub.Field{Name: "f7", Type: datahub.DOUBLE, AllowNull: true}).
        AddField(datahub.Field{Name: "f8", Type: datahub.DECIMAL, AllowNull: false}).
        AddField(datahub.Field{Name: "f9", Type: datahub.BOOLEAN, AllowNull: true}).
        AddField(datahub.Field{Name: "f10", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "f11", Type: datahub.STRING, AllowNull: true})

    recordSchema2 := datahub.NewRecordSchema()
    recordSchema2.AddField(datahub.Field{Name: "field1", Type: datahub.STRING, AllowNull: true}).
        AddField(datahub.Field{Name: "field2", Type: datahub.BIGINT, AllowNull: false}).
        AddField(datahub.Field{Name: "field3", Type: datahub.BIGINT, AllowNull: false})

    cp, err := batchClient.CreateProject(projectName, batchTopic)
    assert.Nil(t, err)
    assert.NotNil(t, cp)
    defer batchClient.DeleteProject(projectName)
    cbt, err := batchClient.CreateBlobTopic(projectName, batchTopic, "test", 1, 1)
    assert.Nil(t, err)
    assert.NotNil(t, cbt)
    defer batchClient.DeleteTopic(projectName, batchTopic)

    //time.Sleep(5)

    rt, err := batchClient.RegisterTopicSchema(projectName, batchTopic, recordSchema1)
    assert.Nil(t, err)
    assert.NotNil(t, rt)
    assert.Equal(t, rt.StatusCode, 201)
    rt, err = batchClient.RegisterTopicSchema(projectName, batchTopic, recordSchema2)
    assert.Nil(t, err)
    assert.NotNil(t, rt)
    assert.Equal(t, rt.StatusCode, 201)

    lt, err := batchClient.ListTopicSchema(projectName, batchTopic)
    assert.Nil(t, err)
    assert.NotNil(t, lt)
    assert.Equal(t, lt.SchemaInfoList[0].RecordSchema.String(), recordSchema1.String())
    assert.Equal(t, lt.SchemaInfoList[0].VersionId, 0)
    assert.Equal(t, lt.SchemaInfoList[1].RecordSchema.String(), recordSchema2.String())
    assert.Equal(t, lt.SchemaInfoList[1].VersionId, 1)

    gs, err := batchClient.GetTopicSchemaByVersion(projectName, batchTopic, 1)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    assert.Equal(t, gs.VersionId, 1)
    assert.Equal(t, gs.RecordSchema.String(), recordSchema2.String())

    gs, err = batchClient.GetTopicSchemaBySchema(projectName, batchTopic, recordSchema2)
    assert.Nil(t, err)
    assert.NotNil(t, gs)
    assert.Equal(t, gs.VersionId, 1)
    assert.Equal(t, gs.RecordSchema.String(), recordSchema2.String())

    records := make([]datahub.IRecord, 0)
    record1 := datahub.NewTupleRecord(recordSchema1, 0)
    record1.SetValueByName("f1", 11)
    record1.SetValueByName("f2", 222)
    record1.SetValueByName("f3", 33333)
    record1.SetValueByName("f4", 44444444)
    record1.SetValueByName("f5", 56789)
    record1.SetValueByName("f6", float32(3.145))
    record1.SetValueByName("f7", 3.146)
    val, _ := decimal.NewFromString("789.123456")
    record1.SetValueByName("f8", val)
    record1.SetValueByName("f9", true)
    record1.SetValueByName("f10", "1234567894546asdf")
    record1.SetAttribute("bbbbb", "ffffffffaaaaaaa")
    record1.SetAttribute("aaaaa", "ffffffffbbbbbb")
    records = append(records, record1)

    record2 := datahub.NewTupleRecord(recordSchema2, 0)
    record2.SetValueByName("field1", "test2")
    record2.SetValueByName("field2", 111)
    record2.SetValueByName("field3", 123)
    records = append(records, record2)

    record3 := datahub.NewTupleRecord(recordSchema2, 0)
    record3.SetValueByName("field1", "test3")
    record3.SetValueByName("field2", 222)
    record3.SetValueByName("field3", 333)
    record3.SetAttribute("key1", "value1")
    record3.SetAttribute("key2", "value2")
    record3.SetAttribute("key3", "value3")
    records = append(records, record3)

    record4 := datahub.NewTupleRecord(recordSchema2, 0)
    record4.SetValueByName("field1", "test4")
    record4.SetValueByName("field2", 2222)
    record4.SetValueByName("field3", 3333)
    record4.SetAttribute("key1", "value1")
    record4.SetAttribute("key2", "value2")
    record4.SetAttribute("key3", "value3")
    records = append(records, record4)

    ret, err := batchClient.PutRecordsByShard(projectName, batchTopic, "0", records)
    assert.Nil(t, err)
    assert.NotNil(t, ret)

    gc, err := batchClient.GetCursor(projectName, batchTopic, "0", datahub.OLDEST)
    assert.Nil(t, err)
    assert.NotNil(t, gc)

    gb, err := batchClient.GetBlobRecords(projectName, batchTopic, "0", gc.Cursor, 100)
    assert.Nil(t, err)
    assert.NotNil(t, gb)
    assert.Equal(t, gb.StartSequence, int64(0))
    assert.Equal(t, gb.LatestSequence, int64(0))
    assert.Equal(t, gb.RecordCount, 4)
    assert.Equal(t, len(gb.Records), 4)

    tupleRecord, ok := gb.Records[0].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, 11, tupleRecord.GetValueByIdx(0))
    assert.EqualValues(t, 222, tupleRecord.GetValueByIdx(1))
    assert.EqualValues(t, 33333, tupleRecord.GetValueByIdx(2))
    assert.EqualValues(t, 44444444, tupleRecord.GetValueByIdx(3))
    assert.EqualValues(t, 0xddd5, tupleRecord.GetValueByIdx(4))
    assert.EqualValues(t, 3.145, tupleRecord.GetValueByIdx(5))
    assert.EqualValues(t, 3.146, tupleRecord.GetValueByIdx(6))
    assert.EqualValues(t, "789.123456", tupleRecord.GetValueByIdx(7).String())
    assert.EqualValues(t, true, tupleRecord.GetValueByIdx(8))
    assert.EqualValues(t, "1234567894546asdf", tupleRecord.GetValueByIdx(9))
    assert.EqualValues(t, nil, tupleRecord.GetValueByIdx(10))
    assert.Equal(t, map[string]interface{}(map[string]interface{}{"aaaaa": "ffffffffbbbbbb", "bbbbb": "ffffffffaaaaaaa"}), tupleRecord.GetAttributes())

    tupleRecord, ok = gb.Records[1].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, "test2", tupleRecord.GetValueByIdx(0))
    assert.EqualValues(t, 111, tupleRecord.GetValueByIdx(1))
    assert.EqualValues(t, 123, tupleRecord.GetValueByIdx(2))

    tupleRecord, ok = gb.Records[2].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, "test3", tupleRecord.GetValueByIdx(0))
    assert.EqualValues(t, 222, tupleRecord.GetValueByIdx(1))
    assert.EqualValues(t, 333, tupleRecord.GetValueByIdx(2))

    tupleRecord, ok = gb.Records[3].(*datahub.TupleRecord)
    assert.True(t, ok)
    assert.EqualValues(t, "test4", tupleRecord.GetValueByIdx(0))
    assert.EqualValues(t, 2222, tupleRecord.GetValueByIdx(1))
    assert.EqualValues(t, 3333, tupleRecord.GetValueByIdx(2))
}
