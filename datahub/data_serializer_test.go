package datahub

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

type topicSchemaCacheForTest struct {
	avroSchema avro.Schema
	dhSchema   *RecordSchema
}

func (tsc *topicSchemaCacheForTest) init() {
}

func (tsc *topicSchemaCacheForTest) getMaxSchemaVersionId() int {
	return 1
}
func (tsc *topicSchemaCacheForTest) getSchemaByVersionId(versionId int) *RecordSchema {
	return tsc.dhSchema
}
func (tsc *topicSchemaCacheForTest) getVersionIdBySchema(schema *RecordSchema) int {
	return 1
}
func (tsc *topicSchemaCacheForTest) getAvroSchema(schema *RecordSchema) avro.Schema {
	return tsc.avroSchema
}
func (tsc *topicSchemaCacheForTest) getAvroSchemaByVersionId(versionId int) avro.Schema {
	return tsc.avroSchema
}

func randomString(length int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	s := make([]byte, 0)
	for i := 0; i < length; i++ {
		s = append(s, letterBytes[rand.Int()%len(letterBytes)])
	}

	return string(s)
}

func randomJson() string {
	m := make(map[string]string)
	m[randomString(10)] = randomString(20)
	m[randomString(10)] = randomString(20)
	m[randomString(10)] = randomString(20)
	m[randomString(10)] = randomString(20)
	val, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return string(val)
}

func genTupleRecord(schema *RecordSchema) *TupleRecord {
	return genTupleRecordWithNull(schema, 0)
}

func genTupleRecordWithNull(schema *RecordSchema, nullRatio int) *TupleRecord {
	record := NewTupleRecord(schema)
	for _, field := range schema.Fields {
		if nullRatio > 0 && rand.Intn(100) < nullRatio {
			record.SetValueByName(field.Name, nil)
			continue
		}
		switch field.Type {
		case BOOLEAN:
			record.SetValueByName(field.Name, rand.Int()%2 == 0)
		case TINYINT:
			record.SetValueByName(field.Name, rand.Intn(math.MaxInt8))
		case SMALLINT:
			record.SetValueByName(field.Name, rand.Intn(math.MaxInt16))
		case INTEGER:
			record.SetValueByName(field.Name, rand.Intn(math.MaxInt32))
		case BIGINT:
			record.SetValueByName(field.Name, rand.Int63())
		case TIMESTAMP:
			record.SetValueByName(field.Name, time.Now().UnixMilli())
		case FLOAT:
			record.SetValueByName(field.Name, rand.Float32())
		case DOUBLE:
			record.SetValueByName(field.Name, rand.Float64())
		case STRING:
			record.SetValueByName(field.Name, randomString(64))
		case DECIMAL:
			record.SetValueByName(field.Name, decimal.NewFromFloat32(rand.Float32()))
		case JSON:
			record.SetValueByName(field.Name, randomJson())
		}
	}
	record.SetAttribute(randomString(5), randomString(10))
	record.SetAttribute(randomString(5), randomString(10))
	return record
}

// func genBlobRecord() *TupleRecord {
// }

func TestAvroNormalSerialize(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: TINYINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f3", Type: SMALLINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f4", Type: INTEGER, AllowNull: true})
	dhSchema.AddField(Field{Name: "f5", Type: BIGINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f6", Type: TIMESTAMP, AllowNull: true})
	dhSchema.AddField(Field{Name: "f7", Type: FLOAT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f8", Type: DOUBLE, AllowNull: true})
	dhSchema.AddField(Field{Name: "f9", Type: DECIMAL, AllowNull: true})
	dhSchema.AddField(Field{Name: "f10", Type: STRING, AllowNull: true})
	dhSchema.AddField(Field{Name: "f11", Type: JSON, AllowNull: true})

	avroSchema, _ := getAvroSchema(dhSchema)
	cache := topicSchemaCacheForTest{
		avroSchema: avroSchema,
		dhSchema:   dhSchema,
	}

	ser := newDataSerializer(&cache)

	recordNum := 100
	records := make([]IRecord, 0)
	for i := 0; i < recordNum; i++ {
		records = append(records, genTupleRecord(dhSchema))
	}

	buf, err := ser.serialize(records)
	assert.Nil(t, err)

	header := batchHeader{
		schemaVersion: 1,
		recordCount:   int32(recordNum),
	}
	dser := newDataDeserializer(&cache)
	newRecords, err := dser.deserialize(buf, &header)
	assert.Nil(t, err)

	assert.Equal(t, len(records), len(newRecords))
	for i := 0; i < len(records); i++ {
		assert.Equal(t, records[i].GetAttributes(), newRecords[i].GetAttributes())
		assert.Equal(t, records[i].GetData(), newRecords[i].GetData())
	}
}

func TestAvroSerializeWithNullValue(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: TINYINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f3", Type: SMALLINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f4", Type: INTEGER, AllowNull: true})
	dhSchema.AddField(Field{Name: "f5", Type: BIGINT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f6", Type: TIMESTAMP, AllowNull: true})
	dhSchema.AddField(Field{Name: "f7", Type: FLOAT, AllowNull: true})
	dhSchema.AddField(Field{Name: "f8", Type: DOUBLE, AllowNull: true})
	dhSchema.AddField(Field{Name: "f9", Type: DECIMAL, AllowNull: true})
	dhSchema.AddField(Field{Name: "f10", Type: STRING, AllowNull: true})
	dhSchema.AddField(Field{Name: "f11", Type: JSON, AllowNull: true})

	avroSchema, _ := getAvroSchema(dhSchema)
	cache := topicSchemaCacheForTest{
		avroSchema: avroSchema,
		dhSchema:   dhSchema,
	}

	ser := newDataSerializer(&cache)

	recordNum := 100
	records := make([]IRecord, 0)
	for i := 0; i < recordNum; i++ {
		records = append(records, genTupleRecordWithNull(dhSchema, 20))
	}

	buf, err := ser.serialize(records)
	assert.Nil(t, err)

	header := batchHeader{
		schemaVersion: 1,
		recordCount:   int32(recordNum),
	}
	dser := newDataDeserializer(&cache)
	newRecords, err := dser.deserialize(buf, &header)
	assert.Nil(t, err)

	assert.Equal(t, len(records), len(newRecords))
	for i := 0; i < len(records); i++ {
		assert.Equal(t, records[i].GetAttributes(), newRecords[i].GetAttributes())
		assert.Equal(t, records[i].GetData(), newRecords[i].GetData())
	}
}
