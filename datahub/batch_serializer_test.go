package datahub

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchNormalSerialize(t *testing.T) {
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

	ser := newBatchSerializer("project", "topic", &cache, ZSTD)

	recordNum := 1000
	records := make([]IRecord, 0)
	for i := 0; i < recordNum; i++ {
		records = append(records, genTupleRecord(dhSchema))
	}

	buf, err := ser.serialize(records)
	assert.Nil(t, err)
	header, err := parseBatchHeader(buf)
	assert.Nil(t, err)

	assert.Equal(t, batchMagicNum, header.magic)
	assert.Equal(t, int32(1), header.version)
	assert.Equal(t, int32(len(buf)), header.length)
	assert.Equal(t, int16(3), header.attribute)
	assert.Equal(t, int16(2), header.dataType)
	assert.Equal(t, int32(1), header.schemaVersion)
	assert.Equal(t, int32(40), header.dataOffset)
	assert.Equal(t, int32(recordNum), header.recordCount)

	meta := respMeta{
		cursor:     "cursor",
		nextCursor: "nextCursor",
		sequence:   100,
		systemTime: 200,
		serial:     300,
	}
	dser := newBatchDeserializer("0", &cache)
	newRecords, err := dser.deserialize(buf, &meta)
	assert.Nil(t, err)

	assert.Equal(t, len(records), len(newRecords))
	for i := 0; i < len(records); i++ {
		assert.Equal(t, records[i].GetAttributes(), newRecords[i].GetAttributes())
		assert.Equal(t, records[i].GetData(), newRecords[i].GetData())
		assert.Equal(t, int64(100), newRecords[i].GetSequence())
		assert.Equal(t, int64(200), newRecords[i].GetSystemTime())
		assert.Equal(t, int64(300), newRecords[i].GetBaseRecord().Serial)
		assert.Equal(t, "cursor", newRecords[i].GetBaseRecord().Cursor)
		assert.Equal(t, "nextCursor", newRecords[i].GetBaseRecord().NextCursor)
		assert.Equal(t, i, newRecords[i].GetBatchIndex())
	}
}

func TestBatchSerializeWithNullValue(t *testing.T) {
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

	ser := newBatchSerializer("project", "topic", &cache, ZSTD)

	recordNum := 1000
	records := make([]IRecord, 0)
	for i := 0; i < recordNum; i++ {
		records = append(records, genTupleRecordWithNull(dhSchema, 20))
	}

	buf, err := ser.serialize(records)
	assert.Nil(t, err)
	header, err := parseBatchHeader(buf)
	assert.Nil(t, err)

	assert.Equal(t, batchMagicNum, header.magic)
	assert.Equal(t, int32(1), header.version)
	assert.Equal(t, int32(len(buf)), header.length)
	assert.Equal(t, int16(3), header.attribute)
	assert.Equal(t, int16(2), header.dataType)
	assert.Equal(t, int32(1), header.schemaVersion)
	assert.Equal(t, int32(40), header.dataOffset)
	assert.Equal(t, int32(recordNum), header.recordCount)

	meta := respMeta{
		cursor:     "cursor",
		nextCursor: "nextCursor",
		sequence:   100,
		systemTime: 200,
		serial:     300,
	}
	dser := newBatchDeserializer("0", &cache)
	newRecords, err := dser.deserialize(buf, &meta)
	assert.Nil(t, err)

	assert.Equal(t, len(records), len(newRecords))
	for i := 0; i < len(records); i++ {
		assert.Equal(t, records[i].GetAttributes(), newRecords[i].GetAttributes())
		assert.Equal(t, records[i].GetData(), newRecords[i].GetData())
		assert.Equal(t, int64(100), newRecords[i].GetSequence())
		assert.Equal(t, int64(200), newRecords[i].GetSystemTime())
		assert.Equal(t, int64(300), newRecords[i].GetBaseRecord().Serial)
		assert.Equal(t, "cursor", newRecords[i].GetBaseRecord().Cursor)
		assert.Equal(t, "nextCursor", newRecords[i].GetBaseRecord().NextCursor)
		assert.Equal(t, i, newRecords[i].GetBatchIndex())
	}
}

func TestDeserializeWithTruncateSchema(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: INTEGER, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: DOUBLE, AllowNull: true})
	avroSchema, _ := getAvroSchema(dhSchema)

	newSchema := NewRecordSchema()
	newSchema.AddField(Field{Name: "f1", Type: INTEGER, AllowNull: true})
	newSchema.AddField(Field{Name: "f2", Type: DOUBLE, AllowNull: true})
	newSchema.AddField(Field{Name: "f3", Type: STRING, AllowNull: true})
	newAvroSchema, _ := getAvroSchema(newSchema)

	serializeCache := topicSchemaCacheForTest{
		avroSchema: avroSchema,
		dhSchema:   dhSchema,
	}

	deserializeCache := topicSchemaCacheForTest{
		avroSchema: newAvroSchema,
		dhSchema:   newSchema,
	}

	ser := newBatchSerializer("project", "topic", &serializeCache, ZSTD)

	recordNum := 1000
	records := make([]IRecord, 0)
	for i := 0; i < recordNum; i++ {
		records = append(records, genTupleRecord(dhSchema))
	}

	buf, err := ser.serialize(records)
	assert.Nil(t, err)
	header, err := parseBatchHeader(buf)
	assert.Nil(t, err)

	assert.Equal(t, batchMagicNum, header.magic)
	assert.Equal(t, int32(1), header.version)
	assert.Equal(t, int32(len(buf)), header.length)
	assert.Equal(t, int16(3), header.attribute)
	assert.Equal(t, int16(2), header.dataType)
	assert.Equal(t, int32(1), header.schemaVersion)
	assert.Equal(t, int32(40), header.dataOffset)
	assert.Equal(t, int32(recordNum), header.recordCount)

	meta := respMeta{
		cursor:     "cursor",
		nextCursor: "nextCursor",
		sequence:   100,
		systemTime: 200,
		serial:     300,
	}
	dser := newBatchDeserializer("0", &deserializeCache)
	newRecords, err := dser.deserialize(buf, &meta)
	assert.Nil(t, err)

	assert.Equal(t, len(records), len(newRecords))
	for i := 0; i < len(records); i++ {
		assert.Equal(t, records[i].GetAttributes(), newRecords[i].GetAttributes())
		assert.Equal(t, records[i].GetData(), newRecords[i].GetData())
		assert.Equal(t, int64(100), newRecords[i].GetSequence())
		assert.Equal(t, int64(200), newRecords[i].GetSystemTime())
		assert.Equal(t, int64(300), newRecords[i].GetBaseRecord().Serial)
		assert.Equal(t, "cursor", newRecords[i].GetBaseRecord().Cursor)
		assert.Equal(t, "nextCursor", newRecords[i].GetBaseRecord().NextCursor)
		assert.Equal(t, i, newRecords[i].GetBatchIndex())
	}
}
