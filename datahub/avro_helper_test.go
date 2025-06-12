package datahub

import (
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/stretchr/testify/assert"
)

func TestGenBlobSchema(t *testing.T) {
	avroSchema, err := getAvroSchema(nil)
	assert.Nil(t, err)
	recordSchema, ok := avroSchema.(*avro.RecordSchema)
	assert.True(t, ok)
	assert.Equal(t, recordSchema.Name(), "AvroRecord")
	assert.Equal(t, len(recordSchema.Fields()), 2)
	assert.Equal(t, recordSchema.Fields()[0].String(), "{\"name\":\"data\",\"type\":\"bytes\"}")
	assert.Equal(t, recordSchema.Fields()[1].String(), "{\"name\":\"__dh_attribute__\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}")
}

func TestGenTupleSchema(t *testing.T) {
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

	avroSchema, err := getAvroSchema(dhSchema)
	assert.Nil(t, err)
	recordSchema, ok := avroSchema.(*avro.RecordSchema)
	assert.True(t, ok)
	assert.Equal(t, recordSchema.Name(), "AvroRecord")
	assert.Equal(t, len(recordSchema.Fields()), 12)
	assert.Equal(t, recordSchema.Fields()[0].String(), "{\"name\":\"f1\",\"type\":[\"null\",\"boolean\"]}")
	assert.Equal(t, recordSchema.Fields()[1].String(), "{\"name\":\"f2\",\"type\":[\"null\",\"int\"]}")
	assert.Equal(t, recordSchema.Fields()[2].String(), "{\"name\":\"f3\",\"type\":[\"null\",\"int\"]}")
	assert.Equal(t, recordSchema.Fields()[3].String(), "{\"name\":\"f4\",\"type\":[\"null\",\"int\"]}")
	assert.Equal(t, recordSchema.Fields()[4].String(), "{\"name\":\"f5\",\"type\":[\"null\",\"long\"]}")
	assert.Equal(t, recordSchema.Fields()[5].String(), "{\"name\":\"f6\",\"type\":[\"null\",\"long\"]}")
	assert.Equal(t, recordSchema.Fields()[6].String(), "{\"name\":\"f7\",\"type\":[\"null\",\"float\"]}")
	assert.Equal(t, recordSchema.Fields()[7].String(), "{\"name\":\"f8\",\"type\":[\"null\",\"double\"]}")
	assert.Equal(t, recordSchema.Fields()[8].String(), "{\"name\":\"f9\",\"type\":[\"null\",\"string\"]}")
	assert.Equal(t, recordSchema.Fields()[9].String(), "{\"name\":\"f10\",\"type\":[\"null\",\"string\"]}")
	assert.Equal(t, recordSchema.Fields()[10].String(), "{\"name\":\"f11\",\"type\":[\"null\",\"string\"]}")
	assert.Equal(t, recordSchema.Fields()[11].String(), "{\"name\":\"__dh_attribute__\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}")
}

func TestGenTupleSchemaWithNotNull(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: false})
	dhSchema.AddField(Field{Name: "f2", Type: TINYINT, AllowNull: false})
	dhSchema.AddField(Field{Name: "f3", Type: SMALLINT, AllowNull: false})
	dhSchema.AddField(Field{Name: "f4", Type: INTEGER, AllowNull: false})
	dhSchema.AddField(Field{Name: "f5", Type: BIGINT, AllowNull: false})
	dhSchema.AddField(Field{Name: "f6", Type: TIMESTAMP, AllowNull: false})
	dhSchema.AddField(Field{Name: "f7", Type: FLOAT, AllowNull: false})
	dhSchema.AddField(Field{Name: "f8", Type: DOUBLE, AllowNull: false})
	dhSchema.AddField(Field{Name: "f9", Type: DECIMAL, AllowNull: false})
	dhSchema.AddField(Field{Name: "f10", Type: STRING, AllowNull: false})
	dhSchema.AddField(Field{Name: "f11", Type: JSON, AllowNull: false})

	avroSchema, err := getAvroSchema(dhSchema)
	assert.Nil(t, err)
	recordSchema, ok := avroSchema.(*avro.RecordSchema)
	assert.True(t, ok)
	assert.Equal(t, recordSchema.Name(), "AvroRecord")
	assert.Equal(t, len(recordSchema.Fields()), 12)
	assert.Equal(t, recordSchema.Fields()[0].String(), "{\"name\":\"f1\",\"type\":\"boolean\"}")
	assert.Equal(t, recordSchema.Fields()[1].String(), "{\"name\":\"f2\",\"type\":\"int\"}")
	assert.Equal(t, recordSchema.Fields()[2].String(), "{\"name\":\"f3\",\"type\":\"int\"}")
	assert.Equal(t, recordSchema.Fields()[3].String(), "{\"name\":\"f4\",\"type\":\"int\"}")
	assert.Equal(t, recordSchema.Fields()[4].String(), "{\"name\":\"f5\",\"type\":\"long\"}")
	assert.Equal(t, recordSchema.Fields()[5].String(), "{\"name\":\"f6\",\"type\":\"long\"}")
	assert.Equal(t, recordSchema.Fields()[6].String(), "{\"name\":\"f7\",\"type\":\"float\"}")
	assert.Equal(t, recordSchema.Fields()[7].String(), "{\"name\":\"f8\",\"type\":\"double\"}")
	assert.Equal(t, recordSchema.Fields()[8].String(), "{\"name\":\"f9\",\"type\":\"string\"}")
	assert.Equal(t, recordSchema.Fields()[9].String(), "{\"name\":\"f10\",\"type\":\"string\"}")
	assert.Equal(t, recordSchema.Fields()[10].String(), "{\"name\":\"f11\",\"type\":\"string\"}")
	assert.Equal(t, recordSchema.Fields()[11].String(), "{\"name\":\"__dh_attribute__\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}]}")
}
