package datahub

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRecordSchema_UnmaschemahalJSON(t *testing.T) {
	schema := NewRecordSchema()

	err := json.Unmarshal([]byte(`{
		"fields":[
			{"name":"f1","type":"STRING","notnull":false,"comment":"c1"},
			{"name":"f2","type":"BIGINT","notnull":true,"comment":"c2"},
			{"name":"f3","type":"DOUBLE","notnull":false,"comment":"c3"}
		]}`), schema)

	assert.Nil(t, err)
	assert.Equal(t, 3, len(schema.Fields))
	assert.Equal(t, "f1", schema.Fields[0].Name)
	assert.Equal(t, STRING, schema.Fields[0].Type)
	assert.Equal(t, false, schema.Fields[0].AllowNull)
	assert.Equal(t, "c1", schema.Fields[0].Comment)

	assert.Equal(t, "f2", schema.Fields[1].Name)
	assert.Equal(t, BIGINT, schema.Fields[1].Type)
	assert.Equal(t, true, schema.Fields[1].AllowNull)
	assert.Equal(t, "c2", schema.Fields[1].Comment)

	assert.Equal(t, "f3", schema.Fields[2].Name)
	assert.Equal(t, DOUBLE, schema.Fields[2].Type)
	assert.Equal(t, false, schema.Fields[2].AllowNull)
	assert.Equal(t, "c3", schema.Fields[2].Comment)

	assert.Equal(t, 3, len(schema.fieldIndexMap))
	assert.Equal(t, 0, schema.fieldIndexMap["f1"])
	assert.Equal(t, 1, schema.fieldIndexMap["f2"])
	assert.Equal(t, 2, schema.fieldIndexMap["f3"])
}

func TestSchemaGetFiled(t *testing.T) {
	schema := NewRecordSchema()

	schema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true, Comment: "test_f1"})
	schema.AddField(Field{Name: "f2", Type: INTEGER, AllowNull: false, Comment: "test_f2"})
	schema.AddField(Field{Name: "f3", Type: STRING, AllowNull: true, Comment: "test_f3"})

	assert.Equal(t, schema.GetFieldIndex("f1"), 0)
	assert.Equal(t, schema.GetFieldIndex("f2"), 1)
	assert.Equal(t, schema.GetFieldIndex("f3"), 2)
	assert.Equal(t, schema.GetFieldIndex("f4"), -1)

	col, err := schema.GetFieldByName("f1")
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f1")
	assert.Equal(t, col.Type, BOOLEAN)
	assert.Equal(t, col.AllowNull, true)
	assert.Equal(t, col.Comment, "test_f1")
	col, err = schema.GetFieldByIndex(0)
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f1")
	assert.Equal(t, col.Type, BOOLEAN)
	assert.Equal(t, col.AllowNull, true)
	assert.Equal(t, col.Comment, "test_f1")

	col, err = schema.GetFieldByName("f2")
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f2")
	assert.Equal(t, col.Type, INTEGER)
	assert.Equal(t, col.AllowNull, false)
	assert.Equal(t, col.Comment, "test_f2")
	col, err = schema.GetFieldByIndex(1)
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f2")
	assert.Equal(t, col.Type, INTEGER)
	assert.Equal(t, col.AllowNull, false)
	assert.Equal(t, col.Comment, "test_f2")

	col, err = schema.GetFieldByName("f3")
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f3")
	assert.Equal(t, col.Type, STRING)
	assert.Equal(t, col.AllowNull, true)
	assert.Equal(t, col.Comment, "test_f3")
	col, err = schema.GetFieldByIndex(2)
	assert.Nil(t, err)
	assert.Equal(t, col.Name, "f3")
	assert.Equal(t, col.Type, STRING)
	assert.Equal(t, col.AllowNull, true)
	assert.Equal(t, col.Comment, "test_f3")

	col, err = schema.GetFieldByName("f4")
	assert.Nil(t, col)
	assert.True(t, IsFieldNotExistsError(err))
	assert.Equal(t, err.Error(), "field[f4] not exist")

	col, err = schema.GetFieldByIndex(3)
	assert.Nil(t, col)
	assert.True(t, IsFieldNotExistsError(err))
	assert.Equal(t, err.Error(), "field index[3] out of range")

	col, err = schema.GetFieldByIndex(-1)
	assert.Nil(t, col)
	assert.True(t, IsFieldNotExistsError(err))
	assert.Equal(t, err.Error(), "field index[-1] out of range")
}
