package datahub

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestTupleRecordFromJson(t *testing.T) {
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

	str := "{\"f1\":false,\"f10\":\"QmbqUcPJjukbGBVtKo7hao5fqBHLFK3qROgQylKw3gW9sZ52hIzgomSF0esBZnVz\",\"f2\":14,\"f3\":20929,\"f4\":1237817139,\"f5\":6855982949904009034,\"f6\":1748599432750,\"f7\":0.7207972,\"f8\":0.3287779159869558,\"f9\":\"0.6769154\"}"
	record, err := NewTupleRecordFromJson(dhSchema, []byte(str))
	assert.Nil(t, err)
	val, _ := record.GetValueByName("f1")
	assert.Equal(t, val, Boolean(false))
	val, _ = record.GetValueByName("f2")
	assert.Equal(t, val, Tinyint(14))
	val, _ = record.GetValueByName("f2")
	assert.Equal(t, val, Tinyint(14))
	val, _ = record.GetValueByName("f3")
	assert.Equal(t, val, Smallint(20929))
	val, _ = record.GetValueByName("f4")
	assert.Equal(t, val, Integer(1237817139))
	val, _ = record.GetValueByName("f5")
	assert.Equal(t, val, Bigint(6855982949904009034))
	val, _ = record.GetValueByName("f6")
	assert.Equal(t, val, Timestamp(1748599432750))
	val, _ = record.GetValueByName("f7")
	assert.Equal(t, val, Float(0.7207972))
	val, _ = record.GetValueByName("f8")
	assert.Equal(t, val, Double(0.3287779159869558))
	val, _ = record.GetValueByName("f9")
	eval, _ := decimal.NewFromString("0.6769154")
	assert.Equal(t, val, Decimal(eval))
	val, _ = record.GetValueByName("f10")
	assert.Equal(t, val, String("QmbqUcPJjukbGBVtKo7hao5fqBHLFK3qROgQylKw3gW9sZ52hIzgomSF0esBZnVz"))
	val, _ = record.GetValueByName("f11")
	assert.Nil(t, val)
}

func TestTupleRecordFromJsonWithNotExistKey(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: INTEGER, AllowNull: true})

	// column not match
	str := "{\"f1\":false,\"f10\":\"Qmbq\"}"
	_, err := NewTupleRecordFromJson(dhSchema, []byte(str))
	assert.Equal(t, err.Error(), "field[f10] not exist")

	// ignore column not match
	record, err := NewTupleRecordFromJson(dhSchema, []byte(str), WithIgnoreNotExistKey(true))
	assert.Nil(t, err)
	val, _ := record.GetValueByName("f1")
	assert.Equal(t, val, Boolean(false))
	val, _ = record.GetValueByName("f2")
	assert.Nil(t, val)
}

func TestTupleRecordFromJsonWithFail(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: INTEGER, AllowNull: true})

	// invalid json
	str := "{\"f1\":false,\"f10\":\"Qmbq"
	_, err := NewTupleRecordFromJson(dhSchema, []byte(str))
	assert.NotNil(t, err)

	// type not match
	str = "{\"f1\":false,\"f1\":\"Qmbq\"}"
	_, err = NewTupleRecordFromJson(dhSchema, []byte(str))
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "value type[string] not match field type[BOOLEAN]")
}

func TestSetValueError(t *testing.T) {
	dhSchema := NewRecordSchema()
	dhSchema.AddField(Field{Name: "f1", Type: BOOLEAN, AllowNull: true})
	dhSchema.AddField(Field{Name: "f2", Type: INTEGER, AllowNull: true})

	record := NewTupleRecord(dhSchema)
	err := record.SetValueByIdx(3, 1)
	assert.True(t, IsFieldNotExistsError(err))

	err = record.SetValueByIdx(-1, 1)
	assert.True(t, IsFieldNotExistsError(err))

	err = record.SetValueByName("f3", 1)
	assert.True(t, IsFieldNotExistsError(err))
}
