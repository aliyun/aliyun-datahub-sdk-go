package datahub

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

const (
	defaultAvroAttributeName    = "__dh_attribute__"
	defaultAvroBlobColumnName   = "data"
	defaultAvroRecordName       = "AvroRecord"
	defaultInnterAvroRecordName = "InnerAvroRecord"
)

func getAvroColumnSchema(fieldType FieldType) (avro.Schema, error) {
	switch fieldType {
	case BOOLEAN:
		return avro.NewPrimitiveSchema(avro.Boolean, nil), nil
	case TINYINT, SMALLINT, INTEGER:
		return avro.NewPrimitiveSchema(avro.Int, nil), nil
	case BIGINT, TIMESTAMP:
		return avro.NewPrimitiveSchema(avro.Long, nil), nil
	case FLOAT:
		return avro.NewPrimitiveSchema(avro.Float, nil), nil
	case DOUBLE:
		return avro.NewPrimitiveSchema(avro.Double, nil), nil
	case STRING, JSON, DECIMAL:
		return avro.NewPrimitiveSchema(avro.String, nil), nil
	default:
		// cannot reach here
		return nil, fmt.Errorf("unknown field type %s", fieldType)
	}
}

func getAvroField(name string, fieldType FieldType, allowNull bool) (*avro.Field, error) {
	schema, err := getAvroColumnSchema(fieldType)
	if err != nil {
		return nil, err
	}

	if allowNull {
		schemas := []avro.Schema{avro.NewNullSchema(), schema}
		schema, err = avro.NewUnionSchema(schemas)
		if err != nil {
			return nil, err
		}
	}

	return avro.NewField(name, schema)
}

func getAttrAvroField() (*avro.Field, error) {
	attrValueSchema, err := getAvroColumnSchema(STRING)
	if err != nil {
		return nil, err
	}

	mapSchema := avro.NewMapSchema(attrValueSchema)
	nullMapSchema := []avro.Schema{avro.NewNullSchema(), mapSchema}
	attrSchema, err := avro.NewUnionSchema(nullMapSchema)
	if err != nil {
		return nil, err
	}

	return avro.NewField(defaultAvroAttributeName, attrSchema, avro.WithDefault(nil))
}

func getTupleFields(schema *RecordSchema) ([]*avro.Field, error) {
	avroFields := make([]*avro.Field, 0)
	for _, field := range schema.Fields {
		avroField, err := getAvroField(field.Name, field.Type, field.AllowNull)
		if err != nil {
			return nil, err
		}

		avroFields = append(avroFields, avroField)
	}
	return avroFields, nil
}

func getBlobFields() ([]*avro.Field, error) {
	avroFields := make([]*avro.Field, 0)
	schema := avro.NewPrimitiveSchema(avro.Bytes, nil)
	field, err := avro.NewField(defaultAvroBlobColumnName, schema)
	if err != nil {
		return nil, err
	}
	avroFields = append(avroFields, field)
	return avroFields, nil
}

func GetAvroSchema(schema *RecordSchema) (avro.Schema, error) {
	var avroFields []*avro.Field = nil
	var err error = nil
	if schema != nil {
		avroFields, err = getTupleFields(schema)
	} else {
		avroFields, err = getBlobFields()
	}
	if err != nil {
		return nil, err
	}

	attrField, err := getAttrAvroField()
	if err != nil {
		return nil, err
	}

	avroFields = append(avroFields, attrField)
	avroSchema, err := avro.NewRecordSchema(defaultAvroRecordName, "", avroFields)
	if err != nil {
		return nil, err
	}

	return avroSchema, nil
}
