package datahub

import (
	"bytes"
	"fmt"

	"github.com/hamba/avro/v2"
	"github.com/shopspring/decimal"
)

type dataSerializer interface {
	serialize(records []IRecord) ([]byte, error)
}

func newDataSerializer(schemaCache topicSchemaCache) dataSerializer {
	return &avroDataSerializer{schemaCache: schemaCache}
}

type dataDeserializer interface {
	deserialize(data []byte, header *batchHeader) ([]IRecord, error)
}

func newDataDeserializer(schemaCache topicSchemaCache) dataDeserializer {
	return &avroDataDeserializer{schemaCache: schemaCache}
}

type avroDataSerializer struct {
	schemaCache topicSchemaCache
}

func (as *avroDataSerializer) serialize(records []IRecord) ([]byte, error) {
	avroSchema, err := as.getSchema(records[0])
	if err != nil {
		return nil, err
	}

	buffer := bytes.NewBuffer(make([]byte, 0, 16*1024))
	encoder := avro.NewEncoderForSchema(avroSchema, buffer)

	avroRecord := as.genAvroRecord(records[0])
	for _, dhRecord := range records {
		// resuse record can reduce cost of allocate memory
		as.assignRecord(dhRecord, avroRecord)
		err := encoder.Encode(avroRecord)
		if err != nil {
			return nil, err
		}
	}

	return buffer.Bytes(), nil
}

func (as *avroDataSerializer) genAvroRecord(record IRecord) map[string]any {
	avroRecord := map[string]any{}

	switch realRecord := record.(type) {
	case *TupleRecord:
		for _, field := range realRecord.RecordSchema.Fields {
			avroRecord[field.Name] = nil
		}
	case *BlobRecord:
		avroRecord[defaultAvroBlobColumnName] = nil
	}

	attrCol := map[string]any{}
	avroRecord[defaultAvroAttributeName] = attrCol
	return avroRecord
}

func (as *avroDataSerializer) setTupleRecord(dhRecord *TupleRecord, avroRecord map[string]any) {
	for _, field := range dhRecord.RecordSchema.Fields {
		val, _ := dhRecord.GetValueByName(field.Name)
		colVal := as.getColumnValue(val, field.Type)
		avroRecord[field.Name] = colVal
	}
}

func (as *avroDataSerializer) setBlobRecord(dhRecord *BlobRecord, avroRecord map[string]any) {
	avroRecord[defaultAvroBlobColumnName] = dhRecord.GetRawData()
}

func (as *avroDataSerializer) assignRecord(record IRecord, avroRecord map[string]any) map[string]any {
	switch realRecord := record.(type) {
	case *TupleRecord:
		as.setTupleRecord(realRecord, avroRecord)
	case *BlobRecord:
		as.setBlobRecord(realRecord, avroRecord)
	}

	attrCol := avroRecord[defaultAvroAttributeName].(map[string]any)
	var attrVal map[string]string = nil
	if len(record.GetAttributes()) > 0 {
		attrVal = record.GetAttributes()
	}
	attrCol["map"] = attrVal

	return avroRecord
}

func (as *avroDataSerializer) getSchema(record IRecord) (avro.Schema, error) {
	var dhSchema *RecordSchema = nil
	tupleRecord, ok := record.(*TupleRecord)
	if ok {
		dhSchema = tupleRecord.RecordSchema
	}

	schema := as.schemaCache.getAvroSchema(dhSchema)
	if schema != nil {
		return schema, nil
	}

	return nil, fmt.Errorf("cannot get avro schema")
}

func (as *avroDataSerializer) getColumnValue(data DataType, fieldType FieldType) any {
	if data == nil {
		return nil
	}

	var value any
	switch v := data.(type) {
	case Boolean:
		value = bool(v)
	case Tinyint:
		value = int32(v)
	case Smallint:
		value = int32(v)
	case Integer:
		value = int32(v)
	case Timestamp:
		value = int64(v)
	case Bigint:
		value = int64(v)
	case Float:
		value = float32(v)
	case Double:
		value = float64(v)
	default:
		value = v.String()
	}

	return value
}

type avroDataDeserializer struct {
	schemaCache topicSchemaCache
}

func (ad *avroDataDeserializer) deserialize(data []byte, header *batchHeader) ([]IRecord, error) {
	dhSchema := ad.schemaCache.getSchemaByVersionId(int(header.schemaVersion))
	avroSchema := ad.schemaCache.getAvroSchemaByVersionId(int(header.schemaVersion))

	// avro schema cannot be null
	if avroSchema == nil || (header.schemaVersion >= 0 && dhSchema == nil) {
		return nil, fmt.Errorf("cannot get schema, version:%d", header.schemaVersion)
	}

	buffer := bytes.NewBuffer(data)
	decoder := avro.NewDecoderForSchema(avroSchema, buffer)

	records := make([]IRecord, 0)
	for i := 0; i < int(header.recordCount); i++ {
		avroRecord := make(map[string]any)
		err := decoder.Decode(&avroRecord)
		if err != nil {
			return nil, err
		}

		dhRecord, err := ad.convertRecord(dhSchema, avroRecord)
		if err != nil {
			return nil, err
		}

		records = append(records, dhRecord)
	}

	return records, nil
}

func (ad *avroDataDeserializer) convertRecord(dhSchema *RecordSchema, avroRecord map[string]any) (IRecord, error) {
	if dhSchema != nil {
		return ad.convertTupleRecord(dhSchema, avroRecord)
	} else {
		return ad.convertBlobRecord(avroRecord)
	}
}

func (ad *avroDataDeserializer) convertTupleRecord(dhSchema *RecordSchema, avroRecord map[string]any) (IRecord, error) {
	dhRecord := NewTupleRecord(dhSchema)

	for _, field := range dhSchema.Fields {
		err := ad.setColumnValue(dhRecord, avroRecord[field.Name], &field)
		if err != nil {
			return nil, err
		}
	}

	err := ad.convertAttribute(avroRecord, dhRecord)
	if err != nil {
		return nil, err
	}

	return dhRecord, nil
}

func (ad *avroDataDeserializer) setColumnValue(dhRecord *TupleRecord, val any, field *Field) error {
	if val == nil {
		dhRecord.SetValueByName(field.Name, nil)
		return nil
	}

	switch field.Type {
	case DECIMAL:
		tmp, err := decimal.NewFromString(val.(string))
		if err != nil {
			return err
		}
		dhRecord.SetValueByName(field.Name, tmp)
	default:
		dhRecord.SetValueByName(field.Name, val)
	}
	return nil
}

func (ad *avroDataDeserializer) convertBlobRecord(avroRecord map[string]any) (IRecord, error) {
	data, ok := avroRecord[defaultAvroBlobColumnName]
	if !ok {
		return nil, fmt.Errorf("cannot get blob data")
	}

	rawData, ok := data.([]byte)
	if !ok {
		return nil, fmt.Errorf("blob data is not []byte")
	}

	dhRecord := NewBlobRecord(rawData)

	err := ad.convertAttribute(avroRecord, dhRecord)
	if err != nil {
		return nil, err
	}

	return dhRecord, nil
}

func (ad *avroDataDeserializer) convertAttribute(avroRecord map[string]any, dhRecord IRecord) error {
	attrCol, ok := avroRecord[defaultAvroAttributeName]
	if !ok || attrCol == nil {
		return nil
	}

	unionAttr, ok := attrCol.(map[string]any)
	if !ok {
		return fmt.Errorf("attribute column is not map[string]any")
	}

	if len(unionAttr) == 0 {
		return nil
	}

	attrVal, ok := unionAttr["map"]
	if !ok {
		return nil
	}

	attrMap, ok := attrVal.(map[string]any)
	if !ok {
		return fmt.Errorf("attribute value is not map[string]string")
	}

	for k, v := range attrMap {
		dhRecord.SetAttribute(k, v.(string))
	}
	return nil
}
