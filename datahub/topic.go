package datahub

import (
	"encoding/json"
	"fmt"
)

type Field struct {
	Name      string    `json:"name"`
	Type      FieldType `json:"type"`
	AllowNull bool      `json:"notnull"`
	Comment   string    `json:"comment"`
}

// RecordSchema
type RecordSchema struct {
	Fields        []Field        `json:"fields"`
	fieldIndexMap map[string]int `json:"-"`
}

// NewRecordSchema create a new record schema for tuple record
func NewRecordSchema() *RecordSchema {
	return &RecordSchema{
		Fields:        make([]Field, 0),
		fieldIndexMap: make(map[string]int),
	}
}

func NewRecordSchemaFromJson(SchemaJson string) (recordSchema *RecordSchema, err error) {
	recordSchema = &RecordSchema{}
	if err = json.Unmarshal([]byte(SchemaJson), recordSchema); err != nil {
		return
	}
	for _, v := range recordSchema.Fields {
		if !validateFieldType(v.Type) {
			panic(fmt.Sprintf("field type %q illegal", v.Type))
		}
	}
	return
}

func (rs *RecordSchema) UnmarshalJSON(data []byte) error {
	schema := &struct {
		Fields []Field `json:"fields"`
	}{}
	if err := json.Unmarshal(data, schema); err != nil {
		return err
	}

	rs.fieldIndexMap = make(map[string]int)
	for _, v := range schema.Fields {
		rs.AddField(v)
	}

	return nil
}

func (rs *RecordSchema) String() string {
	type FieldHelper struct {
		Name    string    `json:"name"`
		Type    FieldType `json:"type"`
		NotNull bool      `json:"notnull,omitempty"`
		Comment string    `json:"comment,omitempty"`
	}

	fields := make([]FieldHelper, 0, rs.Size())
	for _, field := range rs.Fields {
		tmpField := FieldHelper{field.Name, field.Type, !field.AllowNull, field.Comment}
		fields = append(fields, tmpField)
	}

	tmpSchema := struct {
		Fields []FieldHelper `json:"fields"`
	}{fields}

	buf, _ := json.Marshal(tmpSchema)
	return string(buf)
}

// AddField add a field
func (rs *RecordSchema) AddField(f Field) *RecordSchema {
	if !validateFieldType(f.Type) {
		panic(fmt.Sprintf("field type %q illegal", f.Type))
	}
	for _, v := range rs.Fields {
		if v.Name == f.Name {
			panic(fmt.Sprintf("field %q duplicated", f.Name))
		}
	}
	rs.Fields = append(rs.Fields, f)
	rs.fieldIndexMap[f.Name] = len(rs.Fields) - 1
	return rs
}

// GetFieldIndex get index of given field
func (rs *RecordSchema) GetFieldIndex(fname string) int {
	if idx, ok := rs.fieldIndexMap[fname]; ok {
		return idx
	}
	return -1
}

func (rs *RecordSchema) GetFieldByIndex(idx int) (*Field, error) {
	if idx < 0 || idx >= len(rs.Fields) {
		return nil, fmt.Errorf("Invalid Filed index %d", idx)
	}

	return &rs.Fields[idx], nil
}

func (rs *RecordSchema) GetFieldByName(fname string) (*Field, error) {
	idx := rs.GetFieldIndex(fname)

	if idx == -1 {
		return nil, fmt.Errorf("field %s not exists", fname)
	}

	return rs.GetFieldByIndex(idx)
}

// Size get record schema fields size
func (rs *RecordSchema) Size() int {
	return len(rs.Fields)
}

type RecordSchemaInfo struct {
	VersionId    int          `json:"VersionId"`
	RecordSchema RecordSchema `json:"RecordSchema"`
}
