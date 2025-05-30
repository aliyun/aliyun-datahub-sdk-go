package datahub

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

type Field struct {
	Name      string    `json:"name"`
	Type      FieldType `json:"type"`
	AllowNull bool      `json:"notnull"` // Double negation is hard to understand, allownull is easier to understand
	Comment   string    `json:"comment"`
}

func NewField(name string, Type FieldType) *Field {
	return &Field{
		Name:      name,
		Type:      Type,
		AllowNull: true,
		Comment:   "",
	}
}

func NewFieldWithProp(name string, Type FieldType, allowNull bool, comment string) *Field {
	return &Field{
		Name:      name,
		Type:      Type,
		AllowNull: allowNull,
		Comment:   comment,
	}
}

// RecordSchema
type RecordSchema struct {
	Fields        []Field        `json:"fields"`
	fieldIndexMap map[string]int `json:"-"`
	hashVal       uint32
}

// NewRecordSchema create a new record schema for tuple record
func NewRecordSchema() *RecordSchema {
	return &RecordSchema{
		Fields:        make([]Field, 0),
		fieldIndexMap: make(map[string]int),
		hashVal:       0,
	}
}

func NewRecordSchemaFromJson(SchemaJson string) (recordSchema *RecordSchema, err error) {
	recordSchema = &RecordSchema{}
	if err = json.Unmarshal([]byte(SchemaJson), recordSchema); err != nil {
		return
	}
	for _, v := range recordSchema.Fields {
		if !validateFieldType(v.Type) {
			return nil, fmt.Errorf("field type %q illegal", v.Type)
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
		if err := rs.AddField(v); err != nil {
			return err
		}
	}

	return nil
}

func (rs *RecordSchema) HashCode() uint32 {
	return rs.hashCode()
}

func (rs *RecordSchema) hashCode() uint32 {
	if val := atomic.LoadUint32(&rs.hashVal); val != 0 {
		return val

	}

	schemaStr := rs.String()
	newVal, err := calculateHashCode(schemaStr)
	if err != nil {
		log.Warnf("Calculate hash code failed, schema:%s, error:%v", schemaStr, err)
	}

	if atomic.CompareAndSwapUint32(&rs.hashVal, 0, newVal) && log.IsLevelEnabled(log.DebugLevel) {
		log.Debugf("Calculate hash code success schema:%s, code:%d", schemaStr, newVal)
		return newVal
	} else {
		return atomic.LoadUint32(&rs.hashVal)
	}
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
func (rs *RecordSchema) AddField(f Field) error {
	if !validateFieldType(f.Type) {
		return fmt.Errorf("field type %q illegal", f.Type)
	}

	f.Name = strings.ToLower(f.Name)
	_, exists := rs.fieldIndexMap[f.Name]
	if exists {
		return fmt.Errorf("field %s duplicated", f.Name)
	}

	rs.Fields = append(rs.Fields, f)
	rs.fieldIndexMap[f.Name] = len(rs.Fields) - 1
	return nil
}

// GetFieldIndex get index of given field
func (rs *RecordSchema) GetFieldIndex(fname string) int {
	name := strings.ToLower(fname)
	if idx, ok := rs.fieldIndexMap[name]; ok {
		return idx
	}
	return -1
}

func (rs *RecordSchema) GetFieldByIndex(idx int) (*Field, error) {
	if idx < 0 || idx >= len(rs.Fields) {
		return nil, newFieldNotExistsError(fmt.Sprintf("field index[%d] out of range", idx))
	}

	return &rs.Fields[idx], nil
}

func (rs *RecordSchema) GetFieldByName(fname string) (*Field, error) {
	idx := rs.GetFieldIndex(strings.ToLower(fname))

	if idx == -1 {
		return nil, newFieldNotExistsError(fmt.Sprintf("field[%s] not exist", fname))
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
