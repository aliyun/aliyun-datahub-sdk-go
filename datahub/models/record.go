package models

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

// Field
type Field struct {
	Name string          `json:"name"`
	Type types.FieldType `json:"type"`
}

// RecordSchema
type RecordSchema struct {
	Fields []Field `json:"fields"`
}

// NewRecordSchema create a new record schema for tuple record
func NewRecordSchema() *RecordSchema {
	return &RecordSchema{
		Fields: make([]Field, 0, 10),
	}
}

func (rs *RecordSchema) String() string {
	byts, _ := json.Marshal(rs)
	return string(byts)
}

// AddField add a field
func (rs *RecordSchema) AddField(f Field) *RecordSchema {
	if !types.ValidateFieldType(f.Type) {
		panic(fmt.Sprintf("field type %q illegal", f.Type))
	}
	for _, v := range rs.Fields {
		if v.Name == f.Name {
			panic(fmt.Sprintf("field %q duplicated", f.Name))
		}
	}
	rs.Fields = append(rs.Fields, f)
	return rs
}

// GetFieldIndex get index of given field
func (rs *RecordSchema) GetFieldIndex(fname string) int {
	for idx, v := range rs.Fields {
		if fname == v.Name {
			return idx
		}
	}
	return -1
}

// Size get record schema fields size
func (rs *RecordSchema) Size() int {
	return len(rs.Fields)
}

// BaseRecord
type BaseRecord struct {
	ShardId      string                 `json:"ShardId"`
	HashKey      string                 `json:"HashKey"`
	PartitionKey string                 `json:"PartitionKey"`
	Attributes   map[string]interface{} `json:"Attributes"`
}

//RecordEntry
type RecordEntry struct {
	Data interface{} `json:"Data"`
	BaseRecord
}

// IRecord record interface
type IRecord interface {
	fmt.Stringer
	GetData() interface{}
	FillData(data interface{}) error
	GetBaseRecord() BaseRecord
	SetAttribute(key string, val interface{})
}

// BlobRecord blob type record
type BlobRecord struct {
	RawData   []byte
	StoreData string
	BaseRecord
}

// NewBlobRecord new a tuple type record from given record schema
func NewBlobRecord(bytedata []byte) *BlobRecord {
	br := &BlobRecord{}
	if br != nil {
		br.RawData = bytedata
		br.StoreData = base64.StdEncoding.EncodeToString(bytedata)
	}
	br.Attributes = make(map[string]interface{})
	return br
}

func (br *BlobRecord) String() string {
	record := struct {
		Data       string                 `json:"Data"`
		Attributes map[string]interface{} `json:"Attributes"`
	}{
		Data:       br.StoreData,
		Attributes: br.Attributes,
	}
	byts, _ := json.Marshal(record)
	return string(byts)
}

// FillData implement of IRecord interface
func (br *BlobRecord) FillData(data interface{}) error {
	str, ok := data.(string)
	if !ok {
		return errors.New("data must be string")
	}
	bytedata, err := base64.StdEncoding.DecodeString(str)
	if err != nil {
		return err
	}
	br.StoreData = str
	br.RawData = bytedata
	return nil
}

// GetData implement of IRecord interface
func (br *BlobRecord) GetData() interface{} {
	return br.StoreData
}

// GetBaseRecord get base record enbry
func (br *BlobRecord) GetBaseRecord() BaseRecord {
	return br.BaseRecord
}

// SetAtbribute
func (br *BlobRecord) SetAttribute(key string, val interface{}) {
	if br.Attributes == nil {
		br.Attributes = make(map[string]interface{})
	}
	br.Attributes[key] = val
}

// TupleRecord tuple type record
type TupleRecord struct {
	RecordSchema *RecordSchema
	Values       []types.DataType
	BaseRecord
}

// NewTupleRecord new a tuple type record from given record schema
func NewTupleRecord(schema *RecordSchema) *TupleRecord {
	tr := &TupleRecord{}
	if schema != nil {
		tr.RecordSchema = schema
		tr.Values = make([]types.DataType, schema.Size())
	}
	tr.Attributes = make(map[string]interface{})
	for idx, _ := range tr.Values {
		tr.Values[idx] = nil
	}
	return tr
}

func (tr *TupleRecord) String() string {
	record := struct {
		RecordSchema *RecordSchema          `json:"RecordSchema"`
		Values       []types.DataType       `json:"Values"`
		Attributes   map[string]interface{} `json:"Attributes"`
	}{
		RecordSchema: tr.RecordSchema,
		Values:       tr.Values,
		Attributes:   tr.Attributes,
	}
	byts, _ := json.Marshal(record)
	return string(byts)
}

// SetValueByIdx set a value by idx
func (tr *TupleRecord) SetValueByIdx(idx int, val interface{}) *TupleRecord {
	if idx < 0 || idx >= tr.RecordSchema.Size() {
		panic(fmt.Sprintf("index[%d] out range", idx))
	}
	v, err := types.ValidateFieldValue(tr.RecordSchema.Fields[idx].Type, val)
	if err != nil {
		panic(err)
	}
	tr.Values[idx] = v
	return tr
}

// SetValueByName set a value by name
func (tr *TupleRecord) SetValueByName(name string, val interface{}) *TupleRecord {
	idx := tr.RecordSchema.GetFieldIndex(name)
	return tr.SetValueByIdx(idx, val)
}

func (tr *TupleRecord) GetValueByIdx(idx int) types.DataType {
	return tr.Values[idx]
}

func (tr *TupleRecord) GetValueByName(name string) types.DataType {
	idx := tr.RecordSchema.GetFieldIndex(name)
	return tr.GetValueByIdx(idx)
}

// SetValues batch set values
func (tr *TupleRecord) SetValues(values []types.DataType) *TupleRecord {
	if fsize := tr.RecordSchema.Size(); fsize != len(values) {
		panic(fmt.Sprintf("values size not match field size(field.size=%d, values.size=%d)", fsize, len(values)))
	}
	for idx, val := range values {
		v, err := types.ValidateFieldValue(tr.RecordSchema.Fields[idx].Type, val)
		if err != nil {
			panic(err)
		}
		tr.Values[idx] = v
	}
	return tr
}

// FillData implement of IRecord interface
func (tr *TupleRecord) FillData(data interface{}) error {
	datas, ok := data.([]interface{})
	if !ok {
		return errors.New("data must be array")
	} else if fsize := tr.RecordSchema.Size(); len(datas) != fsize {
		return errors.New(fmt.Sprintf("data array size not match field size(field.size=%d, values.size=%d)", fsize, len(datas)))
	}
	for idx, v := range datas {
		if v != nil {
			s, ok := v.(string)
			if !ok {
				return errors.New(fmt.Sprintf("data value type[%T] illegal", v))
			}
			tv, err := types.CastValueFromString(s, tr.RecordSchema.Fields[idx].Type)
			if err != nil {
				return err
			}
			tr.Values[idx] = tv
		}
	}
	return nil
}

// GetData implement of IRecord interface
func (tr *TupleRecord) GetData() interface{} {
	result := make([]interface{}, len(tr.Values))
	for idx, val := range tr.Values {
		if val != nil {
			result[idx] = fmt.Sprintf("%s", val)
		} else {
			result[idx] = nil
		}
	}
	return result
}

// GetBaseRecord get base record entry
func (tr *TupleRecord) GetBaseRecord() BaseRecord {
	return tr.BaseRecord
}

// SetAttribute set attribute
func (tr *TupleRecord) SetAttribute(key string, val interface{}) {
	if tr.Attributes == nil {
		tr.Attributes = make(map[string]interface{})
	}
	tr.Attributes[key] = val
}

// PutResult 数据发布结果
type PutResult struct {
	FailedRecordCount int `json:"FailedRecordCount"`
	FailedRecords     []struct {
		Index        int    `json:"Index"`
		ErrorCode    string `json:"ErrorCode"`
		ErrorMessage string `json:"ErrorMessage"`
	} `json:"FailedRecords"`
}

func (pr *PutResult) String() string {
	byts, _ := json.Marshal(pr)
	return string(byts)
}

// PutRecords 数据发布操作
type PutRecords struct {
	ProjectName string     `json:"ProjectName"`
	TopicName   string     `json:"TopicName"`
	Records     []IRecord  `json:"Records"`
	Result      *PutResult `json:"PutResult"`
}

func (pr *PutRecords) Resource(method string) string {
	return fmt.Sprintf("/projects/%s/topics/%s/shards", pr.ProjectName, pr.TopicName)
}

func (pr *PutRecords) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodPost:
		reqMsg := struct {
			Action  string        `json:"Action"`
			Records []RecordEntry `json:"Records"`
		}{
			Action:  "pub",
			Records: make([]RecordEntry, len(pr.Records)),
		}
		for idx, val := range pr.Records {
			reqMsg.Records[idx].Data = val.GetData()
			reqMsg.Records[idx].BaseRecord = val.GetBaseRecord()
		}
		return json.Marshal(reqMsg)
	default:
		return nil, errors.New(fmt.Sprintf("PutRecords not support method %s", method))
	}
}

func (pr *PutRecords) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodPost:
		if pr.Result == nil {
			pr.Result = &PutResult{}
		}
		return json.Unmarshal(body, pr.Result)
	default:
		return errors.New(fmt.Sprintf("PutRecords not support method %s", method))
	}
}

// GetResult 数据订阅结果
type GetResult struct {
	NextCursor  string    `json:"NextCursor"`
	RecordCount int       `json:"RecordCount"`
	Records     []IRecord `json:"Records"`
}

func (gr *GetResult) String() string {
	byts, _ := json.Marshal(gr)
	return string(byts)
}

// GetRecords  数据订阅操作
type GetRecords struct {
	ProjectName  string        `json:"ProjectName"`
	TopicName    string        `json:"TopicName"`
	ShardId      string        `json:"ShardId"`
	Cursor       string        `json:"Cursor"`
	Limit        int           `json:"Limit"`
	RecordSchema *RecordSchema `json:"RecordSchema"`
	Result       *GetResult    `json:"GetResult"`
}

func (gr *GetRecords) Resource(method string) string {
	return fmt.Sprintf("/projects/%s/topics/%s/shards/%s", gr.ProjectName, gr.TopicName, gr.ShardId)
}

func (gr *GetRecords) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodPost:
		reqMsg := struct {
			Action string `json:"Action"`
			Cursor string `json:"Cursor"`
			Limit  int    `json:"Limit"`
		}{
			Action: "sub",
			Cursor: gr.Cursor,
			Limit:  gr.Limit,
		}
		return json.Marshal(reqMsg)
	default:
		return nil, errors.New(fmt.Sprintf("GetRecords not support method %s", method))
	}
}

func (gr *GetRecords) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodPost:
		respMsg := struct {
			NextCursor  string `json:"NextCursor"`
			RecordCount int    `json:"RecordCount"`
			Records     []*struct {
				SystemTime int                    `json:"SystemTime"`
				Data       interface{}            `json:"Data"`
				Attributes map[string]interface{} `json:"Attributes"`
			} `json:"Records"`
		}{}
		err := json.Unmarshal(body, &respMsg)
		if err != nil {
			fmt.Printf("%v\n", err)
			return err
		}
		if gr.Result == nil {
			gr.Result = &GetResult{}
		}
		gr.Result.NextCursor = respMsg.NextCursor
		gr.Result.RecordCount = respMsg.RecordCount
		gr.Result.Records = make([]IRecord, len(respMsg.Records))
		for idx, record := range respMsg.Records {
			switch dt := record.Data.(type) {
			case []interface{}, []string:
				if gr.RecordSchema == nil {
					return errors.New("tuple record type must set record schema")
				}
				gr.Result.Records[idx] = NewTupleRecord(gr.RecordSchema)
			case string:
				gr.Result.Records[idx] = NewBlobRecord(nil)
			default:
				return errors.New(fmt.Sprintf("illegal record data type[%T]", dt))
			}
			gr.Result.Records[idx].FillData(record.Data)
			for key, val := range record.Attributes {
				gr.Result.Records[idx].SetAttribute(key, val)
			}
		}
		return nil
	default:
		return errors.New(fmt.Sprintf("GetRecords not support method %s", method))
	}
}
