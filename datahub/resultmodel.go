package datahub

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/pbmodel"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/util"
)

// for the common response and detect error
type CommonResponseResult struct {
	// StatusCode http return code
	StatusCode int

	// Request body raw size
	RawSize int

	// Request body size after compress
	ReqSize int

	// RequestId examples request id return by server
	RequestId string
}

func newCommonResponseResult(code int, header *http.Header, body []byte) (*CommonResponseResult, error) {
	result := &CommonResponseResult{
		StatusCode: code,
		RequestId:  header.Get(httpHeaderRequestId),
	}
	var err error
	switch {
	case code >= 400:
		var datahubErr DatahubError
		if err = json.Unmarshal(body, &datahubErr); err != nil {
			return nil, err
		}
		err = errorHandler(code, result.RequestId, datahubErr.Code, datahubErr.Message)
	default:
		err = nil
	}
	return result, err
}

// the result of ListProject
type ListProjectResult struct {
	CommonResponseResult
	ProjectNames []string `json:"ProjectNames"`
}

// convert the response body to ListProjectResult
func newListProjectResult(data []byte, commonResp *CommonResponseResult) (*ListProjectResult, error) {
	lpr := &ListProjectResult{
		CommonResponseResult: *commonResp,
		ProjectNames:         make([]string, 0),
	}
	if err := json.Unmarshal(data, lpr); err != nil {
		return nil, err
	}
	return lpr, nil
}

type CreateProjectResult struct {
	CommonResponseResult
}

func newCreateProjectResult(commonResp *CommonResponseResult) (*CreateProjectResult, error) {
	cpr := &CreateProjectResult{
		CommonResponseResult: *commonResp,
	}
	return cpr, nil
}

type UpdateProjectResult struct {
	CommonResponseResult
}

func newUpdateProjectResult(commonResp *CommonResponseResult) (*UpdateProjectResult, error) {
	upr := &UpdateProjectResult{
		CommonResponseResult: *commonResp,
	}
	return upr, nil
}

type DeleteProjectResult struct {
	CommonResponseResult
}

func newDeleteProjectResult(commonResp *CommonResponseResult) (*DeleteProjectResult, error) {
	dpr := &DeleteProjectResult{
		CommonResponseResult: *commonResp,
	}
	return dpr, nil
}

// the result of GetProject
type GetProjectResult struct {
	CommonResponseResult
	ProjectName    string
	CreateTime     int64  `json:"CreateTime"`
	LastModifyTime int64  `json:"LastModifyTime"`
	Comment        string `json:"Comment"`
}

// convert the response body to GetProjectResult
func newGetProjectResult(data []byte, commonResp *CommonResponseResult) (*GetProjectResult, error) {
	gpr := &GetProjectResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gpr); err != nil {
		return nil, err
	}
	return gpr, nil
}

type UpdateProjectVpcWhitelistResult struct {
	CommonResponseResult
}

func newUpdateProjectVpcWhitelistResult(commonResp *CommonResponseResult) (*UpdateProjectVpcWhitelistResult, error) {
	upvw := &UpdateProjectVpcWhitelistResult{
		CommonResponseResult: *commonResp,
	}
	return upvw, nil
}

type ListTopicResult struct {
	CommonResponseResult
	TopicNames []string `json:"TopicNames"`
}

func newListTopicResult(data []byte, commonResp *CommonResponseResult) (*ListTopicResult, error) {
	lt := &ListTopicResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, lt); err != nil {
		return nil, err
	}
	return lt, nil
}

type CreateBlobTopicResult struct {
	CommonResponseResult
}

func newCreateBlobTopicResult(commonResp *CommonResponseResult) (*CreateBlobTopicResult, error) {
	cbrt := &CreateBlobTopicResult{
		CommonResponseResult: *commonResp,
	}
	return cbrt, nil
}

type CreateTupleTopicResult struct {
	CommonResponseResult
}

func newCreateTupleTopicResult(commonResp *CommonResponseResult) (*CreateTupleTopicResult, error) {
	cttr := &CreateTupleTopicResult{
		CommonResponseResult: *commonResp,
	}
	return cttr, nil
}

type CreateTopicWithParaResult struct {
	CommonResponseResult
}

func newCreateTopicWithParaResult(commonResp *CommonResponseResult) (*CreateTopicWithParaResult, error) {
	ctwp := &CreateTopicWithParaResult{
		CommonResponseResult: *commonResp,
	}
	return ctwp, nil
}

type UpdateTopicResult struct {
	CommonResponseResult
}

func newUpdateTopicResult(commonResp *CommonResponseResult) (*UpdateTopicResult, error) {
	utr := &UpdateTopicResult{
		CommonResponseResult: *commonResp,
	}
	return utr, nil
}

type DeleteTopicResult struct {
	CommonResponseResult
}

func newDeleteTopicResult(commonResp *CommonResponseResult) (*DeleteTopicResult, error) {
	dtr := &DeleteTopicResult{
		CommonResponseResult: *commonResp,
	}
	return dtr, nil
}

type topicExtraConfig struct {
	protocol           Protocol
	compressType       CompressorType
	listShardInterval  time.Duration
	listSchemaInterval time.Duration
}

func defaultTopicExtraConfig() topicExtraConfig {
	return topicExtraConfig{
		protocol:           unknownProtocol,
		compressType:       NOCOMPRESS,
		listShardInterval:  time.Minute * 1,
		listSchemaInterval: time.Minute * 1,
	}
}

type GetTopicResult struct {
	CommonResponseResult
	ProjectName    string
	TopicName      string
	ShardCount     int              `json:"ShardCount"`
	LifeCycle      int              `json:"LifeCycle"`
	RecordType     RecordType       `json:"RecordType"`
	RecordSchema   *RecordSchema    `json:"RecordSchema"`
	Comment        string           `json:"Comment"`
	CreateTime     int64            `json:"CreateTime"`
	LastModifyTime int64            `json:"LastModifyTime"`
	TopicStatus    TopicStatus      `json:"Status"`
	ExpandMode     ExpandMode       `json:"ExpandMode"`
	EnableSchema   bool             `json:"EnableSchemaRegistry"`
	extraConfig    topicExtraConfig `json:"-"`
}

// for deserialize the RecordSchema
func (gtr *GetTopicResult) UnmarshalJSON(data []byte) error {
	msg := &struct {
		ShardCount     int               `json:"ShardCount"`
		LifeCycle      int               `json:"LifeCycle"`
		RecordType     RecordType        `json:"RecordType"`
		RecordSchema   string            `json:"RecordSchema"`
		Comment        string            `json:"Comment"`
		CreateTime     int64             `json:"CreateTime"`
		LastModifyTime int64             `json:"LastModifyTime"`
		TopicStatus    TopicStatus       `json:"Status"`
		ExpandMode     ExpandMode        `json:"ExpandMode"`
		EnableSchema   bool              `json:"EnableSchemaRegistry"`
		ExtraConfig    map[string]string `json:"ExtraConfig"`
	}{}
	if err := json.Unmarshal(data, msg); err != nil {
		return err
	}

	gtr.ShardCount = msg.ShardCount
	gtr.LifeCycle = msg.LifeCycle
	gtr.RecordType = msg.RecordType
	gtr.Comment = msg.Comment
	gtr.CreateTime = msg.CreateTime
	gtr.LastModifyTime = msg.LastModifyTime
	gtr.TopicStatus = msg.TopicStatus
	gtr.ExpandMode = msg.ExpandMode
	gtr.EnableSchema = msg.EnableSchema
	if msg.RecordType == TUPLE {
		rs := &RecordSchema{}
		if err := json.Unmarshal([]byte(msg.RecordSchema), rs); err != nil {
			return err
		}
		for idx := range rs.Fields {
			rs.Fields[idx].AllowNull = !rs.Fields[idx].AllowNull
		}
		gtr.RecordSchema = rs
	}

	gtr.extraConfig = defaultTopicExtraConfig()
	val, ok := msg.ExtraConfig["DataProtocolType"]
	if ok {
		if val != "" {
			if strings.ToUpper(val) == "BATCH" {
				gtr.extraConfig.protocol = Batch
			} else {
				gtr.extraConfig.protocol = Protobuf
			}
		}
	}

	val, ok = msg.ExtraConfig["CompressType"]
	if ok {
		gtr.extraConfig.compressType = parseCompressType(val)
	}

	val, ok = msg.ExtraConfig["ListShardInterval"]
	if ok && val != "" {
		ival, err := strconv.Atoi(val)
		if err != nil {
			gtr.extraConfig.listShardInterval = time.Millisecond * time.Duration(ival)
		}
	}

	val, ok = msg.ExtraConfig["ListSchemaInterval"]
	if ok && val != "" {
		ival, err := strconv.Atoi(val)
		if err != nil {
			gtr.extraConfig.listSchemaInterval = time.Millisecond * time.Duration(ival)
		}
	}

	return nil
}

func newGetTopicResult(data []byte, commonResp *CommonResponseResult) (*GetTopicResult, error) {
	gr := &GetTopicResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gr); err != nil {
		return nil, err
	}
	return gr, nil
}

type ListShardResult struct {
	CommonResponseResult
	Shards     []ShardEntry `json:"Shards"`
	Protocol   string       `json:"Protocol"`
	IntervalMs int64        `json:"Interval"`
}

func newListShardResult(data []byte, commonResp *CommonResponseResult) (*ListShardResult, error) {
	lsr := &ListShardResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, lsr); err != nil {
		return nil, err
	}
	return lsr, nil
}

type SplitShardResult struct {
	CommonResponseResult
	NewShards []ShardEntry `json:"NewShards"`
}

func newSplitShardResult(data []byte, commonResp *CommonResponseResult) (*SplitShardResult, error) {
	ssr := &SplitShardResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ssr); err != nil {
		return nil, err
	}
	return ssr, nil
}

type MergeShardResult struct {
	CommonResponseResult
	ShardId      string `json:"ShardId"`
	BeginHashKey string `json:"BeginHashKey"`
	EndHashKey   string `json:"EndHashKey"`
}

func newMergeShardResult(data []byte, commonResp *CommonResponseResult) (*MergeShardResult, error) {
	ssr := &MergeShardResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ssr); err != nil {
		return nil, err
	}
	return ssr, nil
}

type ExtendShardResult struct {
	CommonResponseResult
}

func newExtendShardResult(commonResp *CommonResponseResult) (*ExtendShardResult, error) {
	esr := &ExtendShardResult{
		CommonResponseResult: *commonResp,
	}
	return esr, nil
}

type GetCursorResult struct {
	CommonResponseResult
	Cursor     string `json:"Cursor"`
	RecordTime int64  `json:"RecordTime"`
	Sequence   int64  `json:"Sequence"`
}

func newGetCursorResult(data []byte, commonResp *CommonResponseResult) (*GetCursorResult, error) {
	gcr := &GetCursorResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gcr); err != nil {
		return nil, err
	}
	return gcr, nil
}

type PutRecordsResult struct {
	CommonResponseResult
	FailedRecordCount int            `json:"FailedRecordCount"`
	FailedRecords     []FailedRecord `json:"FailedRecords"`
}

func newPutRecordsResult(data []byte, commonResp *CommonResponseResult) (*PutRecordsResult, error) {
	prr := &PutRecordsResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, prr); err != nil {
		return nil, err
	}
	return prr, nil
}

func newPutPBRecordsResult(data []byte, commonResp *CommonResponseResult) (*PutRecordsResult, error) {
	pr := &PutRecordsResult{
		CommonResponseResult: *commonResp,
	}
	data, err := util.UnwrapMessage(data)
	if err != nil {
		return nil, err
	}
	prr := &pbmodel.PutRecordsResponse{}
	if err := proto.Unmarshal(data, prr); err != nil {
		return nil, err
	}

	pr.FailedRecordCount = int(*prr.FailedCount)
	if pr.FailedRecordCount > 0 {
		records := make([]FailedRecord, pr.FailedRecordCount)
		for idx, v := range prr.FailedRecords {
			records[idx].ErrorCode = *v.ErrorCode
			records[idx].ErrorMessage = *v.ErrorMessage
			records[idx].Index = int(*v.Index)
		}
		pr.FailedRecords = records
	}
	return pr, nil
}

type PutRecordsByShardResult struct {
	CommonResponseResult
}

func newPutRecordsByShardResult(commonResp *CommonResponseResult) (*PutRecordsByShardResult, error) {
	prbs := &PutRecordsByShardResult{
		CommonResponseResult: *commonResp,
	}
	return prbs, nil
}

type GetRecordsResult struct {
	CommonResponseResult
	NextCursor     string        `json:"NextCursor"`
	RecordCount    int           `json:"RecordCount"`
	StartSequence  int64         `json:"StartSeq"`
	LatestSequence int64         `json:"LatestSeq"`
	LatestTime     int64         `json:"LatestTime"`
	Records        []IRecord     `json:"Records"`
	RecordSchema   *RecordSchema `json:"-"`
}

func (grr *GetRecordsResult) UnmarshalJSON(data []byte) error {
	msg := &struct {
		NextCursor     string `json:"NextCursor"`
		RecordCount    int    `json:"RecordCount"`
		StartSequence  int64  `json:"StartSeq"`
		LatestSequence int64  `json:"LatestSeq"`
		LatestTime     int64  `json:"LatestTime"`
		Records        []*struct {
			SystemTime    int64             `json:"SystemTime"`
			NextCursor    string            `json:"NextCursor"`
			CurrentCursor string            `json:"Cursor"`
			Sequence      int64             `json:"Sequence"`
			Attributes    map[string]string `json:"Attributes"`
			Data          interface{}       `json:"Data"`
		} `json:"Records"`
	}{}
	err := json.Unmarshal(data, msg)
	if err != nil {
		return err
	}
	grr.NextCursor = msg.NextCursor
	grr.RecordCount = msg.RecordCount
	grr.StartSequence = msg.StartSequence
	grr.LatestSequence = msg.LatestSequence
	grr.LatestTime = msg.LatestTime
	grr.Records = make([]IRecord, len(msg.Records))
	for idx, record := range msg.Records {
		if record.Data == nil {
			return fmt.Errorf("invalid record response, record data is nil")
		}

		switch dt := record.Data.(type) {
		case []interface{}, []string:
			if grr.RecordSchema == nil {
				return fmt.Errorf("tuple record type must set record schema")
			}
			grr.Records[idx] = NewTupleRecord(grr.RecordSchema)
		case string:
			grr.Records[idx] = NewBlobRecord([]byte(dt))
		default:
			return fmt.Errorf("illegal record data type[%T]", dt)
		}
		if err := grr.Records[idx].fillData(record.Data); err != nil {
			return err
		}
		for key, val := range record.Attributes {
			grr.Records[idx].SetAttribute(key, val)
		}
		br := BaseRecord{
			SystemTime: msg.Records[idx].SystemTime,
			NextCursor: msg.Records[idx].NextCursor,
			Cursor:     msg.Records[idx].CurrentCursor,
			Sequence:   msg.Records[idx].Sequence,
			Attributes: msg.Records[idx].Attributes,
		}
		grr.Records[idx].SetBaseRecord(br)
	}
	return nil
}

func newGetRecordsResult(data []byte, schema *RecordSchema, commonResp *CommonResponseResult) (*GetRecordsResult, error) {
	grr := &GetRecordsResult{
		CommonResponseResult: *commonResp,
		RecordSchema:         schema,
	}
	if err := json.Unmarshal(data, grr); err != nil {
		return nil, err
	}
	return grr, nil
}

func newGetPBRecordsResult(data []byte, schema *RecordSchema, commonResp *CommonResponseResult) (*GetRecordsResult, error) {
	data, err := util.UnwrapMessage(data)
	if err != nil {
		return nil, err
	}
	grr := &pbmodel.GetRecordsResponse{}
	if err := proto.Unmarshal(data, grr); err != nil {
		return nil, err
	}

	result := &GetRecordsResult{
		CommonResponseResult: *commonResp,
		RecordSchema:         schema,
	}
	if grr.NextCursor != nil {
		result.NextCursor = *(grr.NextCursor)
	}
	if grr.StartSequence != nil {
		result.StartSequence = *grr.StartSequence
	}
	if grr.LatestSequence != nil {
		result.LatestSequence = *grr.LatestSequence
	}
	if grr.LatestTime != nil {
		result.LatestTime = *grr.LatestTime
	}
	if grr.RecordCount != nil {
		result.RecordCount = int(*grr.RecordCount)
		if result.RecordCount > 0 {
			result.Records = make([]IRecord, result.RecordCount)
			for idx, record := range grr.Records {
				//Tuple topic
				if result.RecordSchema != nil {
					tr := NewTupleRecord(result.RecordSchema)
					if err := fillTupleData(tr, record); err != nil {
						return nil, err
					}
					result.Records[idx] = tr
				} else {
					br := NewBlobRecord(record.Data.Data[0].Value)
					if err := fillBlobData(br, record); err != nil {
						return nil, err
					}
					result.Records[idx] = br
				}
			}
		}
	}
	return result, nil
}

func fillTupleData(tr *TupleRecord, recordEntry *pbmodel.RecordEntry) error {
	if recordEntry.ShardId != nil {
		tr.ShardId = *recordEntry.ShardId
	}
	if recordEntry.HashKey != nil {
		tr.HashKey = *recordEntry.HashKey
	}
	if recordEntry.PartitionKey != nil {
		tr.Sequence = *recordEntry.Sequence
	}
	if recordEntry.Cursor != nil {
		tr.Cursor = *recordEntry.Cursor
	}
	if recordEntry.NextCursor != nil {
		tr.NextCursor = *recordEntry.NextCursor
	}
	if recordEntry.Sequence != nil {
		tr.Sequence = *recordEntry.Sequence
	}
	if recordEntry.SystemTime != nil {
		tr.SystemTime = *recordEntry.SystemTime
	}
	if recordEntry.Attributes != nil {
		for _, pair := range recordEntry.Attributes.Attributes {
			tr.Attributes[*pair.Key] = *pair.Value
		}
	}
	data := recordEntry.Data.Data

	for idx, v := range data {
		if v.Value != nil {
			tv, err := castValueFromString(string(v.Value), tr.RecordSchema.Fields[idx].Type)
			if err != nil {
				return err
			}
			tr.Values[idx] = tv
		}
	}
	return nil
}

func fillBlobData(br *BlobRecord, recordEntry *pbmodel.RecordEntry) error {
	if recordEntry.ShardId != nil {
		br.ShardId = *recordEntry.ShardId
	}
	if recordEntry.HashKey != nil {
		br.HashKey = *recordEntry.HashKey
	}
	if recordEntry.PartitionKey != nil {
		br.Sequence = *recordEntry.Sequence
	}
	if recordEntry.Cursor != nil {
		br.Cursor = *recordEntry.Cursor
	}
	if recordEntry.NextCursor != nil {
		br.NextCursor = *recordEntry.NextCursor
	}
	if recordEntry.Sequence != nil {
		br.Sequence = *recordEntry.Sequence
	}
	if recordEntry.SystemTime != nil {
		br.SystemTime = *recordEntry.SystemTime
	}
	if recordEntry.Attributes != nil {
		for _, pair := range recordEntry.Attributes.Attributes {
			br.Attributes[*pair.Key] = *pair.Value
		}
	}
	br.RawData = recordEntry.Data.Data[0].Value
	return nil
}

func newGetBatchRecordsResult(data []byte, schema *RecordSchema, commonResp *CommonResponseResult, deserializer *batchDeserializer) (*GetRecordsResult, error) {
	data, err := util.UnwrapMessage(data)
	if err != nil {
		return nil, err
	}
	gbr := &pbmodel.GetBinaryRecordsResponse{}
	if err := proto.Unmarshal(data, gbr); err != nil {
		return nil, err
	}

	result := &GetRecordsResult{
		CommonResponseResult: *commonResp,
		RecordSchema:         schema,
	}

	if gbr.NextCursor != nil {
		result.NextCursor = *(gbr.NextCursor)
	}
	if gbr.StartSequence != nil {
		result.StartSequence = *gbr.StartSequence
	}
	if gbr.LatestSequence != nil {
		result.LatestSequence = *gbr.LatestSequence
	}
	if gbr.LatestTime != nil {
		result.LatestTime = *gbr.LatestTime
	}

	// 这里的RecordCount不是record数量，而是batch的数量
	if gbr.RecordCount != nil {
		if *gbr.RecordCount > 0 {
			result.Records = make([]IRecord, 0, *gbr.RecordCount)
			for _, record := range gbr.Records {
				meta := &respMeta{
					cursor:     record.GetCursor(),
					nextCursor: record.GetNextCursor(),
					sequence:   record.GetSequence(),
					systemTime: record.GetSystemTime(),
					serial:     int64(record.GetSerial()),
				}

				recordList, err := deserializer.deserialize(record.Data, meta)
				if err != nil {
					return nil, err
				}
				result.Records = append(result.Records, recordList...)
			}
		}
	}
	result.RecordCount = len(result.Records)
	return result, nil
}

type AppendFieldResult struct {
	CommonResponseResult
}

func newAppendFieldResult(commonResp *CommonResponseResult) (*AppendFieldResult, error) {
	afr := &AppendFieldResult{
		CommonResponseResult: *commonResp,
	}
	return afr, nil
}

type GetMeterInfoResult struct {
	CommonResponseResult
	ActiveTime int64 `json:"ActiveTime"`
	Storage    int64 `json:"Storage"`
}

func newGetMeterInfoResult(data []byte, commonResp *CommonResponseResult) (*GetMeterInfoResult, error) {
	gmir := &GetMeterInfoResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gmir); err != nil {
		return nil, err
	}
	return gmir, nil
}

type ListConnectorResult struct {
	CommonResponseResult
	ConnectorIds []string `json:"Connectors"`
}

func newListConnectorResult(data []byte, commonResp *CommonResponseResult) (*ListConnectorResult, error) {
	lcr := &ListConnectorResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, lcr); err != nil {
		return nil, err
	}
	return lcr, nil
}

type CreateConnectorResult struct {
	CommonResponseResult
	ConnectorId string `json:"ConnectorId"`
}

func newCreateConnectorResult(data []byte, commonResp *CommonResponseResult) (*CreateConnectorResult, error) {
	ccr := &CreateConnectorResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ccr); err != nil {
		return nil, err
	}
	return ccr, nil
}

type UpdateConnectorResult struct {
	CommonResponseResult
}

func newUpdateConnectorResult(commonResp *CommonResponseResult) (*UpdateConnectorResult, error) {
	ucr := &UpdateConnectorResult{
		CommonResponseResult: *commonResp,
	}
	return ucr, nil
}

type DeleteConnectorResult struct {
	CommonResponseResult
}

func newDeleteConnectorResult(commonResp *CommonResponseResult) (*DeleteConnectorResult, error) {
	dcr := &DeleteConnectorResult{
		CommonResponseResult: *commonResp,
	}
	return dcr, nil
}

type GetConnectorResult struct {
	CommonResponseResult
	CreateTime     int64             `json:"CreateTime"`
	LastModifyTime int64             `json:"LastModifyTime"`
	ConnectorId    string            `json:"ConnectorId"`
	ClusterAddress string            `json:"ClusterAddress"`
	Type           ConnectorType     `json:"Type"`
	State          ConnectorState    `json:"State"`
	ColumnFields   []string          `json:"ColumnFields"`
	ExtraConfig    map[string]string `json:"ExtraInfo"`
	Creator        string            `json:"Creator"`
	Owner          string            `json:"Owner"`
	Config         interface{}       `json:"Config"`
}

func newGetConnectorResult(data []byte, commonResp *CommonResponseResult) (*GetConnectorResult, error) {
	cType := &struct {
		Type ConnectorType `json:"Type"`
	}{}
	if err := json.Unmarshal(data, cType); err != nil {
		return nil, err
	}

	switch cType.Type {
	case SinkOdps:
		return unmarshalGetOdpsConnector(commonResp, data)
	case SinkOss:
		return unmarshalGetOssConnector(commonResp, data)
	case SinkEs:
		return unmarshalGetEsConnector(commonResp, data)
	case SinkAds:
		return unmarshalGetAdsConnector(commonResp, data)
	case SinkMysql:
		return unmarshalGetMysqlConnector(commonResp, data)
	case SinkFc:
		return unmarshalGetFcConnector(commonResp, data)
	case SinkOts:
		return unmarshalGetOtsConnector(commonResp, data)
	case SinkDatahub:
		return unmarshalGetDatahubConnector(commonResp, data)
	case SinkHologres:
		return unmarshalGetHologresConnector(commonResp, data)
	default:
		return nil, fmt.Errorf("not support connector type %s", cType.Type.String())
	}
}

type GetConnectorDoneTimeResult struct {
	CommonResponseResult
	DoneTime   int64  `json:"DoneTime"`
	TimeZone   string `json:"TimeZone"`
	TimeWindow int    `json:"TimeWindow"`
}

func newGetConnectorDoneTimeResult(data []byte, commonResp *CommonResponseResult) (*GetConnectorDoneTimeResult, error) {
	gcdt := &GetConnectorDoneTimeResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gcdt); err != nil {
		return nil, err
	}
	return gcdt, nil
}

type GetConnectorShardStatusResult struct {
	CommonResponseResult
	ShardStatus map[string]ConnectorShardStatusEntry `json:"ShardStatusInfos"`
}

func newGetConnectorShardStatusResult(data []byte, commonResp *CommonResponseResult) (*GetConnectorShardStatusResult, error) {
	gcss := &GetConnectorShardStatusResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gcss); err != nil {
		return nil, err
	}
	return gcss, nil
}

type GetConnectorShardStatusByShardResult struct {
	CommonResponseResult
	ConnectorShardStatusEntry
}

func newGetConnectorShardStatusByShardResult(data []byte, commonResp *CommonResponseResult) (*GetConnectorShardStatusByShardResult, error) {
	csse := &ConnectorShardStatusEntry{}
	if err := json.Unmarshal(data, csse); err != nil {
		return nil, err
	}

	gcss := &GetConnectorShardStatusByShardResult{
		CommonResponseResult:      *commonResp,
		ConnectorShardStatusEntry: *csse,
	}
	return gcss, nil
}

type ReloadConnectorResult struct {
	CommonResponseResult
}

func newReloadConnectorResult(commonResp *CommonResponseResult) (*ReloadConnectorResult, error) {
	rcr := &ReloadConnectorResult{
		CommonResponseResult: *commonResp,
	}
	return rcr, nil
}

type ReloadConnectorByShardResult struct {
	CommonResponseResult
}

func newReloadConnectorByShardResult(commonResp *CommonResponseResult) (*ReloadConnectorByShardResult, error) {
	rcsr := &ReloadConnectorByShardResult{
		CommonResponseResult: *commonResp,
	}
	return rcsr, nil
}

type UpdateConnectorStateResult struct {
	CommonResponseResult
}

func newUpdateConnectorStateResult(commonResp *CommonResponseResult) (*UpdateConnectorStateResult, error) {
	ucsr := &UpdateConnectorStateResult{
		CommonResponseResult: *commonResp,
	}
	return ucsr, nil
}

type UpdateConnectorOffsetResult struct {
	CommonResponseResult
}

func newUpdateConnectorOffsetResult(commonResp *CommonResponseResult) (*UpdateConnectorOffsetResult, error) {
	ucor := &UpdateConnectorOffsetResult{
		CommonResponseResult: *commonResp,
	}
	return ucor, nil
}

type AppendConnectorFieldResult struct {
	CommonResponseResult
}

func newAppendConnectorFieldResult(commonResp *CommonResponseResult) (*AppendConnectorFieldResult, error) {
	acfr := &AppendConnectorFieldResult{
		CommonResponseResult: *commonResp,
	}
	return acfr, nil
}

type ListSubscriptionResult struct {
	CommonResponseResult
	TotalCount    int64               `json:"TotalCount"`
	Subscriptions []SubscriptionEntry `json:"Subscriptions"`
}

func newListSubscriptionResult(data []byte, commonResp *CommonResponseResult) (*ListSubscriptionResult, error) {
	lsr := &ListSubscriptionResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, lsr); err != nil {
		return nil, err
	}
	return lsr, nil
}

type CreateSubscriptionResult struct {
	CommonResponseResult
	SubId string `json:"SubId"`
}

func newCreateSubscriptionResult(data []byte, commonResp *CommonResponseResult) (*CreateSubscriptionResult, error) {
	csr := &CreateSubscriptionResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, csr); err != nil {
		return nil, err
	}
	return csr, nil
}

type UpdateSubscriptionResult struct {
	CommonResponseResult
}

func newUpdateSubscriptionResult(commonResp *CommonResponseResult) (*UpdateSubscriptionResult, error) {
	usr := &UpdateSubscriptionResult{
		CommonResponseResult: *commonResp,
	}
	return usr, nil
}

type DeleteSubscriptionResult struct {
	CommonResponseResult
}

func newDeleteSubscriptionResult(commonResp *CommonResponseResult) (*DeleteSubscriptionResult, error) {
	dsr := &DeleteSubscriptionResult{
		CommonResponseResult: *commonResp,
	}
	return dsr, nil
}

type GetSubscriptionResult struct {
	CommonResponseResult
	SubscriptionEntry
}

func newGetSubscriptionResult(data []byte, commonResp *CommonResponseResult) (*GetSubscriptionResult, error) {
	gsr := &GetSubscriptionResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gsr); err != nil {
		return nil, err
	}
	return gsr, nil
}

type UpdateSubscriptionStateResult struct {
	CommonResponseResult
}

func newUpdateSubscriptionStateResult(commonResp *CommonResponseResult) (*UpdateSubscriptionStateResult, error) {
	ussr := &UpdateSubscriptionStateResult{
		CommonResponseResult: *commonResp,
	}
	return ussr, nil
}

type OpenSubscriptionSessionResult struct {
	CommonResponseResult
	Offsets map[string]SubscriptionOffset `json:"Offsets"`
}

func newOpenSubscriptionSessionResult(data []byte, commonResp *CommonResponseResult) (*OpenSubscriptionSessionResult, error) {
	ossr := &OpenSubscriptionSessionResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ossr); err != nil {
		return nil, err
	}
	return ossr, nil
}

type GetSubscriptionOffsetResult struct {
	CommonResponseResult
	Offsets map[string]SubscriptionOffset `json:"Offsets"`
}

func newGetSubscriptionOffsetResult(data []byte, commonResp *CommonResponseResult) (*GetSubscriptionOffsetResult, error) {
	gsor := &GetSubscriptionOffsetResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, gsor); err != nil {
		return nil, err
	}
	return gsor, nil
}

type CommitSubscriptionOffsetResult struct {
	CommonResponseResult
}

func newCommitSubscriptionOffsetResult(commonResp *CommonResponseResult) (*CommitSubscriptionOffsetResult, error) {
	csor := &CommitSubscriptionOffsetResult{
		CommonResponseResult: *commonResp,
	}
	return csor, nil
}

type ResetSubscriptionOffsetResult struct {
	CommonResponseResult
}

func newResetSubscriptionOffsetResult(commonResp *CommonResponseResult) (*ResetSubscriptionOffsetResult, error) {
	rsor := &ResetSubscriptionOffsetResult{
		CommonResponseResult: *commonResp,
	}
	return rsor, nil
}

type HeartbeatResult struct {
	CommonResponseResult
	PlanVersion int64    `json:"PlanVersion"`
	ShardList   []string `json:"ShardList"`
	TotalPlan   string   `json:"TotalPlan"`
}

func newHeartbeatResult(data []byte, commonResp *CommonResponseResult) (*HeartbeatResult, error) {
	hr := &HeartbeatResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, hr); err != nil {
		return nil, err
	}
	return hr, nil
}

type JoinGroupResult struct {
	CommonResponseResult
	ConsumerId     string `json:"ConsumerId"`
	VersionId      int64  `json:"VersionId"`
	SessionTimeout int64  `json:"SessionTimeout"`
}

func newJoinGroupResult(data []byte, commonResp *CommonResponseResult) (*JoinGroupResult, error) {
	jgr := &JoinGroupResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, jgr); err != nil {
		return nil, err
	}
	return jgr, nil
}

type SyncGroupResult struct {
	CommonResponseResult
}

func newSyncGroupResult(commonResp *CommonResponseResult) (*SyncGroupResult, error) {
	sgr := &SyncGroupResult{
		CommonResponseResult: *commonResp,
	}
	return sgr, nil
}

type LeaveGroupResult struct {
	CommonResponseResult
}

func newLeaveGroupResult(commonResp *CommonResponseResult) (*LeaveGroupResult, error) {
	lgr := &LeaveGroupResult{
		CommonResponseResult: *commonResp,
	}
	return lgr, nil
}

type ListTopicSchemaResult struct {
	CommonResponseResult
	SchemaInfoList []RecordSchemaInfo `json:"RecordSchemaList"`
}

// for deserialize the RecordSchema
func (gtr *ListTopicSchemaResult) UnmarshalJSON(data []byte) error {
	type RecordSchemaInfoHelper struct {
		VersionId    int    `json:"VersionId"`
		RecordSchema string `json:"RecordSchema"`
	}

	msg := &struct {
		SchemaInfoList []RecordSchemaInfoHelper `json:"RecordSchemaList"`
	}{}

	if err := json.Unmarshal(data, msg); err != nil {
		return err
	}

	for _, info := range msg.SchemaInfoList {
		schema := &RecordSchema{}
		if err := json.Unmarshal([]byte(info.RecordSchema), schema); err != nil {
			return err
		}
		for idx := range schema.Fields {
			schema.Fields[idx].AllowNull = !schema.Fields[idx].AllowNull
		}

		schemaInfo := RecordSchemaInfo{
			VersionId:    info.VersionId,
			RecordSchema: *schema,
		}
		gtr.SchemaInfoList = append(gtr.SchemaInfoList, schemaInfo)
	}
	return nil
}

func newListTopicSchemaResult(data []byte, commonResp *CommonResponseResult) (*ListTopicSchemaResult, error) {
	ret := &ListTopicSchemaResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type GetTopicSchemaResult struct {
	CommonResponseResult
	VersionId    int          `json:"VersionId"`
	RecordSchema RecordSchema `json:"RecordSchema"`
}

func (gtr *GetTopicSchemaResult) UnmarshalJSON(data []byte) error {
	msg := &struct {
		VersionId    int    `json:"VersionId"`
		RecordSchema string `json:"RecordSchema"`
	}{}

	if err := json.Unmarshal(data, msg); err != nil {
		return err
	}

	schema := &RecordSchema{}
	if err := json.Unmarshal([]byte(msg.RecordSchema), schema); err != nil {
		return err
	}
	for idx := range schema.Fields {
		schema.Fields[idx].AllowNull = !schema.Fields[idx].AllowNull
	}

	gtr.VersionId = msg.VersionId
	gtr.RecordSchema = *schema
	return nil
}

func newGetTopicSchemaResult(data []byte, commonResp *CommonResponseResult) (*GetTopicSchemaResult, error) {
	ret := &GetTopicSchemaResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type RegisterTopicSchemaResult struct {
	CommonResponseResult
	VersionId int `json:"VersionId"`
}

func newRegisterTopicSchemaResult(data []byte, commonResp *CommonResponseResult) (*RegisterTopicSchemaResult, error) {
	ret := &RegisterTopicSchemaResult{
		CommonResponseResult: *commonResp,
	}
	if err := json.Unmarshal(data, ret); err != nil {
		return nil, err
	}
	return ret, nil
}

type DeleteTopicSchemaResult struct {
	CommonResponseResult
}

func newDeleteTopicSchemaResult(commonResp *CommonResponseResult) (*DeleteTopicSchemaResult, error) {
	ret := &DeleteTopicSchemaResult{
		CommonResponseResult: *commonResp,
	}
	return ret, nil
}
