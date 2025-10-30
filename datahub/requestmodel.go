package datahub

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/pbmodel"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/util"
)

type requestInfo struct {
	rawSzie int
}

func newRequestInfo(rawSize int) *requestInfo {
	return &requestInfo{
		rawSzie: rawSize,
	}
}

// handel the http request
type RequestModel interface {
	// serialize the requestModel and maybe need add some message on http header
	requestBodyEncode() ([]byte, *requestInfo, error)
	getExtraHeader() map[string]string
	getExtraQuery() map[string]string
}

type commonRequest struct {
	header map[string]string
	query  map[string]string
}

func newDefaultRequest() *commonRequest {
	return &commonRequest{}
}

func (cr *commonRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	return nil, newRequestInfo(0), nil
}

func (cr *commonRequest) getExtraHeader() map[string]string {
	return cr.header
}

func (cr *commonRequest) getExtraQuery() map[string]string {
	return cr.query
}

type CreateProjectRequest struct {
	commonRequest
	Comment string `json:"Comment"`
}

func (cpr *CreateProjectRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(cpr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateProjectRequest struct {
	commonRequest
	Comment string `json:"Comment"`
}

func (upr *UpdateProjectRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(upr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateProjectVpcWhitelistRequest struct {
	commonRequest
	VpcIds string `json:"VpcIds"`
}

func (upv *UpdateProjectVpcWhitelistRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(upv)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type CreateTopicRequest struct {
	commonRequest
	Action       string        `json:"Action"`
	ShardCount   int           `json:"ShardCount"`
	Lifecycle    int           `json:"Lifecycle"`
	RecordType   RecordType    `json:"RecordType"`
	RecordSchema *RecordSchema `json:"RecordSchema,omitempty"`
	Comment      string        `json:"Comment"`
	ExpandMode   ExpandMode    `json:"ExpandMode"`
}

func (ctr *CreateTopicRequest) MarshalJSON() ([]byte, error) {
	msg := &struct {
		Action       string     `json:"Action"`
		ShardCount   int        `json:"ShardCount"`
		Lifecycle    int        `json:"Lifecycle"`
		RecordType   RecordType `json:"RecordType"`
		RecordSchema string     `json:"RecordSchema,omitempty"`
		Comment      string     `json:"Comment"`
		ExpandMode   ExpandMode `json:"ExpandMode"`
	}{
		Action:     ctr.Action,
		ShardCount: ctr.ShardCount,
		Lifecycle:  ctr.Lifecycle,
		RecordType: ctr.RecordType,
		Comment:    ctr.Comment,
		ExpandMode: ctr.ExpandMode,
	}
	switch ctr.RecordType {
	case TUPLE:
		msg.RecordSchema = ctr.RecordSchema.String()
	default:
		msg.RecordSchema = ""

	}
	return json.Marshal(msg)
}

func (ctr *CreateTopicRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ctr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateTopicRequest struct {
	commonRequest
	Comment   string `json:"Comment,omitempty"`
	Lifecycle int    `json:"Lifecycle,omitempty"`
}

func (utr *UpdateTopicRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(utr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type SplitShardRequest struct {
	commonRequest
	Action   string `json:"Action"`
	ShardId  string `json:"ShardId"`
	SplitKey string `json:"SplitKey,omitempty"`
}

func (ssr *SplitShardRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ssr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type MergeShardRequest struct {
	commonRequest
	Action          string `json:"Action"`
	ShardId         string `json:"ShardId"`
	AdjacentShardId string `json:"AdjacentShardId"`
}

func (msr *MergeShardRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(msr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type ExtendShardRequest struct {
	commonRequest
	Action     string `json:"Action"`
	ExtendMode string `json:"ExtendMode"`
	ShardCount int    `json:"ShardNumber"`
}

func (esr *ExtendShardRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(esr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type GetCursorRequest struct {
	commonRequest
	Action     string     `json:"Action"`
	CursorType CursorType `json:"Type"`
	SystemTime int64      `json:"SystemTime"`
	Sequence   int64      `json:"Sequence"`
}

func (gcr *GetCursorRequest) MarshalJSON() ([]byte, error) {
	type ReqMsg struct {
		Action string     `json:"Action"`
		Type   CursorType `json:"Type"`
	}
	reqMsg := ReqMsg{
		Action: gcr.Action,
		Type:   gcr.CursorType,
	}
	switch gcr.CursorType {
	case OLDEST, LATEST:
		return json.Marshal(reqMsg)
	case SYSTEM_TIME:
		return json.Marshal(struct {
			ReqMsg
			SystemTime int64 `json:"SystemTime"`
		}{
			ReqMsg:     reqMsg,
			SystemTime: gcr.SystemTime,
		})
	case SEQUENCE:
		return json.Marshal(struct {
			ReqMsg
			Sequence int64 `json:"Sequence"`
		}{
			ReqMsg:   reqMsg,
			Sequence: gcr.Sequence,
		})
	default:
		return nil, fmt.Errorf("cursor not support type %s", gcr.CursorType)
	}
}

func (gcr *GetCursorRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(gcr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type PutRecordsRequest struct {
	commonRequest
	Action  string    `json:"Action"`
	Records []IRecord `json:"Records"`
}

func (prr *PutRecordsRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(prr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

func (ptr *PutRecordsRequest) MarshalJSON() ([]byte, error) {
	msg := &struct {
		Action  string        `json:"Action"`
		Records []RecordEntry `json:"Records"`
	}{
		Action:  ptr.Action,
		Records: make([]RecordEntry, len(ptr.Records)),
	}
	for idx, val := range ptr.Records {
		msg.Records[idx].Data = val.GetData()
		msg.Records[idx].BaseRecord = val.GetBaseRecord()
	}
	return json.Marshal(msg)
}

type GetRecordRequest struct {
	commonRequest
	Action string `json:"Action"`
	Cursor string `json:"Cursor"`
	Limit  int    `json:"Limit"`
}

func (grr *GetRecordRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(grr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type AppendFieldRequest struct {
	commonRequest
	Action    string    `json:"Action"`
	FieldName string    `json:"FieldName"`
	FieldType FieldType `json:"FieldType"`
}

func (afr *AppendFieldRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(afr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type GetMeterInfoRequest struct {
	commonRequest
	Action string `json:"Action"`
}

func (gmir *GetMeterInfoRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(gmir)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type CreateConnectorRequest struct {
	commonRequest
	Action        string            `json:"Action"`
	Type          ConnectorType     `json:"Type"`
	SinkStartTime int64             `json:"SinkStartTime"`
	ColumnFields  []string          `json:"ColumnFields"`
	ColumnNameMap map[string]string `json:"ColumnNameMap"`
	Config        interface{}       `json:"Config"`
}

func (ccr *CreateConnectorRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	var buf []byte = nil
	var err error = nil
	switch ccr.Type {
	case SinkOdps:
		buf, err = marshalCreateOdpsConnector(ccr)
	case SinkOss:
		buf, err = marshalCreateOssConnector(ccr)
	case SinkEs:
		buf, err = marshalCreateEsConnector(ccr)
	case SinkAds:
		buf, err = marshalCreateAdsConnector(ccr)
	case SinkMysql:
		buf, err = marshalCreateMysqlConnector(ccr)
	case SinkFc:
		buf, err = marshalCreateFcConnector(ccr)
	case SinkOts:
		buf, err = marshalCreateOtsConnector(ccr)
	case SinkDatahub:
		buf, err = marshalCreateDatahubConnector(ccr)
	case SinkHologres:
		buf, err = marshalCreateHologresConnector(ccr)
	default:
		err = fmt.Errorf("not support connector type config: %s", ccr.Type.String())
	}

	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateConnectorRequest struct {
	commonRequest
	Action        string            `json:"Action"`
	ColumnFields  []string          `json:"ColumnFields"`
	ColumnNameMap map[string]string `json:"ColumnNameMap"`
	Config        interface{}       `json:"Config"`
}

func (ucr *UpdateConnectorRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	var buf []byte = nil
	var err error = nil
	if ucr.Config == nil {
		buf, err = marshalUpdateConnector(ucr)
		if err != nil {
			return nil, nil, err
		}
		return buf, newRequestInfo(len(buf)), nil
	}

	switch ucr.Config.(type) {
	case SinkOdpsConfig:
		buf, err = marshalUpdateOdpsConnector(ucr)
	case SinkOssConfig:
		buf, err = marshalUpdateOssConnector(ucr)
	case SinkEsConfig:
		buf, err = marshalUpdateEsConnector(ucr)
	case SinkAdsConfig:
		buf, err = marshalUpdateAdsConnector(ucr)
	case SinkMysqlConfig:
		buf, err = marshalUpdateMysqlConnector(ucr)
	case SinkFcConfig:
		buf, err = marshalUpdateFcConnector(ucr)
	case SinkOtsConfig:
		buf, err = marshalUpdateOtsConnector(ucr)
	case SinkDatahubConfig:
		buf, err = marshalUpdateDatahubConnector(ucr)
	case SinkHologresConfig:
		buf, err = marshalUpdateHologresConnector(ucr)
	default:
		err = fmt.Errorf("this connector type not support, %t", reflect.TypeOf(ucr.Config))
	}

	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type ReloadConnectorRequest struct {
	commonRequest
	Action  string `json:"Action"`
	ShardId string `json:"ShardId,omitempty"`
}

func (rcr *ReloadConnectorRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(rcr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateConnectorStateRequest struct {
	commonRequest
	Action string         `json:"Action"`
	State  ConnectorState `json:"State"`
}

func (ucsr *UpdateConnectorStateRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ucsr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateConnectorOffsetRequest struct {
	commonRequest
	Action    string `json:"Action"`
	ShardId   string `json:"ShardId"`
	Timestamp int64  `json:"CurrentTime"`
	Sequence  int64  `json:"CurrentSequence"`
}

func (ucor *UpdateConnectorOffsetRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ucor)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type GetConnectorShardStatusRequest struct {
	commonRequest
	Action  string `json:"Action"`
	ShardId string `json:"ShardId,omitempty"`
}

func (gcss *GetConnectorShardStatusRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(gcss)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type AppendConnectorFieldRequest struct {
	commonRequest
	Action    string `json:"Action"`
	FieldName string `json:"FieldName"`
}

func (acfr *AppendConnectorFieldRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(acfr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type CreateSubscriptionRequest struct {
	commonRequest
	Action  string `json:"Action"`
	Comment string `json:"Comment"`
}

func (csr *CreateSubscriptionRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(csr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type ListSubscriptionRequest struct {
	commonRequest
	Action    string `json:"Action"`
	PageIndex int    `json:"PageIndex"`
	PageSize  int    `json:"PageSize"`
}

func (lsr *ListSubscriptionRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(lsr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateSubscriptionRequest struct {
	commonRequest
	Comment string `json:"Comment"`
}

func (usr *UpdateSubscriptionRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(usr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type UpdateSubscriptionStateRequest struct {
	commonRequest
	State SubscriptionState `json:"State"`
}

func (ussr *UpdateSubscriptionStateRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ussr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type OpenSubscriptionSessionRequest struct {
	commonRequest
	Action   string   `json:"Action"`
	ShardIds []string `json:"ShardIds"`
}

func (ossr *OpenSubscriptionSessionRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(ossr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type GetSubscriptionOffsetRequest struct {
	commonRequest
	Action   string   `json:"Action"`
	ShardIds []string `json:"ShardIds"`
}

func (gsor *GetSubscriptionOffsetRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(gsor)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type CommitSubscriptionOffsetRequest struct {
	commonRequest
	Action  string                        `json:"Action"`
	Offsets map[string]SubscriptionOffset `json:"Offsets"`
}

func (csor *CommitSubscriptionOffsetRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(csor)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type ResetSubscriptionOffsetRequest struct {
	commonRequest
	Action  string                        `json:"Action"`
	Offsets map[string]SubscriptionOffset `json:"Offsets"`
}

func (rsor *ResetSubscriptionOffsetRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(rsor)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type HeartbeatRequest struct {
	commonRequest
	Action           string   `json:"Action"`
	ConsumerId       string   `json:"ConsumerId"`
	VersionId        int64    `json:"VersionId"`
	HoldShardList    []string `json:"HoldShardList,omitempty"`
	ReadEndShardList []string `json:"ReadEndShardList,omitempty"`
}

func (hr *HeartbeatRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(hr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type JoinGroupRequest struct {
	commonRequest
	Action         String `json:"Action"`
	SessionTimeout int64  `json:"SessionTimeout"`
}

func (jgr *JoinGroupRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(jgr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type SyncGroupRequest struct {
	commonRequest
	Action           string   `json:"Action"`
	ConsumerId       string   `json:"ConsumerId"`
	VersionId        int64    `json:"VersionId"`
	ReleaseShardList []string `json:"ReleaseShardList,omitempty"`
	ReadEndShardList []string `json:"ReadEndShardList,omitempty"`
}

func (sgr *SyncGroupRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(sgr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type LeaveGroupRequest struct {
	commonRequest
	Action     string `json:"Action"`
	ConsumerId string `json:"ConsumerId"`
	VersionId  int64  `json:"VersionId"`
}

func (lgr *LeaveGroupRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(lgr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type ListTopicSchemaRequest struct {
	commonRequest
	Action string `json:"Action"`
}

func (lts *ListTopicSchemaRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(lts)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type GetTopicSchemaRequest struct {
	commonRequest
	Action       string        `json:"Action"`
	VersionId    int           `json:"VersionId"`
	RecordSchema *RecordSchema `json:"RecordSchema,omitempty"`
}

func (gts *GetTopicSchemaRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	msg := &struct {
		Action       string `json:"Action"`
		VersionId    int    `json:"VersionId"`
		RecordSchema string `json:"RecordSchema,omitempty"`
	}{
		Action:    gts.Action,
		VersionId: gts.VersionId,
	}

	if gts.RecordSchema != nil {
		msg.RecordSchema = gts.RecordSchema.String()
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type RegisterTopicSchemaRequest struct {
	commonRequest
	Action       string        `json:"Action"`
	RecordSchema *RecordSchema `json:"RecordSchema"`
}

func (rts *RegisterTopicSchemaRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	msg := &struct {
		Action       string `json:"Action"`
		RecordSchema string `json:"RecordSchema,omitempty"`
	}{
		Action: rts.Action,
	}

	if rts.RecordSchema != nil {
		msg.RecordSchema = rts.RecordSchema.String()
	}

	buf, err := json.Marshal(msg)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type DeleteTopicSchemaRequest struct {
	commonRequest
	Action    string `json:"Action"`
	VersionId int    `json:"VersionId"`
}

func (dtr *DeleteTopicSchemaRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	buf, err := json.Marshal(dtr)
	if err != nil {
		return nil, nil, err
	}
	return buf, newRequestInfo(len(buf)), nil
}

type PutPBRecordsRequest struct {
	commonRequest
	Records []IRecord `json:"Records"`
}

func (pr *PutPBRecordsRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	res := make([]*pbmodel.RecordEntry, len(pr.Records))
	for idx, val := range pr.Records {
		bRecord := val.GetBaseRecord()
		data := val.GetData()

		fds := make([]*pbmodel.FieldData, 0)
		switch val := data.(type) {
		case []byte:
			fd := &pbmodel.FieldData{
				Value: val,
			}
			fds = append(fds, fd)
		default:
			v, ok := data.([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("data format is invalid")
			}
			for _, str := range v {
				fd := &pbmodel.FieldData{}
				if str == nil {
					fd.Value = nil
				} else {
					fd.Value = []byte(fmt.Sprintf("%s", str))
				}
				fds = append(fds, fd)
			}
		}
		rd := &pbmodel.RecordData{
			Data: fds,
		}

		recordEntry := &pbmodel.RecordEntry{
			ShardId: proto.String(bRecord.ShardId),
			Data:    rd,
		}

		if len(bRecord.Attributes) > 0 {
			sps := make([]*pbmodel.StringPair, len(bRecord.Attributes))
			index := 0
			for k, v := range bRecord.Attributes {
				strv := fmt.Sprintf("%v", v)
				sp := &pbmodel.StringPair{
					Key:   proto.String(k),
					Value: proto.String(strv),
				}
				sps[index] = sp
				index++
			}
			ra := &pbmodel.RecordAttributes{
				Attributes: sps,
			}
			recordEntry.Attributes = ra
		}
		res[idx] = recordEntry
	}

	prr := &pbmodel.PutRecordsRequest{
		Records: res,
	}
	buf, err := proto.Marshal(prr)
	if err != nil {
		return nil, nil, err
	}
	wBuf := util.WrapMessage(buf)
	return wBuf, newRequestInfo(len(wBuf)), nil
}

type GetPBRecordRequest struct {
	commonRequest
	Cursor string `json:"Cursor"`
	Limit  int    `json:"Limit"`
}

func (gpr *GetPBRecordRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	limit := int32(gpr.Limit)
	grr := &pbmodel.GetRecordsRequest{
		Cursor: &gpr.Cursor,
		Limit:  &limit,
	}

	buf, err := proto.Marshal(grr)
	if err != nil {
		return nil, nil, err
	}

	wBuf := util.WrapMessage(buf)
	return wBuf, newRequestInfo(len(wBuf)), nil
}

type PutBatchRecordsRequest struct {
	commonRequest
	serializer *batchSerializer
	Records    []IRecord
}

func (pbr *PutBatchRecordsRequest) requestBodyEncode() ([]byte, *requestInfo, error) {
	batchBuf, header, err := pbr.serializer.serialize(pbr.Records)
	if err != nil {
		return nil, nil, err
	}

	entry := &pbmodel.BinaryRecordEntry{
		Data: batchBuf,
	}
	protoReq := &pbmodel.PutBinaryRecordsRequest{
		Records: []*pbmodel.BinaryRecordEntry{entry},
	}

	buf, err := proto.Marshal(protoReq)
	if err != nil {
		return nil, nil, err
	}
	return util.WrapMessage(buf), newRequestInfo(int(header.rawSize)), nil
}

type GetBatchRecordRequest struct {
	GetPBRecordRequest
}
