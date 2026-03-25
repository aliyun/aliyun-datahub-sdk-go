package datahub

import (
	"fmt"
)

/*
examples errors
*/

// Error codes
const (
	InvalidParameter    = "InvalidParameter"
	InvalidSubscription = "InvalidSubscription"
	InvalidCursor       = "InvalidCursor"
	/**
	 * for later arrange error code
	 */
	ResourceNotFound   = "ResourceNotFound"
	NoSuchTopic        = "NoSuchTopic"
	NoSuchProject      = "NoSuchProject"
	NoSuchSubscription = "NoSuchSubscription"
	NoSuchShard        = "NoSuchShard"
	NoSuchConnector    = "NoSuchConnector"
	NoSuchMeterInfo    = "NoSuchMeteringInfo"
	/**
	 * for later arrange error code
	 */
	SeekOutOfRange        = "SeekOutOfRange"
	ResourceAlreadyExist  = "ResourceAlreadyExist"
	ProjectAlreadyExist   = "ProjectAlreadyExist"
	TopicAlreadyExist     = "TopicAlreadyExist"
	ConnectorAlreadyExist = "ConnectorAlreadyExist"
	UnAuthorized          = "Unauthorized"
	NoPermission          = "NoPermission"
	InvalidShardOperation = "InvalidShardOperation"
	OperatorDenied        = "OperationDenied"
	LimitExceed           = "LimitExceeded"
	//ODPSServiceError       = "OdpsServiceError"
	//MysqlServiceError      = "MysqlServiceError"
	//InternalServerErrorS    = "InternalServerError"
	SubscriptionOffline    = "SubscriptionOffline"
	OffsetReseted          = "OffsetReseted"
	OffsetSessionClosed    = "OffsetSessionClosed"
	OffsetSessionChanged   = "OffsetSessionChanged"
	MalformedRecord        = "MalformedRecord"
	NoSuchConsumer         = "NoSuchConsumer"
	ConsumerGroupInProcess = "ConsumerGroupInProcess"
)

const (
	projectNameInvalid   string = "project name should start with letter, only contains [a-zA-Z0-9_], 3 < length < 32"
	commentInvalid       string = "comment can not be empty and length must less than 1024"
	topicNameInvalid     string = "topic name should start with letter, only contains [a-zA-Z0-9_], 1 < length < 128"
	shardIdInvalid       string = "shardId is invalid"
	shardListInvalid     string = "shard list is empty"
	lifecycleInvalid     string = "lifecycle is invalid"
	parameterInvalid     string = "parameter is invalid"
	parameterNull        string = "parameter is nil"
	parameterNumInvalid  string = "parameter num invalid"
	parameterTypeInvalid string = "parameter type is invalid,please check your input parameter type"
	missingRecordSchema  string = "missing record schema for tuple record type"
	recordsInvalid       string = "records is invalid, nil, empty or other invalid reason"
)

// return the specific err type by errCode,
// you can handle the error by type assert
func errorHandler(err *DatahubError) error {

	switch err.Code {
	case InvalidParameter, InvalidSubscription, InvalidCursor:
		return &InvalidParameterError{DatahubError: *err}
	case ResourceNotFound, NoSuchTopic, NoSuchProject, NoSuchSubscription, NoSuchShard, NoSuchConnector,
		NoSuchMeterInfo, NoSuchConsumer:
		return &ResourceNotFoundError{DatahubError: *err}
	case SeekOutOfRange:
		return &SeekOutOfRangeError{DatahubError: *err}
	case ResourceAlreadyExist, ProjectAlreadyExist, TopicAlreadyExist, ConnectorAlreadyExist:
		return &ResourceExistError{DatahubError: *err}
	case UnAuthorized:
		return &AuthorizationFailedError{DatahubError: *err}
	case NoPermission:
		return &NoPermissionError{DatahubError: *err}
	case OperatorDenied:
		return &InvalidOperationError{DatahubError: *err}
	case LimitExceed:
		return &LimitExceededError{DatahubError: *err}
	case SubscriptionOffline:
		return &SubscriptionOfflineError{DatahubError: *err}
	case OffsetReseted:
		return &SubscriptionOffsetResetError{DatahubError: *err}
	case OffsetSessionClosed, OffsetSessionChanged:
		return &SubscriptionSessionInvalidError{DatahubError: *err}
	case MalformedRecord:
		return &MalformedRecordError{DatahubError: *err}
	case ConsumerGroupInProcess:
		return &ServiceInProcessError{DatahubError: *err}
	case InvalidShardOperation:
		return &ShardSealedError{DatahubError: *err}
	}
	return err
}

func IsNetworkError(err error) bool {
	_, ok := err.(*NetworkError)
	return ok
}

func IsDataHubError(err error) bool {
	_, ok := err.(*DatahubError)
	return ok
}

func IsLimitExceedError(err error) bool {
	_, ok := err.(*LimitExceededError)
	return ok
}

func IsShardSealedError(err error) bool {
	_, ok := err.(*ShardSealedError)
	return ok
}

func IsServiceInProcessError(err error) bool {
	_, ok := err.(*ServiceInProcessError)
	return ok
}

func IsSeekOutOfRange(err error) bool {
	_, ok := err.(*SeekOutOfRangeError)
	return ok
}

func IsFieldNotExistsError(err error) bool {
	_, ok := err.(*FieldNotExistsError)
	return ok
}

func IsRetryableError(err error) bool {
	switch err.(type) {
	case *InvalidParameterError, *ResourceNotFoundError, *ResourceExistError, *InvalidOperationError,
		*AuthorizationFailedError, *NoPermissionError, *SeekOutOfRangeError, *SubscriptionOfflineError,
		*SubscriptionOffsetResetError, *SubscriptionSessionInvalidError, *MalformedRecordError,
		*ShardSealedError:
		return false
	case *NetworkError, *DatahubError, *LimitExceededError, *ServiceInProcessError:
		return true
	default:
		return true
	}
}

// Deprecated: Use DatahubError instead.
type DatahubClientError struct {
	StatusCode int    `json:"StatusCode"`   // Http status code
	RequestId  string `json:"RequestId"`    // Request-id to trace the request
	Code       string `json:"ErrorCode"`    // Datahub error code
	Message    string `json:"ErrorMessage"` // Error msg of the error code
}

func (err *DatahubClientError) Error() string {
	return fmt.Sprintf("statusCode: %d, requestId: %s, errCode: %s, errMsg: %s",
		err.StatusCode, err.RequestId, err.Code, err.Message)
}

func NewDatahubError(statusCode int, requestId string, code string, message string) *DatahubError {
	return &DatahubError{StatusCode: statusCode, RequestId: requestId, Code: code, Message: message}
}

type DatahubError struct {
	StatusCode int    `json:"StatusCode"`   // Http status code
	RequestId  string `json:"RequestId"`    // Request-id to trace the request
	Code       string `json:"ErrorCode"`    // Datahub error code
	Message    string `json:"ErrorMessage"` // Error msg of the error code
	Detail     string `json:"ErrorDetail"`  // Error detail
}

func (err *DatahubError) Error() string {
	return fmt.Sprintf("HttpCode: %d, RequestId: %s, ErrCode: %s, ErrMsg: %s, ErrDetail: %s",
		err.StatusCode, err.RequestId, err.Code, err.Message, err.Detail)
}

func newInvalidParameterErrorWithMessage(message string) *InvalidParameterError {
	return &InvalidParameterError{
		DatahubError{
			StatusCode: -1,
			RequestId:  "",
			Code:       "",
			Message:    message,
			Detail:     "",
		},
	}
}

// InvalidParameterError represent the parameter error
type InvalidParameterError struct {
	DatahubError
}

type ResourceNotFoundError struct {
	DatahubError
}

type ResourceExistError struct {
	DatahubError
}

type InvalidOperationError struct {
	DatahubError
}

type LimitExceededError struct {
	DatahubError
}

type AuthorizationFailedError struct {
	DatahubError
}

type NoPermissionError struct {
	DatahubError
}

type SeekOutOfRangeError struct {
	DatahubError
}

type SubscriptionOfflineError struct {
	DatahubError
}

type SubscriptionOffsetResetError struct {
	DatahubError
}

type SubscriptionSessionInvalidError struct {
	DatahubError
}

type MalformedRecordError struct {
	DatahubError
}

type ServiceInProcessError struct {
	DatahubError
}

type ShardSealedError struct {
	DatahubError
}

type ServiceTemporaryUnavailableError struct {
	DatahubError
}

func newNetworkError(err error) *NetworkError {
	return &NetworkError{
		oriErr: err,
	}
}

type NetworkError struct {
	oriErr error
}

func (ne *NetworkError) Error() string {
	return ne.oriErr.Error()
}

func newFieldNotExistsError(msg string) *FieldNotExistsError {
	return &FieldNotExistsError{
		msg: msg,
	}
}

type FieldNotExistsError struct {
	msg string
}

func (ne *FieldNotExistsError) Error() string {
	return ne.msg
}
