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
func errorHandler(statusCode int, requestId string, errorCode string, message string) error {

	switch errorCode {
	case InvalidParameter, InvalidSubscription, InvalidCursor:
		return newInvalidParameterError(statusCode, requestId, errorCode, message)
	case ResourceNotFound, NoSuchTopic, NoSuchProject, NoSuchSubscription, NoSuchShard, NoSuchConnector,
		NoSuchMeterInfo, NoSuchConsumer:
		return newResourceNotFoundError(statusCode, requestId, errorCode, message)
	case SeekOutOfRange:
		return newSeekOutOfRangeError(statusCode, requestId, errorCode, message)
	case ResourceAlreadyExist, ProjectAlreadyExist, TopicAlreadyExist, ConnectorAlreadyExist:
		return newResourceExistError(statusCode, requestId, errorCode, message)
	case UnAuthorized:
		return newAuthorizationFailedError(statusCode, requestId, errorCode, message)
	case NoPermission:
		return newNoPermissionError(statusCode, requestId, errorCode, message)
	case OperatorDenied:
		return newInvalidOperationError(statusCode, requestId, errorCode, message)
	case LimitExceed:
		return newLimitExceededError(statusCode, requestId, errorCode, message)
	case SubscriptionOffline:
		return newSubscriptionOfflineError(statusCode, requestId, errorCode, message)
	case OffsetReseted:
		return newSubscriptionOffsetResetError(statusCode, requestId, errorCode, message)
	case OffsetSessionClosed, OffsetSessionChanged:
		return newSubscriptionSessionInvalidError(statusCode, requestId, errorCode, message)
	case MalformedRecord:
		return newMalformedRecordError(statusCode, requestId, errorCode, message)
	case ConsumerGroupInProcess:
		return newServiceInProcessError(statusCode, requestId, errorCode, message)
	case InvalidShardOperation:
		return newShardSealedError(statusCode, requestId, errorCode, message)
	}
	return NewDatahubError(statusCode, requestId, errorCode, message)
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

// create a new DatahubClientError
func newDatahubClientError(statusCode int, requestId string, code string, message string) *DatahubClientError {
	return &DatahubClientError{StatusCode: statusCode, RequestId: requestId, Code: code, Message: message}
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
}

func (err *DatahubError) Error() string {
	return fmt.Sprintf("statusCode: %d, requestId: %s, errCode: %s, errMsg: %s",
		err.StatusCode, err.RequestId, err.Code, err.Message)
}

func newInvalidParameterErrorWithMessage(message string) *InvalidParameterError {
	return &InvalidParameterError{
		DatahubError{
			StatusCode: -1,
			RequestId:  "",
			Code:       "",
			Message:    message,
		},
	}
}

func newInvalidParameterError(statusCode int, requestId string, code string, message string) *InvalidParameterError {
	return &InvalidParameterError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

// InvalidParameterError represent the parameter error
type InvalidParameterError struct {
	DatahubError
}

func newResourceNotFoundError(statusCode int, requestId string, code string, message string) *ResourceNotFoundError {
	return &ResourceNotFoundError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type ResourceNotFoundError struct {
	DatahubError
}

func newResourceExistError(statusCode int, requestId string, code string, message string) *ResourceExistError {
	return &ResourceExistError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type ResourceExistError struct {
	DatahubError
}

func newInvalidOperationError(statusCode int, requestId string, code string, message string) *InvalidOperationError {
	return &InvalidOperationError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type InvalidOperationError struct {
	DatahubError
}

func newLimitExceededError(statusCode int, requestId string, code string, message string) *LimitExceededError {
	return &LimitExceededError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type LimitExceededError struct {
	DatahubError
}

func newAuthorizationFailedError(statusCode int, requestId string, code string, message string) *AuthorizationFailedError {
	return &AuthorizationFailedError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type AuthorizationFailedError struct {
	DatahubError
}

func newNoPermissionError(statusCode int, requestId string, code string, message string) *NoPermissionError {
	return &NoPermissionError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type NoPermissionError struct {
	DatahubError
}

func newSeekOutOfRangeError(statusCode int, requestId string, code string, message string) *SeekOutOfRangeError {
	return &SeekOutOfRangeError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type SeekOutOfRangeError struct {
	DatahubError
}

func newSubscriptionOfflineError(statusCode int, requestId string, code string, message string) *SubscriptionOfflineError {
	return &SubscriptionOfflineError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type SubscriptionOfflineError struct {
	DatahubError
}

func newSubscriptionOffsetResetError(statusCode int, requestId string, code string, message string) *SubscriptionOffsetResetError {
	return &SubscriptionOffsetResetError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type SubscriptionOffsetResetError struct {
	DatahubError
}

func newSubscriptionSessionInvalidError(statusCode int, requestId string, code string, message string) *SubscriptionSessionInvalidError {
	return &SubscriptionSessionInvalidError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type SubscriptionSessionInvalidError struct {
	DatahubError
}

func newMalformedRecordError(statusCode int, requestId string, code string, message string) *MalformedRecordError {
	return &MalformedRecordError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type MalformedRecordError struct {
	DatahubError
}

func newServiceInProcessError(statusCode int, requestId string, code string, message string) *ServiceInProcessError {
	return &ServiceInProcessError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type ServiceInProcessError struct {
	DatahubError
}

func newShardSealedError(statusCode int, requestId string, code string, message string) *ShardSealedError {
	return &ShardSealedError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
}

type ShardSealedError struct {
	DatahubError
}

func newServiceTemporaryUnavailableError(message string) *ServiceTemporaryUnavailableError {
	return &ServiceTemporaryUnavailableError{
		DatahubError{
			StatusCode: -1,
			RequestId:  "",
			Code:       "",
			Message:    message,
		},
	}
}

func newServiceTemporaryUnavailableErrorWithCode(statusCode int, requestId string, code string, message string) *ServiceTemporaryUnavailableError {
	return &ServiceTemporaryUnavailableError{
		DatahubError{
			StatusCode: statusCode,
			RequestId:  requestId,
			Code:       code,
			Message:    message,
		},
	}
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
