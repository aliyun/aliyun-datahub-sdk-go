package datahub

import (
    "fmt"
)

/*
examples errors
*/

// Error codes
const (
    invalidParameter    = "InvalidParameter"
    invalidSubscription = "InvalidSubscription"
    invalidCursor       = "InvalidCursor"
    /**
     * for later arrange error code
     */
    resourceNotFound   = "ResourceNotFound"
    noSuchTopic        = "NoSuchTopic"
    noSuchProject      = "NoSuchProject"
    noSuchSubscription = "NoSuchSubscription"
    noSuchShard        = "NoSuchShard"
    noSuchConnector    = "NoSuchConnector"
    noSuchMeterInfo    = "NoSuchMeteringInfo"
    /**
     * for later arrange error code
     */
    seekOutOfRange        = "SeekOutOfRange"
    resourceAlreadyExist  = "ResourceAlreadyExist"
    projectAlreadyExist   = "ProjectAlreadyExist"
    topicAlreadyExist     = "TopicAlreadyExist"
    connectorAlreadyExist = "ConnectorAlreadyExist"
    unAuthorized          = "Unauthorized"
    noPermission          = "NoPermission"
    invalidShardOperation = "InvalidShardOperation"
    operatorDenied        = "OperationDenied"
    limitExceed           = "LimitExceeded"
    //ODPSServiceError       = "OdpsServiceError"
    //MysqlServiceError      = "MysqlServiceError"
    //InternalServerErrorS    = "InternalServerError"
    subscriptionOffline    = "SubscriptionOffline"
    offsetReseted          = "OffsetReseted"
    offsetSessionClosed    = "OffsetSessionClosed"
    offsetSessionChanged   = "OffsetSessionChanged"
    malformedRecord        = "MalformedRecord"
    noSuchConsumer         = "NoSuchConsumer"
    consumerGroupInProcess = "ConsumerGroupInProcess"
)

const (
    projectNameInvalid   string = "project name should start with letter, only contains [a-zA-Z0-9_], 3 < length < 32"
    commentInvalid       string = "comment can not be empty and length must less than 1024"
    topicNameInvalid     string = "topic name should start with letter, only contains [a-zA-Z0-9_], 1 < length < 128"
    shardIdInvalid       string = "shardId is invalid"
    shardListInvalid     string = "shard list is emtpy"
    parameterNumInvalid  string = "parameter num invalid"
    parameterTypeInvalid string = "parameter type is invalid,please check your input parameter type"
    missingRecordSchema  string = "missing record schema for tuple record type"
)

// return the specific err type by errCode,
// you can handle the error by type assert
func errorHandler(statusCode int, requestId string, errorCode string, message string) error {

    switch errorCode {
    case invalidParameter, invalidSubscription, invalidCursor:
        return NewInvalidParameterError(statusCode, requestId, errorCode, message)
    case resourceNotFound, noSuchTopic, noSuchProject, noSuchSubscription, noSuchShard, noSuchConnector,
        noSuchMeterInfo, noSuchConsumer:
        return NewResourceNotFoundError(statusCode, requestId, errorCode, message)
    case seekOutOfRange:
        return NewSeekOutOfRangeError(statusCode, requestId, errorCode, message)
    case resourceAlreadyExist, projectAlreadyExist, topicAlreadyExist, connectorAlreadyExist:
        return NewResourceExistError(statusCode, requestId, errorCode, message)
    case unAuthorized:
        return NewAuthorizationFailedError(statusCode, requestId, errorCode, message)
    case noPermission:
        return NewNoPermissionError(statusCode, requestId, errorCode, message)
    case operatorDenied:
        return NewInvalidOperationError(statusCode, requestId, errorCode, message)
    case limitExceed:
        return NewLimitExceededError(statusCode, requestId, errorCode, message)
    case subscriptionOffline:
        return NewSubscriptionOfflineError(statusCode, requestId, errorCode, message)
    case offsetReseted:
        return NewSubscriptionOffsetResetError(statusCode, requestId, errorCode, message)
    case offsetSessionClosed, offsetSessionChanged:
        return NewSubscriptionSessionInvalidError(statusCode, requestId, errorCode, message)
    case malformedRecord:
        return NewMalformedRecordError(statusCode, requestId, errorCode, message)
    case consumerGroupInProcess:
        return NewServiceInProcessError(statusCode, requestId, errorCode, message)
    case invalidShardOperation:
        return NewShardSealedError(statusCode, requestId, errorCode, message)
    }
    return NewDatahubClientError(statusCode, requestId, errorCode, message)
}

// create a new DatahubClientError
func NewDatahubClientError(statusCode int, requestId string, code string, message string) *DatahubClientError {
    return &DatahubClientError{StatusCode: statusCode, RequestId: requestId, Code: code, Message: message}
}

// DatahubError struct
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

func NewInvalidParameterErrorWithMessage(message string) *InvalidParameterError {
    return &InvalidParameterError{
        DatahubClientError{
            StatusCode: -1,
            RequestId:  "",
            Code:       "",
            Message:    message,
        },
    }
}

func NewInvalidParameterError(statusCode int, requestId string, code string, message string) *InvalidParameterError {
    return &InvalidParameterError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

// InvalidParameterError represent the parameter error
type InvalidParameterError struct {
    DatahubClientError
}

func NewResourceNotFoundError(statusCode int, requestId string, code string, message string) *ResourceNotFoundError {
    return &ResourceNotFoundError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type ResourceNotFoundError struct {
    DatahubClientError
}

func NewResourceExistError(statusCode int, requestId string, code string, message string) *ResourceExistError {
    return &ResourceExistError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type ResourceExistError struct {
    DatahubClientError
}

func NewInvalidOperationError(statusCode int, requestId string, code string, message string) *InvalidOperationError {
    return &InvalidOperationError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type InvalidOperationError struct {
    DatahubClientError
}

func NewLimitExceededError(statusCode int, requestId string, code string, message string) *LimitExceededError {
    return &LimitExceededError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type LimitExceededError struct {
    DatahubClientError
}

func NewAuthorizationFailedError(statusCode int, requestId string, code string, message string) *AuthorizationFailedError {
    return &AuthorizationFailedError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type AuthorizationFailedError struct {
    DatahubClientError
}

//func (afe *AuthorizationFailureError) Error() string {
//    return afe.DatahubClientError.Error()
//}

func NewNoPermissionError(statusCode int, requestId string, code string, message string) *NoPermissionError {
    return &NoPermissionError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type NoPermissionError struct {
    DatahubClientError
}

func NewSeekOutOfRangeError(statusCode int, requestId string, code string, message string) *SeekOutOfRangeError {
    return &SeekOutOfRangeError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type SeekOutOfRangeError struct {
    DatahubClientError
}

func NewSubscriptionOfflineError(statusCode int, requestId string, code string, message string) *SubscriptionOfflineError {
    return &SubscriptionOfflineError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type SubscriptionOfflineError struct {
    DatahubClientError
}

func NewSubscriptionOffsetResetError(statusCode int, requestId string, code string, message string) *SubscriptionOffsetResetError {
    return &SubscriptionOffsetResetError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type SubscriptionOffsetResetError struct {
    DatahubClientError
}

func NewSubscriptionSessionInvalidError(statusCode int, requestId string, code string, message string) *SubscriptionSessionInvalidError {
    return &SubscriptionSessionInvalidError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type SubscriptionSessionInvalidError struct {
    DatahubClientError
}

func NewMalformedRecordError(statusCode int, requestId string, code string, message string) *MalformedRecordError {
    return &MalformedRecordError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type MalformedRecordError struct {
    DatahubClientError
}

func NewServiceInProcessError(statusCode int, requestId string, code string, message string) *ServiceInProcessError {
    return &ServiceInProcessError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type ServiceInProcessError struct {
    DatahubClientError
}

func NewShardSealedError(statusCode int, requestId string, code string, message string) *ShardSealedError {
    return &ShardSealedError{
        DatahubClientError{
            StatusCode: statusCode,
            RequestId:  requestId,
            Code:       code,
            Message:    message,
        },
    }
}

type ShardSealedError struct {
    DatahubClientError
}
