package errors

import (
	"fmt"
)

/*
datahub errors
*/

// DatahubError base datahub error struct
type DatahubError struct {
	// Code datahub error code
	Code string `json:"ErrorCode"`

	// Message detail error msg of the error code
	Message string `json:"ErrorMessage"`
}

func (err DatahubError) Error() string {
	return fmt.Sprintf("errcode: %s, errmsg: %s", err.Code, err.Message)
}

// ObjectAlreadyExistError
type ObjectAlreadyExistError struct {
	DatahubError
}

// NoSuchObjectError
type NoSuchObjectError struct {
	DatahubError
}

// InvalidParameterError
type InvalidParameterError struct {
	DatahubError
}

// InvalidShardOperationError
type InvalidShardOperationError struct {
	DatahubError
}

// MalformedRecordError
type MalformedRecordError struct {
	DatahubError
}

// LimitExceededError
type LimitExceededError struct {
	DatahubError
}

// ServerInternalError
type ServerInternalError struct {
	DatahubError
}

func NewError(code, message string) error {
	err := DatahubError{
		Code:    code,
		Message: message,
	}
	switch code {
	case "NoSuchProject", "NoSuchTopic", "NoSuchShard":
		return NoSuchObjectError{
			DatahubError: err,
		}
	case "InvalidShardOperation":
		return InvalidShardOperationError{
			DatahubError: err,
		}
	case "MalformedRecord":
		return MalformedRecordError{
			DatahubError: err,
		}
	case "LimitExceeded":
		return LimitExceededError{
			DatahubError: err,
		}
	case "InvalidParameter", "InvalidCursor":
		return InvalidParameterError{
			DatahubError: err,
		}
	case "ProjectAlreadyExist", "TopicAlreadyExist":
		return ObjectAlreadyExistError{
			DatahubError: err,
		}
	case "ServiceUnavailable":
		return ServerInternalError{
			DatahubError: err,
		}
	default:
		return err
	}
}
