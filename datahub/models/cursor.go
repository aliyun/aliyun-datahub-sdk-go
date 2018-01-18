package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

type Cursor struct {
	Id          string           `json:"Cursor"`
	Sequence    int64            `json:"Sequence"`
	RecordTime  int64            `json:"RecordTime"`
	ProjectName string           `json:"ProjectName"`
	TopicName   string           `json:"TopicName"`
	ShardId     string           `json:"ShardId"`
	Type        types.CursorType `json:"Type"`
	SystemTime  int              `json:"SystemTime"`
}

func (c *Cursor) String() string {
	cbytes, _ := json.Marshal(c)
	return string(cbytes)
}

func (c *Cursor) Resource(method string) string {
	return fmt.Sprintf("/projects/%s/topics/%s/shards/%s", c.ProjectName, c.TopicName, c.ShardId)
}

func (c *Cursor) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodPost:
		if !types.ValidateCursorType(c.Type) {
			return nil, errors.New(fmt.Sprintf("cursor type %q not support", c.Type))
		}
		reqMsg := struct {
			Action     string `json:"Action"`
			SystemTime int    `json:"SystemTime"`
			Type       string `json:"Type"`
		}{
			Action:     "cursor",
			SystemTime: c.SystemTime,
			Type:       c.Type.String(),
		}
		return json.Marshal(reqMsg)
	default:
		return nil, errors.New(fmt.Sprintf("Cursor not support method %s", method))
	}
}

func (c *Cursor) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodPost:
		return json.Unmarshal(body, c)
	default:
		return errors.New(fmt.Sprintf("Cursor not support method %s", method))
	}
}
