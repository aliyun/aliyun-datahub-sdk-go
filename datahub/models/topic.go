package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

// Topic
type Topic struct {
	Name           string           `json:"Name"`
	ProjectName    string           `json:"ProjectName"`
	ShardCount     int              `json:"ShardCount"`
	Lifecycle      int              `json:"Lifecycle"`
	RecordType     types.RecordType `json:"RecordType"`
	RecordSchema   *RecordSchema    `json:"RecordSchema"`
	CreateTime     uint64           `json:"CreateTime"`
	LastModifyTime uint64           `json:"LastModifyTime"`
	Comment        string           `json:"Comment"`
}

func (t *Topic) String() string {
	pbytes, _ := json.Marshal(t)
	return string(pbytes)
}

func (t *Topic) Resource(method string) string {
	return fmt.Sprintf("/projects/%s/topics/%s", t.ProjectName, t.Name)
}

func (t *Topic) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodPut:
		if t.Lifecycle <= 0 || t.Lifecycle > 7 {
			return nil, errors.New(fmt.Sprintf("life cycle must be in [1~7]"))
		} else if len(t.Comment) == 0 {
			return nil, errors.New(fmt.Sprintf("comment info must not be empty"))
		}
		reqMsg := struct {
			Lifecycle int    `json:"Lifecycle"`
			Comment   string `json:"Comment"`
		}{
			Lifecycle: t.Lifecycle,
			Comment:   t.Comment,
		}
		return json.Marshal(reqMsg)
	case http.MethodPost:
		var reqMsg interface{}
		switch t.RecordType {
		case types.BLOB:
			reqMsg = struct {
				Action     string `json:"Action"`
				ShardCount int    `json:"ShardCount"`
				Lifecycle  int    `json:"Lifecycle"`
				RecordType string `json:"RecordType"`
				Comment    string `json:"Comment"`
			}{
				Action:     "create",
				ShardCount: t.ShardCount,
				Lifecycle:  t.Lifecycle,
				RecordType: t.RecordType.String(),
				Comment:    t.Comment,
			}
		case types.TUPLE:
			if t.RecordSchema == nil {
				return nil, errors.New(fmt.Sprintf("tuple record type must be set record schema"))
			}
			reqMsg = struct {
				Action       string `json:"Action"`
				ShardCount   int    `json:"ShardCount"`
				Lifecycle    int    `json:"Lifecycle"`
				RecordType   string `json:"RecordType"`
				RecordSchema string `json:"RecordSchema"`
				Comment      string `json:"Comment"`
			}{
				Action:       "create",
				ShardCount:   t.ShardCount,
				Lifecycle:    t.Lifecycle,
				RecordType:   t.RecordType.String(),
				RecordSchema: t.RecordSchema.String(),
				Comment:      t.Comment,
			}
		default:
			return nil, errors.New(fmt.Sprintf("record type %q not support", t.RecordType))
		}
		return json.Marshal(reqMsg)
	default:
		return nil, nil
	}
}

func (t *Topic) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodGet:
		var respMsg struct {
			ShardCount     int    `json:"ShardCount"`
			Lifecycle      int    `json:"Lifecycle"`
			RecordType     string `json:"RecordType"`
			RecordSchema   string `json:"RecordSchema"`
			Comment        string `json:"Comment"`
			CreateTime     uint64 `json:"CreateTime"`
			LastModifyTime uint64 `json:"LastModifyTime"`
		}
		err := json.Unmarshal(body, &respMsg)
		if err != nil {
			return err
		}
		t.ShardCount = respMsg.ShardCount
		t.Lifecycle = respMsg.Lifecycle
		t.CreateTime = respMsg.CreateTime
		t.LastModifyTime = respMsg.LastModifyTime
		t.Comment = respMsg.Comment
		t.RecordType = types.RecordType(respMsg.RecordType)
		if t.RecordType == types.TUPLE {
			t.RecordSchema = &RecordSchema{}
			err = json.Unmarshal([]byte(respMsg.RecordSchema), t.RecordSchema)
			if err != nil {
				return err
			}
		} else {
			t.RecordSchema = nil
		}
	}
	return nil
}

// Topics 用来获取某个project下的所有topic列表
type Topics struct {
	ProjectName string   `json:"ProjectName"`
	Names       []string `json:"TopicNames"`
}

func (ts *Topics) String() string {
	tsbytes, _ := json.Marshal(ts)
	return string(tsbytes)
}

func (ts *Topics) Resource(method string) string {
	return fmt.Sprintf("/projects/%s/topics", ts.ProjectName)
}

func (ts *Topics) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodGet:
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Topics not support method %s", method))
	}
}

func (ts *Topics) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodGet:
		return json.Unmarshal(body, ts)
	default:
		return errors.New(fmt.Sprintf("Topics not support method %s", method))
	}
}
