package models

/*
models 包提供了各个Datahub对象的实现。
*/

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
)

// Project
type Project struct {
	Name           string `json:"Name"`
	CreateTime     uint64 `json:"CreateTime"`
	LastModifyTime uint64 `json:"LastModifyTime"`
	Comment        string `json:"Comment"`
}

func (p *Project) String() string {
	pbytes, _ := json.Marshal(p)
	return string(pbytes)
}

func (p *Project) Resource(method string) string {
	return fmt.Sprintf("/projects/%s", p.Name)
}

func (p *Project) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodGet:
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Project not support method %s", method))
	}
}

func (p *Project) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodGet:
		return json.Unmarshal(body, p)
	default:
		return errors.New(fmt.Sprintf("Project not support method %s", method))
	}
}

// Projects 用来获取集群里project列表
type Projects struct {
	Names []string `json:"ProjectNames"`
}

func (ps *Projects) String() string {
	psbytes, _ := json.Marshal(ps)
	return string(psbytes)
}

func (ps *Projects) Resource(method string) string {
	return fmt.Sprintf("/projects")
}

func (ps *Projects) RequestBodyEncode(method string) ([]byte, error) {
	switch method {
	case http.MethodGet:
		return nil, nil
	default:
		return nil, errors.New(fmt.Sprintf("Projects not support method %s", method))
	}
}

func (ps *Projects) ResponseBodyDecode(method string, body []byte) error {
	switch method {
	case http.MethodGet:
		return json.Unmarshal(body, ps)
	default:
		return errors.New(fmt.Sprintf("Projects not support method %s", method))
	}
}
