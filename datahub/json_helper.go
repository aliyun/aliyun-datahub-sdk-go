package datahub

import (
	jsoniter "github.com/json-iterator/go"
)

var parser = jsoniter.Config{
	UseNumber: true,
}.Froze()

func parseJson(buf []byte) (map[string]any, error) {
	obj := make(map[string]any)
	err := parser.Unmarshal(buf, &obj)
	if err != nil {
		return nil, err
	}

	return obj, nil
}

type JsonParseOption func(*jsonParseConfig) error

func newDefaultJsonParseConfig() *jsonParseConfig {
	return &jsonParseConfig{
		ignoreNotExistKey: false,
	}
}

func getJsonParseConfig(opts ...JsonParseOption) (*jsonParseConfig, error) {
	config := newDefaultJsonParseConfig()
	for _, opt := range opts {
		if err := opt(config); err != nil {
			return nil, err
		}
	}
	return config, nil
}

type jsonParseConfig struct {
	ignoreNotExistKey bool
}

func WithIgnoreNotExistKey(b bool) JsonParseOption {
	return func(o *jsonParseConfig) error {
		o.ignoreNotExistKey = b
		return nil
	}
}
