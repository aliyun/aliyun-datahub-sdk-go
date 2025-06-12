package datahub

import "time"

type SendStrategy int

const (
	RoundRobin SendStrategy = iota
	Random
)

type BaseConfig struct {
	Account       Account
	UserAgent     string
	Endpoint      string
	Project       string
	Topic         string
	MaxRetry      int
	RetryInterval time.Duration
}

type ProducerConfig struct {
	BaseConfig
	SendStrategy SendStrategy
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		BaseConfig: BaseConfig{
			MaxRetry:      3,
			RetryInterval: 500 * time.Millisecond,
		},
		SendStrategy: RoundRobin,
	}
}
