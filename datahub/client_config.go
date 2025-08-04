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
	SendStrategy         SendStrategy
	Parittioner          PartitionFunc
	Protocol             Protocol
	MaxAsyncFlightingNum int
	MaxAsyncBufferNum    int
	MaxAsyncBufferTime   time.Duration
	EnableSuccessCh      bool
	EnableErrorCh        bool
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		BaseConfig: BaseConfig{
			MaxRetry:      3,
			RetryInterval: 500 * time.Millisecond,
		},
		SendStrategy:         RoundRobin,
		Parittioner:          DefaultPartitionFunc,
		Protocol:             Batch,
		MaxAsyncFlightingNum: 16,
		MaxAsyncBufferNum:    1000,
		MaxAsyncBufferTime:   5 * time.Second,
		EnableSuccessCh:      true,
		EnableErrorCh:        true,
	}
}
