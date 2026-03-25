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

// FetchStrategy defines how to select shard when fetching records
type FetchStrategy int

const (
	// FetchRoundRobin fetches records from shards in round-robin order
	FetchRoundRobin FetchStrategy = iota
	// FetchBalance fetches records from shard with oldest SystemTime first
	FetchBalance
)

// ConsumerConfig configuration for consumer
type ConsumerConfig struct {
	BaseConfig
	SubId            string
	AutoRecordAck    bool          // auto ack after read, default true
	FetchNumber      int           // records per fetch, default 500, max 1000
	BufferNumber     int           // local buffer size, default 500
	MaxInflightFetch int           // max concurrent fetching requests, default 2
	Protocol         Protocol      // data protocol
	FetchStrategy    FetchStrategy // shard selection strategy, default FetchRoundRobin
	CommitInterval   time.Duration // offset commit interval, default 30s
	SessionTimeout   time.Duration // consumer group session timeout, default 60s
}

// NewConsumerConfig creates a new ConsumerConfig with default values
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BaseConfig: BaseConfig{
			MaxRetry:      3,
			RetryInterval: 500 * time.Millisecond,
		},
		AutoRecordAck:    true,
		FetchNumber:      500,
		BufferNumber:     500,
		MaxInflightFetch: 2,
		Protocol:         Batch,
		FetchStrategy:    FetchRoundRobin,
		CommitInterval:   30 * time.Second,
		SessionTimeout:   60 * time.Second,
	}
}
