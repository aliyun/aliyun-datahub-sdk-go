package datahub

import (
	"net/http"
	"time"
)

type SendStrategy int

const (
	RoundRobin SendStrategy = iota
	Random
)

// BaseConfig contains settings shared by producers and consumers.
// Defaults described below are provided by NewProducerConfig and NewConsumerConfig.
type BaseConfig struct {
	Account Account // required authentication credentials
	// UserAgent is appended to the SDK's User-Agent string.
	UserAgent string
	Endpoint  string // required DataHub service endpoint
	Project   string // required project name
	Topic     string // required topic name
	// MaxRetry is the number of retries for writes and consumer fetches after
	// retryable errors. Defaults to 3; zero disables retries, negative values
	// keep retrying without a count limit.
	MaxRetry int
	// RetryInterval is the retry delay, default 500ms. Producers use 100ms
	// instead when the server returns a throttling error.
	RetryInterval time.Duration
	// RequestTimeout limits each HTTP request, including reading its response body.
	// Zero or negative values keep the default of no overall request timeout.
	// Retries and retry intervals are not included.
	RequestTimeout time.Duration
	// ConnectTimeout limits TCP connection establishment, default 10s.
	// Zero or negative values keep the underlying default of 10 seconds.
	ConnectTimeout time.Duration
	// ResponseHeaderTimeout limits the wait for response headers after the request
	// has been fully written, default 30s. It does not include reading the response
	// body. Zero or negative values disable this timeout.
	ResponseHeaderTimeout time.Duration
}

func defaultBaseConfig() BaseConfig {
	return BaseConfig{
		MaxRetry:              3,
		RetryInterval:         500 * time.Millisecond,
		ConnectTimeout:        10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}

func (cfg *BaseConfig) newClientConfig() *Config {
	config := NewDefaultConfig()
	if cfg.RequestTimeout > 0 {
		config.HttpClient.Timeout = cfg.RequestTimeout
	}
	if cfg.ConnectTimeout > 0 {
		config.HttpClient.Transport.(*http.Transport).DialContext = TraceDialContext(cfg.ConnectTimeout)
	}
	config.HttpClient.Transport.(*http.Transport).ResponseHeaderTimeout = cfg.ResponseHeaderTimeout
	return config
}

// ProducerConfig configures synchronous and asynchronous producers.
// Use NewProducerConfig to initialize the defaults before overriding fields.
type ProducerConfig struct {
	BaseConfig
	// SendStrategy selects shards for synchronous Send calls, default RoundRobin.
	// Async producers distribute unassigned batches round-robin independently
	// of this setting. SendByShard always uses the specified shard.
	SendStrategy SendStrategy
	// Parittioner selects a shard for each async record, default DefaultPartitionFunc.
	// The default uses ShardId first, then PartitionKey. An empty result sends
	// the record through the shared batch buffer for round-robin distribution.
	Parittioner PartitionFunc
	// Protocol is Batch (default) or Protobuf. Topic metadata may override it;
	// schema-enabled topics always use Batch.
	Protocol Protocol
	// MaxAsyncFlightingNum is the pending batch queue capacity for the shared
	// buffer and each shard buffer, default 16. Full queues apply backpressure;
	// this is not an HTTP concurrency limit. Used only by async producers.
	MaxAsyncFlightingNum int
	// MaxAsyncBufferNum is the async batching record-count threshold, default 1000.
	MaxAsyncBufferNum int
	// MaxAsyncBufferTime is the async batching wait duration, default 5s.
	// A batch is also flushed when it reaches MaxAsyncBufferNum.
	MaxAsyncBufferTime time.Duration
	// EnableSuccessCh enables async success results, default true.
	// When enabled, drain Successes() throughout writing and Close to avoid blocking.
	EnableSuccessCh bool
	// EnableErrorCh enables async failure results, default true.
	// When enabled, drain Errors() throughout writing and Close to avoid blocking.
	EnableErrorCh bool
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		BaseConfig:           defaultBaseConfig(),
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

// ConsumerConfig configures a consumer.
// Use NewConsumerConfig to initialize the defaults before overriding fields.
type ConsumerConfig struct {
	BaseConfig
	SubId string // required subscription ID
	// AutoRecordAck acknowledges records when Read returns them, default true.
	// Set false to call record.GetRecordKey().Ack() after successful processing.
	// ACK marks records for offset commits; it does not immediately commit them.
	AutoRecordAck    bool
	FetchNumber      int // records per fetch, default 500, max 1000
	BufferNumber     int // local record channel capacity, default 500
	MaxInflightFetch int // maximum concurrent fetches, default 2
	// Protocol is Batch (default) or Protobuf. Topic metadata may override it;
	// schema-enabled topics always use Batch.
	Protocol Protocol
	// FetchStrategy selects ready shards, default FetchRoundRobin.
	// FetchBalance prioritizes the shard with the oldest last-record SystemTime.
	FetchStrategy  FetchStrategy
	CommitInterval time.Duration // offset commit interval, default 30s
	// SessionTimeout is the consumer group session timeout, default 60s.
	// It is independent of Read's wait duration and HTTP request timeouts.
	SessionTimeout time.Duration
}

// NewConsumerConfig creates a new ConsumerConfig with default values
func NewConsumerConfig() *ConsumerConfig {
	return &ConsumerConfig{
		BaseConfig:       defaultBaseConfig(),
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
