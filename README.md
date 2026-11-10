# DataHub Go SDK

English | [简体中文](README.zh-CN.md)

A Go SDK for reading and writing DataHub records, with synchronous Producer, asynchronous Producer, and Consumer APIs. This guide describes the current repository implementation.

## Installation

Requires Go 1.23.0 or later. Run inside an existing Go module:

```sh
go get github.com/aliyun/aliyun-datahub-sdk-go/datahub
```

## Getting started

Prepare an accessible Endpoint, Project, and Topic. Consumption also requires a Subscription ID. Match topic types, field names, and field types to your resources.

The snippets use these imports; keep only those needed:

```go
import (
    "fmt"
    "os"
    "sync"
    "time"

    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)
```

Provide AccessKey credentials through environment variables:

```sh
export ALIBABA_CLOUD_ACCESS_KEY_ID="<access-key-id>"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<access-key-secret>"
```

Create a Producer configuration. Use it separately with either writing example below:

```go
cfg := datahub.NewProducerConfig()
cfg.Account = datahub.NewAliyunAccount(
    os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
    os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
)
cfg.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
cfg.Project = "your_project"
cfg.Topic = "your_blob_topic"
```

The writing snippets use a Blob topic. See the Tuple section for structured records.

## Synchronous writing

Build a batch in your application. `Send` returns the server's write result:

```go
producer := datahub.NewProducer(cfg)
if err := producer.Init(); err != nil {
    panic(err)
}
defer producer.Close()

records := []datahub.IRecord{
    datahub.NewBlobRecord([]byte("hello DataHub")),
}
details, err := producer.Send(records)
if err != nil {
    panic(err)
}
fmt.Printf("shard=%s requestId=%s\n", details.ShardId, details.RequestId)
```

Use `SendByShard(records, shardId)` to select a shard explicitly. Synchronous `Send` selects a shard using `SendStrategy`, not record PartitionKey values or `Parittioner`.

## Asynchronous writing

The SDK batches records automatically. Continuously drain the success and error channels, enabled by default, to avoid blocking writes and shutdown:

```go
producer := datahub.NewAsyncProducer(cfg)
if err := producer.Init(); err != nil {
    panic(err)
}

var wg sync.WaitGroup
wg.Add(2)
go func() {
    defer wg.Done()
    for result := range producer.Successes() {
        fmt.Printf("shard=%s records=%d\n", result.ShardId, len(result.Records))
    }
}()
go func() {
    defer wg.Done()
    for result := range producer.Errors() {
        fmt.Printf("write failed: %v, records=%d\n", result.Err, len(result.Records))
        // Handle failed records according to application requirements.
    }
}()

record := datahub.NewBlobRecord([]byte("hello DataHub"))
record.SetPartitionKey("customer-001") // Optional: select a shard by partition key
producer.Input() <- record

producer.Flush() // Optional: enqueue accepted records; does not wait for server responses.
err := producer.Close()
wg.Wait()
if err != nil {
    panic(err)
}
```

Stop writing to `Input()` before `Close()`, and keep draining results during shutdown. Determine write success from the result channels. Do not call `Flush()` concurrently with `Close()`.

## Tuple records

For a Tuple topic, retrieve its schema after initializing the Producer. Use matching fields and check every assignment:

```go
schema, err := producer.GetSchema()
if err != nil {
    panic(err)
}
record := datahub.NewTupleRecord(schema)
if err := record.SetValueByName("string_field", "hello"); err != nil {
    panic(err)
}
if err := record.SetValueByName("integer_field", int64(123)); err != nil {
    panic(err)
}
// Sync: producer.Send([]datahub.IRecord{record})
// Async: producer.Input() <- record
```

## Consuming records

By default, records are automatically acknowledged when `Read` returns them. Set `AutoRecordAck = false` to acknowledge only after successful processing:

```go
cfg := datahub.NewConsumerConfig()
cfg.Account = datahub.NewAliyunAccount(
    os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_ID"),
    os.Getenv("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
)
cfg.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
cfg.Project = "your_project"
cfg.Topic = "your_topic"
cfg.SubId = "your_subscription_id"
cfg.AutoRecordAck = false

consumer := datahub.NewConsumer(cfg)
if err := consumer.Init(); err != nil {
    panic(err)
}
defer consumer.Close()

for {
    record, err := consumer.Read(3 * time.Second)
    if err != nil {
        panic(err)
    }
    if record == nil {
        continue
    }
    fmt.Printf("record: %v\n", record) // Replace with application processing; ACK after success.
    record.GetRecordKey().Ack()
}
```

ACK marks records as processed. Offsets are committed periodically according to `CommitInterval`, not immediately on every ACK. `Read(0)` is non-blocking; a read wait timeout returns `nil, nil`. Read waits are independent of HTTP request timeouts.

## Configuration

Start with `NewProducerConfig()` or `NewConsumerConfig()`, then override fields. Zero-value structs do not automatically receive these defaults. Duration fields use `time.Duration`.

### Shared parameters (BaseConfig)

| Parameter | Type | Required | Default | Scope / description |
| --- | --- | --- | --- | --- |
| Account | Account | Yes | None | All clients; authentication credentials |
| Endpoint | string | Yes | Empty | All clients; DataHub service endpoint |
| Project | string | Yes | Empty | All clients; project name |
| Topic | string | Yes | Empty | All clients; topic name |
| UserAgent | string | No | Empty | All clients; identifier appended to the SDK User-Agent |
| MaxRetry | int | No | 3 | Retries for writes / consumer fetches; 0 disables retries, negative values keep retrying; only applies to retryable errors |
| RetryInterval | time.Duration | No | 500ms | Retry interval; Producers use 100ms for throttling errors |
| RequestTimeout | time.Duration | No | 0 | All clients; total timeout per HTTP request, including connection, sending, and response body reading; non-positive values leave it unset |
| ConnectTimeout | time.Duration | No | 0 (effective 10s) | All clients; connection establishment timeout; non-positive values preserve the underlying 10s default |

Both timeout settings cover initialization and subsequent HTTP requests. They do not bound total time spent on retries, backoff, asynchronous queues, or application processing. The underlying response-header timeout remains 100s by default; `RequestTimeout = 0` does not make every phase unlimited.

### Producer parameters (ProducerConfig)

All parameters below are optional. Synchronous and asynchronous Producers share this configuration type.

| Parameter | Type | Default | Scope / description |
| --- | --- | --- | --- |
| SendStrategy | SendStrategy | RoundRobin | Synchronous Send shard selection: RoundRobin or Random; async unassigned batches currently rotate through shards independently of this field |
| Parittioner | PartitionFunc | DefaultPartitionFunc | Async partition function; the default uses record ShardId first, then PartitionKey; unassigned records are batched and distributed round-robin |
| Protocol | Protocol | Batch | Sync / async serialization: Batch or Protobuf; topic metadata may override it; schema-enabled topics use Batch |
| MaxAsyncFlightingNum | int | 16 | Async pending batch queue capacity; limits buffering and applies backpressure when full; not an HTTP concurrency limit |
| MaxAsyncBufferNum | int | 1000 | Async batching record-count threshold |
| MaxAsyncBufferTime | time.Duration | 5s | Async batching wait duration |
| EnableSuccessCh | bool | true | Async success channel switch; drain Successes() when enabled |
| EnableErrorCh | bool | true | Async error channel switch; drain Errors() when enabled |

`Parittioner` and `MaxAsyncFlightingNum` retain their existing API spellings. Use positive buffer sizes, capacities, and durations.

### Consumer parameters (ConsumerConfig)

| Parameter | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| SubId | string | Yes | Empty | Subscription ID |
| AutoRecordAck | bool | No | true | Automatically ACK when Read returns records; otherwise call GetRecordKey().Ack() manually |
| FetchNumber | int | No | 500 | Records per fetch; maximum 1000 |
| BufferNumber | int | No | 500 | Local record channel capacity |
| MaxInflightFetch | int | No | 2 | Maximum concurrent fetches |
| Protocol | Protocol | No | Batch | Batch / Protobuf; topic metadata may override it; schema-enabled topics use Batch |
| FetchStrategy | FetchStrategy | No | FetchRoundRobin | FetchRoundRobin rotates through shards; FetchBalance prioritizes the shard with the oldest last-record SystemTime |
| CommitInterval | time.Duration | No | 30s | Offset commit interval |
| SessionTimeout | time.Duration | No | 60s | Consumer group session timeout; separate from Read and HTTP timeouts |

Use positive counts, concurrency limits, and intervals.

## Complete examples

Update credentials, Endpoint, Project, Topic, schema fields, and Subscription ID as appropriate, then run from the repository root:

| Scenario | Example | Command |
| --- | --- | --- |
| Synchronous writing | [producer](examples/producer/producer.go) | `go run ./examples/producer` |
| Asynchronous writing and PartitionKey | [async_producer](examples/async_producer/async_producer.go) | `go run ./examples/async_producer` |
| Consumption with automatic / manual ACK | [consumer](examples/consumer/consumer.go) | `go run ./examples/consumer` |

Reference: [official Go High-Level SDK documentation](https://help.aliyun.com/zh/datahub/developer-reference/go-high-level-sdk). Parameters and Consumer usage here follow the current repository code.

## License

[Apache License 2.0](LICENSE)
