# DataHub Go SDK

[English](README.md) | 简体中文

用于 DataHub 数据读写的 Go SDK，支持同步 Producer、异步 Producer 和 Consumer。本文描述当前仓库实现。

## 安装

需要 Go 1.23.0 或更高版本。在已有 Go Module 中执行：

```sh
go get github.com/aliyun/aliyun-datahub-sdk-go/datahub
```

## 开始使用

提前准备可访问的 Endpoint、Project 和 Topic；消费还需要 Subscription ID。示例中的 Topic 类型、字段名和字段类型必须与实际资源一致。

以下片段使用这些导入（按实际使用保留）：

```go
import (
    "fmt"
    "os"
    "sync"
    "time"

    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)
```

通过环境变量提供 AccessKey：

```sh
export ALIBABA_CLOUD_ACCESS_KEY_ID="<access-key-id>"
export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<access-key-secret>"
```

创建 Producer 配置；下面的同步和异步示例分别使用此配置：

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

以下写入片段使用 Blob Topic；Tuple Topic 用法见后文。

## 同步写入

调用方自行组装一批记录，`Send` 返回服务端写入结果：

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

用 `SendByShard(records, shardId)` 可指定 Shard。同步 `Send` 按 `SendStrategy` 选择 Shard，不使用记录上的 PartitionKey 或 `Parittioner`。

## 异步写入

SDK 自动攒批。默认开启的成功、失败通道必须持续消费，否则可能阻塞发送和关闭：

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
        // 根据业务要求处理失败记录。
    }
}()

record := datahub.NewBlobRecord([]byte("hello DataHub"))
record.SetPartitionKey("customer-001") // 可选：按分区键选择 Shard
producer.Input() <- record

producer.Flush() // 可选：将已接收的数据移入发送队列，不等待服务端响应。
err := producer.Close()
wg.Wait()
if err != nil {
    panic(err)
}
```

`Close()` 前停止向 `Input()` 写入；关闭期间继续消费结果通道。写入是否成功以结果通道为准。`Flush()` 不可与 `Close()` 并发调用。

## Tuple 数据

对于 Tuple Topic，可在 Producer 初始化后读取 Schema 并构造记录；字段必须与实际 Schema 一致，每次赋值都要检查错误：

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
// 同步：producer.Send([]datahub.IRecord{record})
// 异步：producer.Input() <- record
```

## 消费数据

默认在 `Read` 返回记录时自动 ACK。需要处理成功后再确认时，设置 `AutoRecordAck = false`：

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
    fmt.Printf("record: %v\n", record) // 替换为业务处理；成功后再 ACK。
    record.GetRecordKey().Ack()
}
```

ACK 标记记录已处理，位点按 `CommitInterval` 周期提交，并非每次 ACK 都立即提交。`Read(0)` 非阻塞；等待超时返回 `nil, nil`。`Read` 的等待时间与 HTTP 请求超时独立。

## 参数说明

使用 `NewProducerConfig()` / `NewConsumerConfig()` 获取默认值后再修改；直接创建零值结构体不会自动补齐这些默认值。时间参数类型均为 `time.Duration`。

### 通用参数（BaseConfig）

| 参数 | 类型 | 必填 | 默认值 | 适用范围 / 含义 |
| --- | --- | --- | --- | --- |
| Account | Account | 是 | 无 | 所有客户端；身份凭据 |
| Endpoint | string | 是 | 空 | 所有客户端；DataHub 服务地址 |
| Project | string | 是 | 空 | 所有客户端；项目名称 |
| Topic | string | 是 | 空 | 所有客户端；Topic 名称 |
| UserAgent | string | 否 | 空 | 所有客户端；追加到 SDK User-Agent 的标识 |
| MaxRetry | int | 否 | 3 | 写入 / 消费拉取的重试次数；0 不重试，负数持续重试，仅对可重试错误生效 |
| RetryInterval | time.Duration | 否 | 500ms | 重试间隔；Producer 遇到限流错误时使用 100ms |
| RequestTimeout | time.Duration | 否 | 0 | 所有客户端；单次 HTTP 请求总超时，含连接、发送和读取响应体；非正值不设置总超时 |
| ConnectTimeout | time.Duration | 否 | 0（实际 10s） | 所有客户端；连接建立超时；非正值沿用底层 10s |

两个超时参数均覆盖初始化和后续 HTTP 请求，但不包括多次重试、退避、异步排队或业务处理的总耗时。默认仍有底层响应头等待上限 100s；`RequestTimeout = 0` 不代表所有阶段无限等待。

### Producer 参数（ProducerConfig）

以下参数均为可选。同步和异步 Producer 共用同一个配置类型。

| 参数 | 类型 | 默认值 | 适用范围 / 含义 |
| --- | --- | --- | --- |
| SendStrategy | SendStrategy | RoundRobin | 同步 Send 的分片策略：RoundRobin 轮询、Random 随机；当前异步未指定 Shard 的批次独立采用轮询，不受此字段控制 |
| Parittioner | PartitionFunc | DefaultPartitionFunc | 异步分片函数；默认优先使用记录的 ShardId，再按 PartitionKey 分片，未选出 Shard 的记录攒批后轮询发送 |
| Protocol | Protocol | Batch | 同步 / 异步序列化协议，可选 Batch、Protobuf；Topic 元数据可能覆盖配置，启用 Schema 时使用 Batch |
| MaxAsyncFlightingNum | int | 16 | 异步待发送批次队列容量，用于限制缓冲，满时产生背压；不是 HTTP 并发数 |
| MaxAsyncBufferNum | int | 1000 | 异步攒批记录条数阈值 |
| MaxAsyncBufferTime | time.Duration | 5s | 异步攒批等待时间 |
| EnableSuccessCh | bool | true | 异步成功结果通道开关，开启后必须消费 Successes() |
| EnableErrorCh | bool | true | 异步失败结果通道开关，开启后必须消费 Errors() |

`Parittioner` 和 `MaxAsyncFlightingNum` 为现有 API 的实际拼写。缓冲数量、容量和时间使用正值。

### Consumer 参数（ConsumerConfig）

| 参数 | 类型 | 必填 | 默认值 | 含义 |
| --- | --- | --- | --- | --- |
| SubId | string | 是 | 空 | Subscription ID |
| AutoRecordAck | bool | 否 | true | Read 返回记录时自动 ACK；关闭后需手动调用 GetRecordKey().Ack() |
| FetchNumber | int | 否 | 500 | 每次拉取的记录数量，最大 1000 |
| BufferNumber | int | 否 | 500 | 本地记录通道容量 |
| MaxInflightFetch | int | 否 | 2 | 最大并发拉取数量 |
| Protocol | Protocol | 否 | Batch | Batch / Protobuf；Topic 元数据可能覆盖配置，启用 Schema 时使用 Batch |
| FetchStrategy | FetchStrategy | 否 | FetchRoundRobin | FetchRoundRobin 轮询；FetchBalance 优先选择上次记录 SystemTime 较早的 Shard |
| CommitInterval | time.Duration | 否 | 30s | 消费位点提交周期 |
| SessionTimeout | time.Duration | 否 | 60s | 消费组会话超时，不是 Read 或 HTTP 超时 |

数量、并发和周期参数使用正值。

## 完整示例

修改示例中的凭据、Endpoint、Project、Topic、Schema 字段和 Subscription ID 后，在仓库根目录运行：

| 场景 | 示例 | 命令 |
| --- | --- | --- |
| 同步写入 | [producer](examples/producer/producer.go) | `go run ./examples/producer` |
| 异步写入与 PartitionKey | [async_producer](examples/async_producer/async_producer.go) | `go run ./examples/async_producer` |
| 自动 / 手动 ACK 消费 | [consumer](examples/consumer/consumer.go) | `go run ./examples/consumer` |

参考：[官方 Go High-Level SDK 文档](https://help.aliyun.com/zh/datahub/developer-reference/go-high-level-sdk)。本文的参数及 Consumer 用法以当前仓库代码为准。

## License

[Apache License 2.0](LICENSE)
