# 快速上手
## Datahub相关的基本概念
详情参见[Datahub基本概念](https://help.aliyun.com/document_detail/158776.html)

## 准备工作
- 访问DataHub服务需要使用阿里云认证账号，需要提供阿里云accessId及accessKey。 同时需要提供可访问的DataHub服务地址。
- Datahub Python SDK提供的所有API接口均由 datahub.DataHub 类实现，所以第一步就是初始化一个DataHub对象。可以直接创建默认参数的Datahub对象：

```go
accessId := ""
accessKey := ""
endpoint := ""
dh := datahub.New(accessId, accessKey, endpoint)
```

- 也可以使用自定义参数进行配置，目前支持配置的参数有：

 | 参数           | 参数类型       | 参数选项                       | 参数含义                                                                                          |
 | -------------- | -------------- | ------------------------------ | ------------------------------------------------------------------------------------------------- |
 | UserAgent      | string         | -                              | 用户名代理                                                                                        |
 | CompressorType | CompressorType | NOCOMPRESS、LZ4、DEFLATE、ZLIB | 传输时支持的压缩格式，默认为NOCOMPRESS，不压缩                                                    |
 | EnableBinary   | bool           | true/false                     | 主要在put/get record时，使用protobuf协议。Datahub版本未支持protobuf时需要手动指定enable_pb为False |
 | HttpClient     | *http.Client   | -                              | 具体可参考[net/http](https://pkg.go.dev/net/http@go1.18.3#Client)                                 |

- **因为go中的bool默认为false，所以使用自定义参数，除非特别需要，建议指定```EnableBinary:true```**
```go
accessId := ""
accessKey := ""
endpoint := ""
config := &datahub.Config{
	UserAgent:"***",
	EnableBinary:true,
	CompressorType:datahub.LZ4,
	HttpClient:&http.Client{},
}
dh := datahub.NewClientWithConfig(accessId, accessKey, endpoint,config)
```

## 接口示例

### project 操作
项目（Project）是DataHub数据的基本组织单元,下面包含多个Topic。**需要注意的是**，DataHub的项目空间与MaxCompute的项目空间是相互独立的。用户在MaxCompute中创建的项目不能复用于DataHub，需要独立创建。
#### 创建Project
> CreateProject(projectName, comment string) error

- 参数
	- projectName: project name
	- comment: project comment

- return
- error
	- ResourceExistError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func createProjet(dh datahub.DataHub, projectName string) {
    if err := dh.CreateProject(projectName, "project comment"); err != nil {
        fmt.Println("create project failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create successful")
}
```

#### 删除Project
DeleteProject接口删除project。
> DeleteProject(projectName string) error

- 参数
	- projectName: project name

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例
  
```go
func deleteProject(dh datahub.DataHub, projectName string) {
    if err := dh.DeleteProject("123"); err != nil {
        fmt.Println("delete project failed")
        fmt.Println(err)
        return
    }
    fmt.Println("delete project successful")
}
```

#### 列出Project
ListProject 接口列出project。
> ListProject() (*ListProjectResult, error)

- 参数

- return

```
type ListProjectResult struct {
    ProjectNames []string `json:"ProjectNames"`
}
```

- error
	- AuthorizationFailedError
	- DatahubClientError

- 示例

```go
func listProject(dh datahub.DataHub, projectName string) {
    lp, err := dh.ListProject()
    if err != nil {
        fmt.Println("get project list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get project list successful")
    for _, projectName := range lp.ProjectNames {
        fmt.Println(projectName)
    }
}
```

#### 查询Project
GetProject查询project
> GetProject(projectName string) (*GetProjectResult, error)

- 参数
	- projectName: project name


- return 

```go
type GetProjectResult struct {
    CreateTime     int64  `json:"CreateTime"`
    LastModifyTime int64  `json:"LastModifyTime"`
    Comment        string `json"Comment"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getProject(dh datahub.DataHub, projectName string) {
    gp, err := dh.GetProject(projectName)
    if err != nil {
        fmt.Println("get project message failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get project message successful")
    fmt.Println(*gp)
}
```

#### 更新project
> UpdateProject(projectName, comment string) error

- 参数
	- projectName: project name
	- comment: project comment


- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func updateProject(dh datahub.DataHub, projectName string) {
    if err := dh.UpdateProject(projectName, "new project comment"); err != nil {
        fmt.Println("update project comment failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update project comment successful")
}
```

### topic操作
Topic是 DataHub 订阅和发布的最小单位，用户可以用Topic来表示一类或者一种流数据。目前支持Tuple与Blob两种类型。Tuple类型的Topic支持类似于数据库的记录的数据，每条记录包含多个列。Blob类型的Topic仅支持写入一块二进制数据。

#### 创建Topic
##### Tuple Topic
> CreateTupleTopic(projectName, topicName, comment string, shardCount, lifeCycle int, recordSchema *RecordSchema) error

Tuple类型Topic写入的数据是有格式的，需要指定Record Schema，目前支持以下几种数据类型:

| 类型      | 含义                    | 值域                                       |
| --------- | ----------------------- | ------------------------------------------ |
| BIGINT    | 8字节有符号整型         | -9223372036854775807 ~ 9223372036854775807 |
| DOUBLE    | 8字节双精度浮点数       | -1.0 * 10^308 ~ 1.0 * 10^308               |
| BOOLEAN   | 布尔类型                | True/False或true/false或0/1                |
| TIMESTAMP | 时间戳类型              | 表示到微秒的时间戳类型                     |
| STRING    | 字符串，只支持UTF-8编码 | 单个STRING列最长允许1MB                    |

- 参数
	- projectName: project name
	- topicName: topic name
	- comment: topic comment
	- lifeCycle: The expire time of the data (Unit: DAY). The data written before that time is not accessible.
	- recordSchema: The records schema of this topic.


- return
- error
	- ResourceExistError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func Example_CreateTupleTopic(dh datahub.DataHub, projectName, topicName string) {
    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false}).
        AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
        AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN})
    if err := dh.CreateTupleTopic(projectName, topicName, "topic comment", 5, 7, recordSchema); err != nil {
        fmt.Println("create topic failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create topic successful")
}
```

##### Blob Topic
> CreateBlobTopic(projectName, topicName, comment string, shardCount, lifeCycle int) error

- 参数
	- projectName: project name
	- topicName: topic name
	- comment: topic comment
	- lifeCycle: The expire time of the data (Unit: DAY). The data written before that time is not accessible.



- return
- error
	- ResourceExistError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func Example_CreateBlobTopic(dh datahub.DataHub, projectName, topicName string) {
    if err := dh.CreateBlobTopic(projectName, topicName, "topic comment", 5, 7); err != nil {
        fmt.Println("create topic failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create topic successful")
}
```

#### 删除Topic
> DeleteTopic(projectName, topicName string) error

- 参数
	- projectName: project name
	- topicName: topic name

- return 
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func ExampleDataHub_DeleteTopic(dh datahub.DataHub, projectName, topicName string) {
    if err := dh.DeleteTopic(projectName, topicName); err != nil {
        fmt.Println("delete failed")
        fmt.Println(err)
        return
    }
    fmt.Println("delete successful")
}
```

#### 列出Topic
> ListTopic(projectName string) (*ListTopicResult, error)

- 参数
	- projectName: project name

- return 

```go
type ListTopicResult struct {
    TopicNames [] string `json:"TopicNames"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func ExampleDataHub_ListTopic(dh datahub.DataHub, projectName, topicName string) {
    lt, err := dh.ListTopic(projectName)
    if err != nil {
        fmt.Println("get topic list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get topic list successful")
    fmt.Println(lt)
}
```

#### 更新Topic
> UpdateTopic(projectName, topicName, comment string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- comment: topic comment


- return 
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

``` go
func ExampleDataHub_UpdateTopic(dh datahub.DataHub, projectName, topicName string) {
    if err := dh.UpdateTopic(projectName, topicName, "new topic comment"); err != nil {
        fmt.Println("update topic comment failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update topic comment successful")
}
```

### schema类型
schema是用来标明数据存储的名称和对应类型的，在创建tuple topic 和 读写 record 的时候用到。因为网络传输中，数据都是以字符串的形式发送，需要schema来转换成对应的类型。
schema就是一个Field对象的slice，Field包含三个参数，第一个参数是field的名称，第二个是field的类型，第三个参数是bool值，True表示field的值允许为空， False表示field的值不能为空。

#### 获取schema
对于已创建的Tuple topic，可以使用get_topic接口来获取schema信息

- 示例

```go
func getSchema(dh datahub.DataHub, projectName, topicName string) {
    gt, err := dh.GetTopic(projectName, "topic_test")
    if err != nil {
        fmt.Println("get topic failed")
        fmt.Println(err)
        return
    } else {
        schema := gt.RecordSchema
        fmt.Println(schema)
    }
}
```

#### 定义schema
要创建新的tuple topic,需要自己定义schema，schema可以通过以下方式进行初始化。

- 直接创建

```go
func createSchema1(dh datahub.DataHub, projectName, topicName string) {
    fields := []datahub.Field{
        {"field1", datahub.STRING, true},
        {"field2", datahub.BIGINT, false},
    }
    schema := datahub.RecordSchema{
        fields,
    }

    fmt.Println(schema)
}
```

- 逐个对schema进行set

```go
func createSchema2(dh datahub.DataHub, projectName, topicName string) {
    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false}).
        AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
        AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN})
}

```

- 通过json字符串定义schema

```go
func createSchema3(dh datahub.DataHub, projectName, topicName string) {
    str := ""
    schema, err := datahub.NewRecordSchemaFromJson(str)
    if err != nil {
        fmt.Println("create recordSchema failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create recordSchema successful")
    fmt.Println(schema)
}
```

json字符串的格式如下：

“{“fields”:[{“type”:”BIGINT”,”name”:”a”},{“type”:”STRING”,”name”:”b”}]}”

### shard 操作
Shard表示对一个Topic进行数据传输的并发通道，每个Shard会有对应的ID。每个Shard会有多种状态: Opening - 启动中，Active - 启动完成可服务。每个Shard启用以后会占用一定的服务端资源，建议按需申请Shard数量。shard可以进行合并和分裂，当数据量增大时，可以采用分裂shard来增加数据通道，提高数据写入的并发量，当数据量减小时，应该合并shard减少服务器资源浪费。例如淘宝在双11期间，数据量骤增，这个时候每个shard的写入压力过大，便可以增加shard提高写入效率，在双11过后，数据量明显降低，则需要合并shard。

#### 列出shard
> ListShard(projectName, topicName string) (*ListShardResult, error)

- 参数
	- projectName: project name
	- topicName: topic name


- return 

```go
type SplitShardResult struct {
    NewShards []ShardEntry `json:"NewShards"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func ExampleDataHub_ListShard() {
    ls, err := dh.ListShard(projectName, topicName)
    if err != nil {
        fmt.Println("get shard list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get shard list successful")
    for _, shard := range ls.Shards {
        fmt.Println(shard)
    }
}
```

#### 分裂shard
只有处于ACTIVE状态的shard才可以进行分裂，分裂成功后，会生成两个新的shard，同时原shard状态会变为CLOSED。
分裂shard时，需要指定splitKey，可以采用系调用第一个method，系统将会自动生成spiltKey，如果有特殊需求，则可以采用第二个method自己指定spiltKey。spiltKey规则可以参考基本概念中的[Shard Hash Key Range](https://help.aliyun.com/document_detail/158776.html)。
> SplitShard(projectName, topicName, shardId string) (*SplitShardResult, error)

> SplitShardWithSplitKey(projectName, topicName, shardId, splitKey string) (*SplitShardResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The shard which to split
	- splitKey: The split key which is used to split shard

- return 

```go
type SplitShardResult struct {
    NewShards []ShardEntry `json:"NewShards"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```
func ExampleDataHub_SplitShard() {
    // the shardId of you want to split
    shardId := "0"
    ss, err := dh.SplitShard(projectName, topicName, shardId)
    if err != nil {
        fmt.Println("split shard failed")
        fmt.Println(err)
        return
    }
    fmt.Println("split shard successful")
    fmt.Println(ss)

    // After splitting, you need to wait for all shard states to be ready
    // before you can perform related operations.
    dh.WaitAllShardsReady(projectName, topicName)
}
```

#### 合并shard
合并两个shard时，要求两个shard必须是相邻的，并且状态都是ACTIVE。
> MergeShard(projectName, topicName, shardId, adjacentShardId string) (*MergeShardResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The shard which will be merged
	- adjacentShardId: The adjacent shard of the specified shard.

- 示例



- return 

```go
type MergeShardResult struct {
    ShardId      string `json:"ShardId"`
    BeginHashKey string `json:"BeginHashKey"`
    EndHashKey   string `json:"EndHashKey"`
}
```

- error
	- ResourceNotFoundError
	- InvalidOperationError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
	- ShardSealedError

```go
func ExampleDataHub_MergeShard() {
    shardId := "3"
    adjacentShardId := "4"
    ms, err := dh.MergeShard(projectName, topicName, shardId, adjacentShardId)
    if err != nil {
        fmt.Println("merge shard failed")
        fmt.Println(err)
        return
    }
    fmt.Println("merge shard successful")
    fmt.Println(ms)

    // After splitting, you need to wait for all shard states to be ready
    // before you can perform related operations.
    dh.WaitAllShardsReady(projectName, topicName)
}
```

### 数据发布/订阅
处于ACTIVE和CLOSED状态的shard都可以进行数据订阅，但是只有处于ACTIVE状态的shard可以进行数据发布，向CLOSED状态的shard发布数据会直接返回ShardSealedError错误，处于CLOSED状态的shard读取数据到末尾时也会返回ShardSealedError错误，表示不会有新的数据。
#### 发布数据
向某个topic下发布数据记录时，每条数据记录需要指定该topic下的一个shard, 因此一般需要通过 listShard 接口查看下当前topic下的shard列表。**使用PutRecords接口时注意检查返回结果是否数据发布失败的情况。**
> PutRecords(projectName, topicName string, records []IRecord) (*PutRecordsResult, error)

> PutRecordsByShard(projectName, topicName, shardId string, records []IRecord) error

**服务器2.12版本及之后版本开始支持PutRecordsByShard接口，低版本请使用PutRecords接口。**
- 参数
	- projectName: project name
	- topicName: topic name
	- shardId : id of shard
	- records: Records list to written.

- return 

```go
type PutRecordsResult struct {
    FailedRecordCount int            `json:"FailedRecordCount"`
    FailedRecords     []FailedRecord `json:"FailedRecords"`
}

```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
// put tuple data
func putTupleData() {
    topic, err := dh.GetTopic(projectName, topicName)
    if err != nil {
        fmt.Println("get topic failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get topic successful")

    records := make([]datahub.IRecord, 3)
    record1 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record1.ShardId = "0"
    record1.SetValueByName("field1", "TEST1")
    record1.SetValueByName("field2", 1)
    //you can add some attributes when put record
    record1.SetAttribute("attribute", "test attribute")
    records[0] = record1

    record2 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record2.ShardId = "1"
    record2.SetValueByName("field1", datahub.String("TEST2"))
    record2.SetValueByName("field2", datahub.Bigint(2))
    records[1] = record2

    record3 := datahub.NewTupleRecord(topic.RecordSchema, 0)
    record3.ShardId = "2"
    record3.SetValueByName("field1", datahub.String("TEST3"))
    record3.SetValueByName("field2", datahub.Bigint(3))
    records[2] = record3

    maxReTry := 3
    retryNum := 0
    for retryNum < maxReTry {
        result, err := dh.PutRecords(projectName, topicName, records)
        if err != nil {
            if _, ok := err.(*datahub.LimitExceededError); ok {
                fmt.Println("maybe qps exceed limit,retry")
                retryNum++
                time.Sleep(5 * time.Second)
                continue
            } else {
                fmt.Println("put record failed")
                fmt.Println(err)
                return
            }
        }
        fmt.Printf("put successful num is %d, put records failed num is %d\n", len(records)-result.FailedRecordCount, result.FailedRecordCount)
        for _, v := range result.FailedRecords {
            fmt.Println(v)
        }
        break
    }
    if retryNum >= maxReTry {
        fmt.Printf("put records failed ")
    }
}
// put blob data
func putBlobData() {
    records := make([]datahub.IRecord, 3)
    record1 := datahub.NewBlobRecord([]byte("blob test1"), 0)
    record1.ShardId = "0"
    records[0] = record1

    record2 := datahub.NewBlobRecord([]byte("blob test2"), 0)
    record2.ShardId = "1"
    record2.SetAttribute("attribute", "test attribute")
    records[1] = record2

    record3 := datahub.NewBlobRecord([]byte("blob test3"), 0)
    record3.ShardId = "2"
    records[2] = record3

    maxReTry := 3
    retryNum := 0
    for retryNum < maxReTry {
        result, err := dh.PutRecords(projectName, blobTopicName, records)
        if err != nil {
            if _, ok := err.(*datahub.LimitExceededError); ok {
                fmt.Println("maybe qps exceed limit,retry")
                retryNum++
                time.Sleep(5 * time.Second)
                continue
            } else {
                fmt.Println("put record failed")
                fmt.Println(err)
                return
            }
        }
        fmt.Printf("put successful num is %d, put records failed num is %d\n", len(records)-result.FailedRecordCount, result.FailedRecordCount)
        for _, v := range result.FailedRecords {
            fmt.Println(v)
        }
        break
    }
    if retryNum >= maxReTry {
        fmt.Printf("put records failed ")
    }
}
// put data by shard
func putDataByShard() {
    shardId := "0"
    records := make([]datahub.IRecord, 3)
    record1 := datahub.NewBlobRecord([]byte("blob test1"), 0)
    records[0] = record1

    record2 := datahub.NewBlobRecord([]byte("blob test2"), 0)
    record2.SetAttribute("attribute", "test attribute")
    records[1] = record2

    record3 := datahub.NewBlobRecord([]byte("blob test3"), 0)
    records[2] = record3

    maxReTry := 3
    retryNum := 0
    for retryNum < maxReTry {
        if err := dh.PutRecordsByShard(projectName, blobTopicName, shardId, records); err != nil {
            if _, ok := err.(*datahub.LimitExceededError); ok {
                fmt.Println("maybe qps exceed limit,retry")
                retryNum++
                time.Sleep(5 * time.Second)
                continue
            } else {
                fmt.Println("put record failed")
                fmt.Println(err)
                return
            }
        }
    }
    if retryNum >= maxReTry {
        fmt.Printf("put records failed ")
    }else {
        fmt.Println("put record successful")
    }
}
```




除了数据本身以外，在进行数据发布时，还可以添加和数据相关的额外信息，例如数据采集场景等。添加方式为

```go
record1 := datahub.NewTupleRecord(topic.RecordSchema, 0)
record1.SetAttribute("attribute","test attribute")
record2 := datahub.NewBlobRecord([]byte("blob test2"), 0)
record2.SetAttribute("attribute","test attribute")
```


#### 订阅数据
订阅一个topic下的数据，同样需要指定对应的shard，同时需要指定读取游标位置，通过 getCursor 接口获取。
> GetCursor(projectName, topicName, shardId string, ctype CursorType, param ...int64) (*GetCursorResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.
	- ctype: Which type used to get cursor.可以通过四种方式获取：OLDEST, LATEST, SEQUENCE, SYSTEM_TIME。
		- OLDEST: 表示获取的cursor指向当前有效数据中时间最久远的record
		- LATEST: 表示获取的cursor指向当前最新的record
		- SEQUENCE: 表示获取的cursor指向该序列的record
		- SYSTEM_TIME: 表示获取的cursor指向该时间之后接收到的第一条record
	- param: Parameter used to get cursor.when use SEQUENCE and SYSTEM_TIME need to be set.




- return 

```go
type GetCursorResult struct {
    Cursor     string `json:"Cursor"`
    RecordTime int64  `json:"RecordTime"`
    Sequence   int64  `json:"Sequence"`
}
```

- error
	- ResourceNotFoundError
	- SeekOutOfRangeError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
	- ShardSealedError

- 示例

```go
func cursor(dh datahub.DataHub, projectName, topicName string) {
    shardId := "0"
    gr, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
    if err != nil {
        fmt.Println("get cursor failed")
        fmt.Println(err)
    }else{
        fmt.Println(gr)
    }
    

    gr, err = dh.GetCursor(projectName, topicName, shardId, datahub.LATEST)
    fmt.Println(err)
    fmt.Println(gr)

    var seq int64 = 10
    gr, err = dh.GetCursor(projectName, topicName, shardId, datahub.SEQUENCE, seq)
    if err != nil {
        fmt.Println("get cursor failed")
        fmt.Println(err)
    }else{
        fmt.Println(gr)
    }
}
```

从指定shard读取数据，需要指定从哪个cursor开始读，并指定读取的上限数据条数，如果从cursor到shard结尾少于Limit条数的数据，则返回实际的条数的数据。

#### Tuple topic data
> GetTupleRecords(projectName, topicName, shardId, cursor string, limit int, recordSchema *RecordSchema) (*GetRecordsResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.
	- cursor: The start cursor used to read data.
	- limit:Max record size to read.
	- recordSchema: RecordSchema for the topic.

- return 

```go
type GetRecordsResult struct {
    NextCursor    string        `json:"NextCursor"`
    RecordCount   int           `json:"RecordCount"`
    StartSequence int64         `json:"StartSeq"`
    Records       []IRecord     `json:"Records"`
    RecordSchema  *RecordSchema `json:"-"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getTupleData() {
    shardId := "1"
    topic, err := dh.GetTopic(projectName, topicName)
    if err != nil {
        fmt.Println("get topic failed")
        return
    }
    fmt.Println("get topic successful")

    cursor, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
    if err != nil {
        fmt.Println("get cursor failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get cursor successful")

    limitNum := 100
    maxReTry := 3
    retryNum := 0
    for retryNum < maxReTry {
        gr, err := dh.GetTupleRecords(projectName, topicName, shardId, cursor.Cursor, limitNum, topic.RecordSchema)
        if err != nil {
            if _, ok := err.(*datahub.LimitExceededError); ok {
                fmt.Println("maybe qps exceed limit,retry")
                retryNum++
                time.Sleep(5 * time.Second)
                continue
            } else {
                fmt.Println("get record failed")
                fmt.Println(err)
                return
            }
        }
        fmt.Println("get record successful")
        for _, record := range gr.Records {
            data, ok := record.(*datahub.TupleRecord)
            if !ok {
                fmt.Printf("record type is not TupleRecord, is %v\n", reflect.TypeOf(record))
            } else {
                fmt.Println(data.Values)
            }
        }
        break
    }
    if retryNum >= maxReTry {
        fmt.Printf("get records failed ")
    }
}
```

#### Blob topic data
> GetBlobRecords(projectName, topicName, shardId, cursor string, limit int) (*GetRecordsResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.
	- cursor: The start cursor used to read data.
	- limit:Max record size to read.

- return 
```go

type GetRecordsResult struct {
    NextCursor    string        `json:"NextCursor"`
    RecordCount   int           `json:"RecordCount"`
    StartSequence int64         `json:"StartSeq"`
    Records       []IRecord     `json:"Records"`
    RecordSchema  *RecordSchema `json:"-"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getBlobData() {
    shardId := "1"

    cursor, err := dh.GetCursor(projectName, blobTopicName, shardId, datahub.OLDEST)
    if err != nil {
        fmt.Println("get cursor failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get cursor successful")

    limitNum := 100

    maxReTry := 3
    retryNum := 0
    for retryNum < maxReTry {
        gr, err := dh.GetBlobRecords(projectName, blobTopicName, shardId, cursor.Cursor, limitNum)
        if err != nil {
            if _, ok := err.(*datahub.LimitExceededError); ok {
                fmt.Println("maybe qps exceed limit,retry")
                retryNum++
                time.Sleep(5 * time.Second)
                continue
            } else {
                fmt.Println("get record failed")
                fmt.Println(err)
                return
            }
        }
        fmt.Println("get record successful")
        for _, record := range gr.Records {
            data, ok := record.(*datahub.BlobRecord)
            if !ok {
                fmt.Printf("record type is not TupleRecord, is %v\n", reflect.TypeOf(record))
            } else {
                fmt.Println(data.StoreData)
            }
        }
        break
    }
    if retryNum >= maxReTry {
        fmt.Printf("get records failed ")
    }
}
```

### meter操作
metering info是对shard的资源占用情况的统计信息，一小时更新一次。
> GetMeterInfo(projectName, topicName, shardId string) (*GetMeterInfoResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.



- return

```go
type GetMeterInfoResult struct {
    ActiveTime int64 `json:"ActiveTime"`
    Storage    int64 `json:"Storage"`
}
```

- error- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func meter(dh datahub.DataHub, projectName, topicName string) {
    shardId := "0"
    gmi, err := dh.GetMeterInfo(projectName, topicName, shardId)
    if err != nil {
        fmt.Println("get meter information failed")
        return
    }
    fmt.Println("get meter information successful")
    fmt.Println(gmi)
}
```

### connector操作
DataHub Connector是把DataHub服务中的流式数据同步到其他云产品中的功能，目前支持将Topic中的数据实时/准实时同步到MaxCompute(原ODPS)、OSS（Object Storage Service，阿里云对象存储服务）、ES（Elasticsearch）、ADS（AnalyticDB for MySQL，分析型数据库MySQL版）、MYSQL、FC（Function Compute、函数计算）、OTS（Open Table Store、表格存储）、DataHub中。用户只需要向DataHub中写入一次数据，并在DataHub服务中配置好同步功能，便可以在其他云产品中使用这份数据。

这里所有的示例代码均以MaxCompute为例。MaxCompute Config的配置信息可以参考[同步数据到MaxCompute](https://help.aliyun.com/document_detail/158808.html)。

**datahub2.14.0版本之后将接口参数connectorType修改connectorId（createConnector除外）,不过接口依旧兼容2.14.0之前版本，只需将参数connectorType转为string作为参数即可。**
- 使用示例

```go
gcr, err := dh.GetConnector(projectName, topicName, string(datahub.SinkOdps))
```

#### 创建connector
> CreateConnector(projectName, topicName string, cType ConnectorType, columnFields []string, config interface{}) (*CreateConnectorResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- cType: The type of connector which you want create.
	- columnFields： Which fields you want synchronize.
	- config: Detail config of specified connector type.


- return 

```go
type CreateConnectorResult struct {
    ConnectorId string `json:"ConnectorId"`
}
```

- error
	- ResourceNotFoundError
	- ResourceExistError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func createConnector(dh datahub.DataHub, projectName, topicName string) {
    odpsEndpoint := ""
    odpsProject := "datahub_test"
    odpsTable := "datahub_go_example"
    odpsAccessId := ""
    odpsAccessKey := "="
    odpsTimeRange := 60
    odpsPartitionMode := datahub.SystemTimeMode
    connectorType := datahub.SinkOdps

    odpsPartitionConfig := datahub.NewPartitionConfig()
    odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
    odpsPartitionConfig.AddConfig("hh", "%H")
    odpsPartitionConfig.AddConfig("mm", "%M")

    sinkOdpsConfig := datahub.SinkOdpsConfig{
        Endpoint:        odpsEndpoint,
        Project:         odpsProject,
        Table:           odpsTable,
        AccessId:        odpsAccessId,
        AccessKey:       odpsAccessKey,
        TimeRange:       odpsTimeRange,
        PartitionMode:   odpsPartitionMode,
        PartitionConfig: *odpsPartitionConfig,
    }

    fileds := []string{"field1", "field2"}

    if err := dh.CreateConnector(projectName, topicName, connectorType, fileds, *sinkOdpsConfig); err != nil {
        fmt.Println("create odps connector failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create odps connector successful")
}
```

#### 列出connector
> ListConnector(projectName, topicName string) (*ListConnectorResult, error)

- 参数
	- projectName: project name
	- topicName: topic name


- return

```go
type ListConnectorResult struct {
    ConnectorIds []string `json:"Connectors"`
}
```

- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func listConnector(dh datahub.DataHub, projectName, topicName string) {
    lc, err := dh.ListConnector(projectName, topicName)
    if err != nil {
        fmt.Println("get connector list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get connector list successful")
    fmt.Println(lc)
}
```

#### 查询connector
> GetConnector(projectName, topicName, connectorId string) (*GetConnectorResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector


- return

```go
type GetConnectorResult struct {
    CreateTime     int64             `json:"CreateTime"`
    LastModifyTime int64             `json:"LastModifyTime"`
    ConnectorId    string            `json:"ConnectorId"`
    ClusterAddress string            `json:"ClusterAddress"`
    Type           ConnectorType     `json:"Type"`
    State          ConnectorState    `json:"State"`
    ColumnFields   []string          `json:"ColumnFields"`
    ExtraConfig    map[string]string `json:"ExtraInfo"`
    Creator        string            `json:"Creator"`
    Owner          string            `json:"Owner"`
    Config         interface{}       `json:"Config"`
}
``` 

- error
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
- 示例

```go
func getConnector(dh datahub.DataHub, projectName, topicName, connectorId string) {
    gcr, err := dh.GetConnector(projectName, topicName, connectorId)
    if err != nil {
        fmt.Println("get odps conector failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get odps conector successful")
    fmt.Println(*gcr)
}
```

#### 更新connector配置
> UpdateConnector(projectName, topicName, connectorId string, config interface{}) error

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.
	- config: Detail config of specified connector type.


- return 
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例	

```go
func updateConnector(dh datahub.DataHub, projectName, topicName, connectorId string) {
    gc, err := dh.GetConnector(projectName, topicName, connectorId)
    if err != nil {
        fmt.Println("get odps connector failed")
        fmt.Println(err)
        return
    }
    config, ok := gc.Config.(datahub.SinkOdpsConfig)
    if !ok {
        fmt.Println("convert config to SinkOdpsConfig failed")
        return
    }

    // modify the config
    config.TimeRange = 200

    if err := dh.UpdateConnector(projectName, topicName, connectorId, config); err != nil {
        fmt.Println("update odps config failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update odps config successful")
}
```

#### 删除connector
> DeleteConnector(projectName, topicName, connectorId string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.
- return
- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func deleteConnector(dh datahub.DataHub, projectName, topicName, connectorId string) {
    if err := dh.DeleteConnector(projectName, topicName, connectorId); err != nil {
        fmt.Println("delete odps connector failed")
        fmt.Println(err)
        return
    }
    fmt.Println("delete odps connector successful")
}
```

#### 查询connector shard状态
可以获取某个topic下所有shard的状态信息，也可以获取topic下指定shard的状态信息。
> GetConnectorShardStatus(projectName, topicName, connectorId string) (*GetConnectorShardStatusResult, error)

> GetConnectorShardStatusByShard(projectName, topicName, connectorId, shardId string) (*ConnectorShardStatusEntry, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.
	- connectorId: The id of the connector.

- return

```go
// getConnectorShardStatus
type GetConnectorShardStatusResult struct {
    ShardStatus map[string]ConnectorShardStatusEntry `json:"ShardStatusInfos"`
}
// GetConnectorShardStatusByShard
type ConnectorShardStatusEntry struct {
    StartSequence    int64               `json:"StartSequence"`
    EndSequence      int64               `json:"EndSequence"`
    CurrentSequence  int64               `json:"CurrentSequence"`
    CurrentTimestamp int64               `json:"CurrentTimestamp"`
    UpdateTime       int64               `json:"UpdateTime"`
    State            ConnectorShardState `json:"State"`
    LastErrorMessage string              `json:"LastErrorMessage"`
    DiscardCount     int64               `json:"DiscardCount"`
    DoneTime         int64               `json:"DoneTime"`
    WorkerAddress    string              `json:"WorkerAddress"`
}
```

- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getConnectorShardStatus(dh datahub.DataHub, projectName, topicName, connectorId string) {
    gcs, err := dh.GetConnectorShardStatus(projectName, topicName, connectorId)
    if err != nil {
        fmt.Println("get connector shard status failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get connector shard status successful")
    for shard, status := range gcs.ShardStatus {
        fmt.Println(shard, status.State)
    }

    shardId := "0"
    gc, err := dh.GetConnectorShardStatusByShard(projectName, topicName, connectorId, shardId)
    if err != nil {
        fmt.Println("get connector shard status failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get connector shard status successful")
    fmt.Println(*gc)
}
```

#### 重启connector shard
可以重启topic下的所有shard，也可以重启topic下的指定shard。
> ReloadConnector(projectName, topicName, connectorId string) error

> ReloadConnectorByShard(projectName, topicName, connectorId, shardId string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.
	- shardId: The id of the shard.

	

- return
- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func reloadConnector(dh datahub.DataHub, projectName, topicName, connectorId string) {
    if err := dh.ReloadConnector(projectName, topicName, connectorId); err != nil {
        fmt.Println("reload connector shard failed")
        fmt.Println(err)
        return
    }
    fmt.Println("reload connector shard successful")

    shardId := "2"
    if err := dh.ReloadConnectorByShard(projectName, topicName, connectorId, shardId); err != nil {
        fmt.Println("reload connector shard failed")
        fmt.Println(err)
        return
    }
    fmt.Println("reload connector shard successful")
}
```

#### 添加新field
可以给connector添加指定列，但要求datahub的topic中和odps都存在对应的列。
> AppendConnectorField(projectName, topicName, connectorId, fieldName string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.
	- fieldName: The name of the field.



- return 
- error 
	- ResourceNotFoundError
	- InvalidParameterError

- 示例

```go
func appendConnectorField(dh datahub.DataHub, projectName, topicName, connectorId string) {
    if err := dh.AppendConnectorField(projectName, topicName, connectorId, "field2"); err != nil {
        fmt.Println("append filed failed")
        fmt.Println(err)
        return
    }
    fmt.Println("append filed successful")
}
```

#### 更新connector状态
connector状态分两种，CONNECTOR_PAUSED和CONNECTOR_RUNNING,分别表示停止和运行中。
> UpdateConnectorState(projectName, topicName, connectorId string, state ConnectorState) error

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.
	- state:The state of the connector which you want update.



- return
- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func updateConnectorState(dh datahub.DataHub, projectName, topicName, connectorId string) {
    if err := dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorStopped); err != nil {
        fmt.Println("update connector state failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update connector state successful")

    if err := dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorRunning); err != nil {
        fmt.Println("update connector state failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update connector state successful")
}
```

#### 更新connector点位信息
> UpdateConnectorOffset(projectName, topicName, connectorId, shardId string, offset ConnectorOffset) error

- 参数
	- projectName: project name
	- topicName: topic name
	- shardId: The id of the shard.
	- connectorId: The id of the connector.
	- offset: The connector offset.

- return
- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func updateConnectorOffset(dh datahub.DataHub, projectName, topicName, connectorId string) {
    shardId := "10"
    offset := datahub.ConnectorOffset{
        Timestamp: 1565864139000,
        Sequence:  104,
    }

    dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorStopped)
    defer dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorRunning)
    if err := dh.UpdateConnectorOffset(projectName, topicName, connectorId, shardId, offset); err != nil {
        fmt.Println("update connector offset failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update connector offset successful")
}
```

#### 查询connector完成时间
只有MaxCompute可以查询完成时间。
> GetConnectorDoneTime(projectName, topicName, connectorId string) (*GetConnectorDoneTimeResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- connectorId: The id of the connector.


- return

```go
type GetConnectorDoneTimeResult struct {
    DoneTime int64 `json:"DoneTime"`
}
```

- error 
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
- 示例

```go
func doneTime(dh datahub.DataHub, projectName, topicName, connectorId string) {

    gcd, err := dh.GetConnectorDoneTime(projectName, topicName, connectorId)
    if err != nil {
        fmt.Println("get connector done time failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get connector done time successful")
    fmt.Println(gcd.DoneTime)
}
```

### subscription操作
订阅服务提供了服务端保存用户消费点位的功能，只需要通过简单配置和处理，就可以实现高可用的点位存储服务。
#### 创建subscription
> CreateSubscription(projectName, topicName, comment string) (*CreateSubscriptionResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- comment: subscription comment

- return

```go
type CreateSubscriptionResult struct {
    CommonResponseResult
    SubId string `json:"SubId"`
}
 ```
 
- error 
	- ResourceExistError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func createSubscription() {
    csr, err := dh.CreateSubscription(projectName, topicName, "sub comment")
    if err != nil {
        fmt.Println("create subscription failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create subscription successful")
    fmt.Println(*csr)
}
```

#### 删除subscription
> DeleteSubscription(projectName, topicName, subId string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func delSubscription(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565577384801DCN0O"
    if err := dh.DeleteSubscription(projectName, topicName, subId); err != nil {
        fmt.Println("delete subscription failed")
        return
    }
    fmt.Println("delete subscription successful")
}
```

#### 查询subscription
> GetSubscription(projectName, topicName, subId string) (*GetSubscriptionResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.



- return

```go
type GetSubscriptionResult struct {
    SubscriptionEntry
}
```

- error
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getSubscription(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565577384801DCN0O"
    gs, err := dh.GetSubscription(projectName, topicName, subId)
    if err != nil {
        fmt.Println("get subscription failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get subscription successful")
    fmt.Println(gs)
}
```

#### 列出subscription
通过pageIndex和pageSize获取指定范围的subscription信息，如pageIndex=1, pageSize=10，获取1-10个subscription； pageIndex=2, pageSize=5则获取6-10的subscription。
> ListSubscription(projectName, topicName string, pageIndex, pageSize int) (*ListSubscriptionResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- pageIndex: The page index used to list subscriptions.	- pageSize: The page size used to list subscriptions.



- return

```go
type ListSubscriptionResult struct {
    TotalCount    int64               `json:"TotalCount"`
    Subscriptions []SubscriptionEntry `json:"Subscriptions"`
}
```

- error
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func listSubscription(dh datahub.DataHub, projectName, topicName string) {
    pageIndex := 1
    pageSize := 5
    ls, err := dh.ListSubscription(projectName, topicName, pageIndex, pageSize)
    if err != nil {
        fmt.Println("get subscription list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get subscription list successful")
    for _, sub := range ls.Subscriptions {
        fmt.Println(sub)
    }
}
```

#### 更新subscription
目前仅支持更新subscription comment
> UpdateSubscription(projectName, topicName, subId, comment string) error

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- comment: subcription comment

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func updateSubscription(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565580329258VXSY8"
    if err := dh.UpdateSubscription(projectName, topicName, subId, "new sub comment"); err != nil {
        fmt.Println("update subscription comment failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update subscription comment successful")
}
```

#### 更新subscription状态
subscription 有两种状态，SUB_OFFLINE 和 SUB_ONLINE,分别表示离线和在线。
> UpdateSubscriptionState(projectName, topicName, subId string, state SubscriptionState) error

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- state: The state you want to change.

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func updateSubState(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565580329258VXSY8"
    if err := dh.UpdateSubscriptionState(projectName, topicName, subId, datahub.SUB_OFFLINE); err != nil {
        fmt.Println("update subscription state failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update subscription state successful")
}
```

### offset操作
一个subscription创建后，初始状态是未消费的，要使用subscription服务提供的点位存储功能，需要进行一些offset操作。
#### 初始化offset
初始化subscrition是使用subscription进行点位操作的第一步。一个subscription不支持并行操作，如果需要在多个进程中消费同一份数据，则需要使用不同的subscription。调用OpenSubscriptionSession之后，获取的点位信息中，SessionId会+1，并且之前的session失效，无法进行更新offset操作。
> OpenSubscriptionSession(projectName, topicName, subId string, shardIds []string) (*OpenSubscriptionSessionResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- shardIds: The id list of the shards.



- return

```go
type OpenSubscriptionSessionResult struct {
    Offsets map[string]SubscriptionOffset `json:"Offsets"`
}
// SubscriptionOffset
type SubscriptionOffset struct {
    Timestamp int64  `json:"Timestamp"`
    Sequence  int64  `json:"Sequence"`
    VersionId int64  `json:"Version"`
    SessionId *int64 `json:"SessionId"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
- 示例

```go
func openOffset(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565580329258VXSY8"
    shardIds := []string{"0", "1", "2"}
    oss, err := dh.OpenSubscriptionSession(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("open session failed")
        fmt.Println(err)
        return
    }
    fmt.Println("open session successful")
    fmt.Println(oss)
}
```

#### 获取offset
获取subscription的当前点位信息。与OpenSubscriptionSession不同的是，GetSubscriptionOffse获取的点位信息中SubscriptionOffset的SessionId为nil，是无法进行commit点位操作的，因此GetSubscriptionOffset一般用来查看点位信息。
> GetSubscriptionOffset(projectName, topicName, subId string, shardIds []string) (*GetSubscriptionOffsetResult, error)

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- shardIds: The id list of the shards.

- return

```go
type OpenSubscriptionSessionResult struct {
    Offsets map[string]SubscriptionOffset `json:"Offsets"`
}
// SubscriptionOffset
type SubscriptionOffset struct {
    Timestamp int64  `json:"Timestamp"`
    Sequence  int64  `json:"Sequence"`
    VersionId int64  `json:"Version"`
    SessionId *int64 `json:"SessionId"`
}
```

- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError

- 示例

```go
func getOffset(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565580329258VXSY8"
    shardIds := []string{"0", "1", "2"}
    gss, err := dh.GetSubscriptionOffset(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("get session failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get session successful")
    fmt.Println(gss)
}
```

#### 更新offset
更新点位时会验证versionId和sessionId，必须与当前session一致才会更新成功。更新点位时，需要同时设置Timestamp和Sequence，才会更新为有效点位，如果两者不对应，则会更新点位到Timestamp对应的点位，建议更新点位时，选择record中对应的Timestamp和Sequence进行点位更新。
> CommitSubscriptionOffset(projectName, topicName, subId string, offsets map[string]SubscriptionOffset) error

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- offsets: The offset map of shards.

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
- 示例

```go
func updateOffset() {
    shardIds := []string{"0", "1", "2"}
    oss, err := dh.OpenSubscriptionSession(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("open session failed")
        fmt.Println(err)
    }
    fmt.Println("open session successful")
    fmt.Println(oss)

    offset := oss.Offsets["0"]

    // set offset message
    offset.Sequence = 900
    offset.Timestamp = 1565593166690

    offsetMap := map[string]datahub.SubscriptionOffset{
        "0": offset,
    }
    if err := dh.CommitSubscriptionOffset(projectName, topicName, subId, offsetMap); err != nil {
        if _, ok := err.(*datahub.SubscriptionOfflineError); ok {
            fmt.Println("the subscription has offline")
        } else if _, ok := err.(*datahub.SubscriptionSessionInvalidError); ok {
            fmt.Println("the subscription is open elsewhere")
        } else if _, ok := err.(*datahub.SubscriptionOffsetResetError); ok {
            fmt.Println("the subscription is reset elsewhere")
        } else {
            fmt.Println(err)
        }
        fmt.Println("update offset failed")
        return
    }
    fmt.Println("update offset successful")
}
```

#### 重置offset
重置offset可以将offset重置到某个时间点上，重置之后，并且获取的offset信息中，VersionId会+1，之前的session失效，无法进行更新点位操作。
> ResetSubscriptionOffset(projectName, topicName, subId string, offsets map[string]SubscriptionOffset) error

- 参数
	- projectName: project name
	- topicName: topic name
	- subId: The id of the subscription.
	- offsets: The offset map of shards.

- return
- error
	- ResourceNotFoundError
	- AuthorizationFailedError
	- DatahubClientError
	- InvalidParameterError
- 示例

```go
func resetOffset(dh datahub.DataHub, projectName, topicName string) {
    subId := "1565580329258VXSY8"
    offset := datahub.SubscriptionOffset{
        Timestamp: 1565593166690,
    }
    offsetMap := map[string]datahub.SubscriptionOffset{
        "1": offset,
    }

    if err := dh.ResetSubscriptionOffset(projectName, topicName, subId, offsetMap); err != nil {
        fmt.Println("reset offset failed")
        fmt.Println(err)
        return
    }
    fmt.Println("reset offset successful")
}
```

### batch模式操作
使用NewBatchClient接口创建Datahub对象:
```go
var dh = datahub.NewBatchClient(accessId, accessKey, endpoint)
```
其中的accessId, accessKey, endpoint参数同上面准备工作中datahub.New接口的。

DataHub对象的其它接口中batch模式不支持：PutRecords(projectName, topicName string, records []IRecord)，其它接口batch模式均支持，使用上和非batch模式的相同。

***

### error类型
GO SDK对datahub的错误类型进行了整理，用户可以使用类型断言进行错误类型的判断，然后根据错误的类型进行响应的处理。
其中错误类型中，除DatahubClientError和LimitExceededError之外，其余均属于不可重试错误，而DatahubClientError中包含部分可重试错误，例如server busy,server unavailable等，因此**建议遇到DatahubClientError和LimitExceededError时，可以在代码逻辑中添加重试逻辑，但应严格限制重试次数。**

| 类名                            | 错误码                                                                                                             | 描述                                                                                                   |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------ |
| InvalidParameterError           | InvalidParameter, InvalidCursor                                                                                    | 非法参数                                                                                               |
| ResourceNotFoundError           | ResourceNotFound, NoSuchProject, NoSuchTopic, NoSuchShard, NoSuchSubscription, NoSuchConnector, NoSuchMeteringInfo | 访问的资源不存在（注：进行Split/Merge操作后，立即发送其他请求，有可能会抛出该异常 ）                   |
| ResourceExistError              | ResourceAlreadyExist, ProjectAlreadyExist, TopicAlreadyExist, ConnectorAlreadyExist                                | 资源已存在（创建时如果资源已存在，就会抛出这个异常                                                     |
| SeekOutOfRangeError             | SeekOutOfRange                                                                                                     | getCursor时，给的sequence不在有效范围内（通常数据已过期），或给的timestamp大于当前时间                 |
| AuthorizationFailedError        | Unauthorized                                                                                                       | Authorization 签名解析异常，检查AK是否填写正确                                                         |
| NoPermissionError               | NoPermission, OperationDenied                                                                                      | 没有权限，通常是RAM配置不正确，或没有正确授权子账号                                                    |
| NewShardSealedError             | InvalidShardOperation                                                                                              | shard 处于CLOSED状态可读不可写，继续往CLOSED的shard 写数据，或读到最后一条数据后继续读取，会抛出该异常 |
| LimitExceededError              | LimitExceeded                                                                                                      | 接口使用超限                                                                                           |
| SubscriptionOfflineError        | SubscriptionOffline                                                                                                | 订阅处于下线状态不可用                                                                                 |
| SubscriptionSessionInvalidError | OffsetSessionChanged, OffsetSessionClosed                                                                          | 订阅会话异常，使用订阅时会建立一个session，用于提交点位，如果有其他客户端使用该订阅，会得到该异常      |
| SubscriptionOffsetResetError    | OffsetReseted                                                                                                      | 订阅点位被重置                                                                                         |
| MalformedRecordError            | MalformedRecord                                                                                                    | 非法的 Record 格式，可能的情况有：schema 不正确、包含非utf-8字符、客户端使用pb而服务端不支持、等等     |
| DatahubClientError              | 其他所有，并且是所有异常的基类                                                                                     | 如排除以上异常情况，通常重试即可，但应限制重试次数                                                     |


#### ```DatahubClientError```
datahub的基础错误类型，所有的error都继承了这个错误类型。datahub的错误类型除了已经定义的错误类型，其余错误均属于DatahubClientError，其中包括服务器busy、服务器unavailable等可重试错误，用户可以在自己的代码逻辑中添加一些重试机制。

```go
type DatahubClientError struct {
    StatusCode int    `json:"StatusCode"`   // Http status code
    RequestId  string `json:"RequestId"`    // Request-id to trace the request
    Code       string `json:"ErrorCode"`    // Datahub error code
    Message    string `json:"ErrorMessage"` // Error msg of the error code
}
```

#### error使用示例： 

```go
func example_error() {
    accessId := ""
    accessKey := ""
    endpoint := ""
    projectName := "datahub_go_test"
    maxRetry := 3

    dh := datahub.New(accessId, accessKey, endpoint)

    if err := dh.CreateProject(projectName, "project comment"); err != nil {
        if _, ok := err.(*datahub.InvalidParameterError); ok {
            fmt.Println("invalid parameter,please check your input parameter")
        } else if _, ok := err.(*datahub.ResourceExistError); ok {
            fmt.Println("project already exists")
        } else if _, ok := err.(*datahub.AuthorizationFailedError); ok {
            fmt.Println("accessId or accessKey err,please check your accessId and accessKey")
        } else if _, ok := err.(*datahub.LimitExceededError); ok {
            fmt.Println("limit exceed, so retry")
            for i := 0; i < maxRetry; i++ {
                // wait 5 seconds
                time.Sleep(5 * time.Second)
                if err := dh.CreateProject(projectName, "project comment"); err != nil {
                    fmt.Println("create project failed")
                    fmt.Println(err)
                } else {
                    fmt.Println("create project successful")
                    break
                }
            }
        } else {
            fmt.Println("unknown error")
            fmt.Println(err)
        }
    } else {
        fmt.Println("create project successful")
    }
}
```
