package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func genRecord(schema *datahub.RecordSchema) datahub.IRecord {
	if schema != nil { // TUPLE record
		record := datahub.NewTupleRecord(schema)
		err := record.SetValueByName("string_field", "test111")
		check(err)
		err = record.SetValueByName("double_field", 3.145)
		check(err)
		err = record.SetValueByName("integer_field", 123456)
		check(err)
		return record
	} else {
		str := "hello world"
		return datahub.NewBlobRecord([]byte(str))
	}
}

func handleSuccessRun(producer datahub.AsyncProducer) {
	for suc := range producer.Successes() {
		// handle request success
		fmt.Printf("shard:%s, rid:%s, records:%d, latency:%v\n",
			suc.ShardId, suc.RequestId, len(suc.Records), suc.Latency)
	}
}

func handleFailedRun(producer datahub.AsyncProducer) {
	// handle request failed
	for err := range producer.Errors() {
		fmt.Printf("shard:%s, records:%d, latency:%v, error:%v\n",
			err.ShardId, len(err.Records), err.Latency, err.Err)
	}
}

func asyncWrite() {
	cfg := datahub.NewProducerConfig()
	cfg.Account = datahub.NewAliyunAccount("ak", "sk")
	cfg.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
	cfg.Project = "test_project"
	cfg.Topic = "test_topic"

	producer := datahub.NewAsyncProducer(cfg)
	err := producer.Init()

	if err != nil {
		panic(err)
	}

	schema, err := producer.GetSchema()
	if err != nil {
		panic(err)
	}

	go handleSuccessRun(producer)
	go handleFailedRun(producer)

	for i := 0; i < 1000; i++ {
		producer.Input() <- genRecord(schema)
	}

	err = producer.Close()
	if err != nil {
		panic(err)
	}
}

func asyncWritewithHash() {
	cfg := datahub.NewProducerConfig()
	cfg.Account = datahub.NewAliyunAccount("ak", "sk")
	cfg.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
	cfg.Project = "test_project"
	cfg.Topic = "test_topic"

	producer := datahub.NewAsyncProducer(cfg)
	err := producer.Init()

	if err != nil {
		panic(err)
	}

	schema, err := producer.GetSchema()
	if err != nil {
		panic(err)
	}

	go handleSuccessRun(producer)
	go handleFailedRun(producer)

	for i := 0; i < 1000; i++ {
		record := genRecord(schema)
		// set partition key, it will decide which shard to write to
		record.SetPartitionKey(fmt.Sprintf("pk_%d", i))
		producer.Input() <- genRecord(schema)
	}

	err = producer.Close()
	if err != nil {
		panic(err)
	}
}

func main() {
	// normal async write
	asyncWrite()

	// async write with hash
	asyncWritewithHash()
}
