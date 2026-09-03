package main

import (
	"fmt"
	"sync"

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

func genRecordWithPartitionKey(schema *datahub.RecordSchema, index int) datahub.IRecord {
	record := genRecord(schema)
	record.SetPartitionKey(fmt.Sprintf("pk_%d", index))
	return record
}

func handleSuccessRun(producer datahub.AsyncProducer, wg *sync.WaitGroup) {
	defer wg.Done()
	for suc := range producer.Successes() {
		// handle request success
		fmt.Printf("shard:%s, rid:%s, records:%d, latency:%v\n",
			suc.ShardId, suc.RequestId, len(suc.Records), suc.Latency)
	}
}

func handleFailedRun(producer datahub.AsyncProducer, wg *sync.WaitGroup) {
	defer wg.Done()
	// handle request failed
	for err := range producer.Errors() {
		fmt.Printf("shard:%s, records:%d, latency:%v, error:%v\n",
			err.ShardId, len(err.Records), err.Latency, err.Err)
	}
}

func startResultHandlers(producer datahub.AsyncProducer) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(2)
	go handleSuccessRun(producer, &wg)
	go handleFailedRun(producer, &wg)
	return &wg
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

	handlers := startResultHandlers(producer)

	for i := 0; i < 500; i++ {
		producer.Input() <- genRecord(schema)
	}

	// Flush is optional. It moves records currently in aggregation buffers to
	// the send queues without closing the producer. It waits only for the local
	// flush; server results are still delivered through Successes and Errors.
	producer.Flush()

	for i := 500; i < 1000; i++ {
		producer.Input() <- genRecord(schema)
	}

	err = producer.Close()
	handlers.Wait()
	check(err)
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

	handlers := startResultHandlers(producer)

	for i := 0; i < 1000; i++ {
		// Set a partition key to decide which shard receives the record.
		producer.Input() <- genRecordWithPartitionKey(schema, i)
	}

	err = producer.Close()
	handlers.Wait()
	check(err)
}

func main() {
	// normal async write
	asyncWrite()

	// async write with hash
	asyncWritewithHash()
}
