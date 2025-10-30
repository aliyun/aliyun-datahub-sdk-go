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

func syncSend() {
	cfg := datahub.NewProducerConfig()
	cfg.Account = datahub.NewAliyunAccount("ak", "sk")
	cfg.Endpoint = "https://dh-cn-wulanchabu.aliyuncs.com"
	cfg.Project = "test_project"
	cfg.Topic = "test_topic"

	producer := datahub.NewProducer(cfg)
	err := producer.Init()

	if err != nil {
		panic(err)
	}

	schema, err := producer.GetSchema()
	if err != nil {
		panic(err)
	}

	// recommended size 512KB ~ 1MB，cannot exceed 4MB
	records := make([]datahub.IRecord, 0)
	for i := 0; i < 100; i++ {
		records = append(records, genRecord(schema))
	}

	deatils, err := producer.Send(records)
	if err != nil {
		panic(err)
	}

	fmt.Printf("send to shard %s success\n", deatils.ShardId)
}

func main() {
	syncSend()
}
