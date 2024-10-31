package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/shopspring/decimal"
)

func main() {
	dh = datahub.New(accessId, accessKey, endpoint)
	//getBlobData()
	//putBlobData()

	//putTupleData()

	//getTupleData()
}

func cursor() {
	shardId := "0"
	gr, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
	if err != nil {
		fmt.Println("get cursor failed")
		fmt.Println(err)
	} else {
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
	} else {
		fmt.Println(gr)
	}

	// 毫秒时间戳
	var system int64 = 1567481075000
	gr, err = dh.GetCursor(projectName, topicName, shardId, datahub.SYSTEM_TIME, system)
}

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
				fmt.Println(data.RawData)
			}
		}
		break
	}
	if retryNum >= maxReTry {
		fmt.Printf("get records failed ")
	}
}

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
	record1.SetValueByName("bigint_field", 1)
	record1.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
	record1.SetValueByName("string_field", "Test1")
	record1.SetValueByName("double_field", 1.1111)
	record1.SetValueByName("boolean_field", true)
	record1.SetValueByName("decimal_field", decimal.NewFromFloat32(-13.1415926))

	// you can add some attributes when put record
	record1.SetAttribute("attribute", "Test attribute")
	records[0] = record1

	record2 := datahub.NewTupleRecord(topic.RecordSchema, 0)
	record2.ShardId = "1"
	record2.SetValueByName("bigint_field", 2)
	record2.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
	record2.SetValueByName("string_field", "Test2")
	record2.SetValueByName("double_field", 2.2222)
	record2.SetValueByName("boolean_field", true)
	record2.SetValueByName("decimal_field", decimal.NewFromFloat32(-23.1415926))
	records[1] = record2

	record3 := datahub.NewTupleRecord(topic.RecordSchema, 0)
	record3.ShardId = "2"
	record3.SetValueByName("bigint_field", 3)
	record3.SetValueByName("timestamp_field", time.Now().UnixNano()/1000000)
	record3.SetValueByName("string_field", "Test3")
	record3.SetValueByName("double_field", 3.3333)
	record3.SetValueByName("boolean_field", true)
	record3.SetValueByName("decimal_field", decimal.NewFromFloat32(-33.1415926))
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
		if _, err := dh.PutRecordsByShard(projectName, blobTopicName, shardId, records); err != nil {
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
	} else {
		fmt.Println("put record successful")
	}
}
