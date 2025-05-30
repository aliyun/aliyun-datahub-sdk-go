package main

import (
	"fmt"
	"reflect"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func cursor(projectName, topicName, shardId string) {
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

func getBlobData(projectName, topicName, shardId string) {
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

func getTupleData(projectName, topicName, shardId string) {
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

func main() {
	dh = datahub.New("ak", "sk", "https://dh-cn-wulanchabu.aliyuncs.com")
	//getBlobData()

	project := "test_project"
	topic := "test_topic"
	shardId := "0"

	getTupleData(project, topic, shardId)
}
