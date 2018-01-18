package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/models"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

// subcommands
var PutRecordsCommand *flag.FlagSet
var GetRecordsCommand *flag.FlagSet

// flag arguments
var ShardId string
var RecordSource string
var Timeout int

func init() {
	PutRecordsCommand = flag.NewFlagSet("pr", flag.ExitOnError)
	PutRecordsCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	PutRecordsCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	PutRecordsCommand.StringVar(&ShardId, "shardid", "", "shard id. (Required)")
	PutRecordsCommand.StringVar(&RecordSource, "source", "", "record source. (Required. Blob type is file name, Tuple type is json string)")
	RegisterSubCommand("pr", PutRecordsCommand, put_records_parsed_check, put_records)

	GetRecordsCommand = flag.NewFlagSet("gr", flag.ExitOnError)
	GetRecordsCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	GetRecordsCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	GetRecordsCommand.StringVar(&ShardId, "shardid", "", "shard id. (Required)")
	GetRecordsCommand.IntVar(&Timeout, "timeout", 0, "timeout.")
	RegisterSubCommand("gr", GetRecordsCommand, get_records_parsed_check, get_records)
}

func put_records_parsed_check() bool {
	if ProjectName == "" || TopicName == "" || ShardId == "" || RecordSource == "" {
		return false
	}
	return true
}

func put_records(dh *datahub.DataHub) error {
	topic, err := dh.GetTopic(TopicName, ProjectName)
	if err != nil {
		return err
	}

	var records []models.IRecord
	switch topic.RecordType {
	case types.BLOB:
		dat, err := ioutil.ReadFile(RecordSource)
		if err != nil {
			return err
		}

		records = make([]models.IRecord, 1)
		record := models.NewBlobRecord(dat)
		record.ShardId = ShardId
		records[0] = record

	case types.TUPLE:
		recordsData := &struct {
			Records []map[string]interface{} `json:records`
		}{}

		decoder := json.NewDecoder(strings.NewReader(RecordSource))
		decoder.UseNumber()
		err := decoder.Decode(recordsData)
		if err != nil {
			return err
		}

		records = make([]models.IRecord, len(recordsData.Records))
		for idx, record_data := range recordsData.Records {
			record := models.NewTupleRecord(topic.RecordSchema)
			for key, val := range record_data {
				record.ShardId = ShardId
				record.SetValueByName(key, val)
			}
			records[idx] = record
		}
	}

	trynum := 0
	for {
		result, err := dh.PutRecords(ProjectName, TopicName, records)
		if err != nil {
			return err
		}
		if len(result.FailedRecords) == 0 {
			fmt.Println("put records suc! trynum:", trynum)
			return nil
		} else {
			fail_records := make([]models.IRecord, len(result.FailedRecords))
			for idx, failinfo := range result.FailedRecords {
				fail_records[idx] = records[failinfo.Index]
			}
			records = fail_records
		}
		fmt.Printf("put records failed last time, trynum: %d, result: \n%s\n", trynum, result)
		trynum++
		return nil
	}
}

func get_records_parsed_check() bool {
	if ProjectName == "" || TopicName == "" || ShardId == "" {
		return false
	}
	return true
}

func get_records(dh *datahub.DataHub) error {
	topic, err := dh.GetTopic(TopicName, ProjectName)
	if err != nil {
		return err
	}

	cursor, err := dh.GetCursor(ProjectName, TopicName, ShardId, types.OLDEST, 0)
	if err != nil {
		return err
	}

	rch := make(chan models.IRecord, 10)
	quit := make(chan int)

	// productor goroutine
	go func(dh *datahub.DataHub, topic *models.Topic, shardid, cursor string) {
		for {
			result, err := dh.GetRecords(topic, shardid, cursor, 10)
			if err != nil {
				fmt.Println("get records occured error! err=", err)
				continue
			}

			if len(result.Records) == 0 {
				continue
			}

			for _, record := range result.Records {
				rch <- record
			}
			cursor = result.NextCursor
		}
	}(dh, topic, ShardId, cursor.Id)

	// consumer goroutine
	go func(rt types.RecordType) {
		switch topic.RecordType {
		case types.BLOB:
			for record := range rch {
				br := record.(*models.BlobRecord)
				fmt.Println(br)
			}
		case types.TUPLE:
			for record := range rch {
				tr := record.(*models.TupleRecord)
				fmt.Println(tr)
			}
		}
	}(topic.RecordType)

	// timeout goroutine
	go func(timeout int) {
		if timeout > 0 {
			time.Sleep(time.Duration(timeout) * time.Second)
			quit <- 1
		}
	}(Timeout)

	<-quit
	fmt.Println("get records main thread is timeout, quit!")
	return nil
}
