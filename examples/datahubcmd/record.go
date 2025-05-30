package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
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

func put_records(dh datahub.DataHubApi) error {
	topic, err := dh.GetTopic(TopicName, ProjectName)
	if err != nil {
		return err
	}

	var records []datahub.IRecord
	switch topic.RecordType {
	case datahub.BLOB:
		dat, err := ioutil.ReadFile(RecordSource)
		if err != nil {
			return err
		}

		records = make([]datahub.IRecord, 1)
		record := datahub.NewBlobRecord(dat, 0)
		record.ShardId = ShardId
		records[0] = record

	case datahub.TUPLE:
		recordsData := &struct {
			Records []map[string]interface{} `json:records`
		}{}

		decoder := json.NewDecoder(strings.NewReader(RecordSource))
		decoder.UseNumber()
		err := decoder.Decode(recordsData)
		if err != nil {
			return err
		}

		records = make([]datahub.IRecord, len(recordsData.Records))
		for idx, record_data := range recordsData.Records {
			record := datahub.NewTupleRecord(topic.RecordSchema, 0)
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
			fail_records := make([]datahub.IRecord, len(result.FailedRecords))
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

func get_records(dh datahub.DataHubApi) error {
	topic, err := dh.GetTopic(ProjectName, TopicName)
	if err != nil {
		return err
	}

	gc, err := dh.GetCursor(ProjectName, TopicName, ShardId, datahub.OLDEST)
	if err != nil {
		return err
	}

	rch := make(chan datahub.IRecord, 10)
	quit := make(chan int)

	// productor goroutine
	go func(dh datahub.DataHubApi, projectName, topicName, shardId, cursor string, schema *datahub.RecordSchema) {
		for {
			result, err := dh.GetTupleRecords(projectName, topicName, shardId, cursor, 10, schema)
			//result, err := dh.GetRecords(topic, shardId, cursor, 10)
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
	}(dh, ProjectName, TopicName, ShardId, gc.Cursor, topic.RecordSchema)

	// consumer goroutine
	go func(rt datahub.RecordType) {
		switch topic.RecordType {
		case datahub.BLOB:
			for record := range rch {
				br := record.(*datahub.BlobRecord)
				fmt.Println(br)
			}
		case datahub.TUPLE:
			for record := range rch {
				tr := record.(*datahub.TupleRecord)
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
