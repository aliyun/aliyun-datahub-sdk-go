package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

// subcommands
var ListTopicsCommand *flag.FlagSet
var GetTopicCommand *flag.FlagSet
var CreateTopicCommand *flag.FlagSet
var DeleteTopicCommand *flag.FlagSet
var UpdateTopicCommand *flag.FlagSet

// flag arguments
var TopicName string
var ShardCount int
var RecordType string
var RecordSchema string
var Comment string
var Lifecycle int

func init() {
	// list topics cmd
	ListTopicsCommand = flag.NewFlagSet("lt", flag.ExitOnError)
	ListTopicsCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	RegisterSubCommand("lt", ListTopicsCommand, list_topics_parsed_check, list_topics)

	// get topic cmd
	GetTopicCommand = flag.NewFlagSet("gt", flag.ExitOnError)
	GetTopicCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	GetTopicCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	RegisterSubCommand("gt", GetTopicCommand, get_topic_parsed_check, get_topic)

	// create topic cmd
	CreateTopicCommand = flag.NewFlagSet("ct", flag.ExitOnError)
	CreateTopicCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	CreateTopicCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	CreateTopicCommand.IntVar(&ShardCount, "shardcount", 3, "shard count.")
	CreateTopicCommand.StringVar(&RecordType, "type", "blob", "record type.")
	CreateTopicCommand.StringVar(&RecordSchema, "schema", "", "record schema. (If type is tuple, it is required, and only json format)")
	CreateTopicCommand.StringVar(&Comment, "comment", "topic comment", "topic comment.")
	CreateTopicCommand.IntVar(&Lifecycle, "lifecycle", 7, "topic life cycle.")
	RegisterSubCommand("ct", CreateTopicCommand, create_topic_parsed_check, create_topic)

	// delete topic cmd
	DeleteTopicCommand = flag.NewFlagSet("dt", flag.ExitOnError)
	DeleteTopicCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	DeleteTopicCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	RegisterSubCommand("dt", DeleteTopicCommand, delete_topic_parsed_check, delete_topic)

	// update topic cmd
	UpdateTopicCommand = flag.NewFlagSet("ut", flag.ExitOnError)
	UpdateTopicCommand.StringVar(&ProjectName, "project", "", "project name. (Required)")
	UpdateTopicCommand.StringVar(&TopicName, "topic", "", "topic name. (Required)")
	UpdateTopicCommand.IntVar(&Lifecycle, "lifecycle", 7, "topic life cycle.")
	UpdateTopicCommand.StringVar(&Comment, "comment", "", "topic comment.")
	RegisterSubCommand("ut", UpdateTopicCommand, update_topic_parsed_check, update_topic)
}

func list_topics_parsed_check() bool {
	if ProjectName == "" {
		return false
	}
	return true
}

func list_topics(dh datahub.DataHubApi) error {
	topics, err := dh.ListTopic(ProjectName)
	if err != nil {
		return err
	}
	fmt.Println(*topics)
	return nil
}

func get_topic_parsed_check() bool {
	if ProjectName == "" || TopicName == "" {
		return false
	}
	return true
}

func get_topic(dh datahub.DataHubApi) error {
	topic, err := dh.GetTopic(ProjectName, TopicName)
	if err != nil {
		return err
	}
	fmt.Println(*topic)
	return nil
}

func create_topic_parsed_check() bool {
	if ProjectName == "" || TopicName == "" {
		return false
	}
	if strings.ToLower(RecordType) == "tuple" && RecordSchema == "" {
		return false
	}
	return true
}

func create_topic(dh datahub.DataHubApi) error {

	if strings.ToLower(RecordType) == "tuple" {
		recordSchema := datahub.NewRecordSchema()
		var schameMap map[string]string
		buffer := bytes.NewBufferString(RecordSchema)
		err := json.Unmarshal(buffer.Bytes(), &schameMap)
		if err != nil {
			return err
		}
		for key, val := range schameMap {
			field := datahub.Field{
				Name: key,
				Type: datahub.FieldType(strings.ToUpper(val)),
			}
			recordSchema.AddField(field)
		}
		if _, err := dh.CreateTupleTopic(ProjectName, TopicName, Comment, ShardCount, Lifecycle, recordSchema); err != nil {
			return err
		}
	} else {
		if _, err := dh.CreateBlobTopic(ProjectName, TopicName, Comment, ShardCount, Lifecycle); err != nil {
			return err
		}
	}
	if ready := dh.WaitAllShardsReadyWithTime(ProjectName, TopicName, 1); ready {
		fmt.Printf("all shard ready? %v\n", ready)
	}
	fmt.Printf("topic create suc!\n")
	return nil
}

func delete_topic_parsed_check() bool {
	if ProjectName == "" || TopicName == "" {
		return false
	}
	return true
}

func delete_topic(dh datahub.DataHubApi) error {
	if _, err := dh.DeleteTopic(ProjectName, TopicName); err != nil {
		return err
	}
	fmt.Printf("del %s suc\n", TopicName)
	return nil
}

func update_topic_parsed_check() bool {
	if ProjectName == "" || TopicName == "" {
		return false
	}
	return true
}

func update_topic(dh datahub.DataHubApi) error {
	if _, err := dh.UpdateTopic(ProjectName, TopicName, Comment); err != nil {
		return err
	}
	fmt.Printf("update %s suc\n", TopicName)
	return nil
}
