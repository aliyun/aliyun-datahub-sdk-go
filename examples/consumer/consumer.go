package main

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func normalExample() {
	config := datahub.NewConsumerConfig()
	config.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
	config.Project = "your_project"
	config.Topic = "your_tuple_topic"
	config.SubId = "your_subscription"
	config.Account = datahub.NewAliyunAccount("your_access_id", "your_access_key")

	consumer := datahub.NewConsumer(config)
	if err := consumer.Init(); err != nil {
		fmt.Printf("Init failed: %v\n", err)
		return
	}
	defer consumer.Close()

	for {
		record, err := consumer.Read(3 * time.Second)
		if err != nil {
			fmt.Println(err)
			break
		}

		if record == nil {
			continue
		}

		// handle record
		fmt.Printf("Record: sequence=%d, systemTime=%d\n",
			record.GetSequence(), record.GetSystemTime())
	}
}

func manualAckExample() {

	config := datahub.NewConsumerConfig()
	config.Endpoint = "https://dh-cn-hangzhou.aliyuncs.com"
	config.Project = "your_project"
	config.Topic = "your_tuple_topic"
	config.SubId = "your_subscription"
	config.Account = datahub.NewAliyunAccount("your_access_id", "your_access_key")

	// auto ack set to false
	config.AutoRecordAck = false

	consumer := datahub.NewConsumer(config)
	if err := consumer.Init(); err != nil {
		fmt.Printf("Init failed: %v\n", err)
		return
	}
	defer consumer.Close()

	for {
		record, err := consumer.Read(3 * time.Second)
		if err != nil {
			fmt.Println(err)
			break
		}

		if record == nil {
			continue
		}

		// handle record
		fmt.Printf("Record: sequence=%d, systemTime=%d\n",
			record.GetSequence(), record.GetSystemTime())
		// invoke ack means you have successfully processed this data.
		// ack is mandatory; otherwise, the offset will not be committed.
		record.GetRecordKey().Ack()
	}
}

func main() {
	normalExample()
	manualAckExample()
}
