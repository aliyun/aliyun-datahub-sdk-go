package e2e

import (
	"fmt"
	"os"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

var accessId = ""
var accessKey = ""
var endpoint = ""
var projectName = ""
var tupleTopicName = ""
var blobTopicName = ""
var batchTupleTopicName = ""
var batchBlobTopicName = ""

var client = datahub.New(accessId, accessKey, endpoint)
var batchClient = datahub.NewBatchClient(accessId, accessKey, endpoint)

func init() {
	var reinit = false
	if len(accessId) == 0 {
		accessId = os.Getenv("ACCESS_ID")
		if len(accessId) > 0 {
			reinit = true
			fmt.Printf("Inited accessId from ENV ACCESS_ID: %s\n", accessId)
		}
	}

	if len(accessKey) == 0 {
		accessKey = os.Getenv("ACCESS_KEY")
		if len(accessKey) > 0 {
			reinit = true
			fmt.Printf("Inited accessKey from ENV ACCESS_KEY: %s\n", accessKey)
		}
	}

	if len(endpoint) == 0 {
		endpoint = os.Getenv("ENDPOINT")
		if len(endpoint) > 0 {
			reinit = true
			fmt.Printf("Inited endpoint from ENV ENDPOINT: %s\n", endpoint)
		}
	}

	if reinit == true {
		client = datahub.New(accessId, accessKey, endpoint)
		batchClient = datahub.NewBatchClient(accessId, accessKey, endpoint)
	}

	if len(projectName) == 0 {
		projectName = os.Getenv("PROJECT_NAME")
		if len(projectName) > 0 {
			fmt.Printf("Inited projectName from ENV PROJECT_NAME: %s\n", projectName)
		}
	}

	if len(tupleTopicName) == 0 {
		tupleTopicName = os.Getenv("TUPLE_TOPIC_NAME")
		if len(tupleTopicName) > 0 {
			fmt.Printf("Inited tupleTopicName from ENV TUPLE_TOPIC_NAME: %s\n", tupleTopicName)
		}
	}

	if len(blobTopicName) == 0 {
		blobTopicName = os.Getenv("BLOB_TOPIC_NAME")
		if len(blobTopicName) > 0 {
			fmt.Printf("Inited blobTopicName from ENV BLOB_TOPIC_NAME: %s\n", blobTopicName)
		}
	}

	if len(batchTupleTopicName) == 0 {
		batchTupleTopicName = os.Getenv("BATCH_TUPLE_TOPIC_NAME")
		if len(batchTupleTopicName) > 0 {
			fmt.Printf("Inited batchTupleTopicName from ENV BATCH_TUPLE_TOPIC_NAME: %s\n", batchTupleTopicName)
		}
	}

	if len(batchBlobTopicName) == 0 {
		batchBlobTopicName = os.Getenv("BATCH_BLOB_TOPIC_NAME")
		if len(batchBlobTopicName) > 0 {
			fmt.Printf("Inited batchBlobTopicName from ENV BATCH_BLOB_TOPIC_NAME: %s\n", batchBlobTopicName)
		}
	}
}
