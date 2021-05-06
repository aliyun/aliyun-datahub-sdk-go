package e2e

import "github.com/aliyun/aliyun-datahub-sdk-go/datahub"

var accessId = ""
var accessKey = ""
var endpoint = ""
var projectName = ""
var tupleTopicName = ""
var blobTopicName = ""
var batchTopic = ""

var client = datahub.New(accessId, accessKey, endpoint)
var batchClient = datahub.NewBatchClient(accessId, accessKey, endpoint)
