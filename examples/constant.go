package main

import (
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

const (
    accessId      = ""
    accessKey     = ""
    endpoint      = ""
    projectName   = ""
    topicName     = ""
    blobTopicName = ""
    subId         = ""
    connectorId   = ""

    spiltShardId           = ""
    mergeShardId           = ""
    mergeAdjacentShardId = ""

    odpsEndpoint  = ""
    odpsProject   = ""
    odpsTable     = ""
    odpsAccessId  = ""
    odpsAccessKey = ""
)

var dh datahub.DataHub
