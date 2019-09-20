package main

import (
    "fmt"
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
    dh = datahub.New(accessId, accessKey, endpoint)
}

func meter(dh datahub.DataHub, projectName, topicName string) {
    shardId := "0"
    gmi, err := dh.GetMeterInfo(projectName, topicName, shardId)
    if err != nil {
        fmt.Println("get meter information failed")
        return
    }
    fmt.Println("get meter information successful")
    fmt.Println(*gmi)
}
