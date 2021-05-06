package main

import (
    "fmt"
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
    dh = datahub.New(accessId, accessKey, endpoint)
}

func openOffset() {
    shardIds := []string{"0", "1", "2"}
    oss, err := dh.OpenSubscriptionSession(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("open session failed")
        fmt.Println(err)
    }
    fmt.Println("open session successful")
    fmt.Println(oss)
}

func getOffset() {
    shardIds := []string{"0", "1", "2"}
    gss, err := dh.GetSubscriptionOffset(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("get session failed")
        fmt.Println(err)
    }
    fmt.Println("get session successful")
    fmt.Println(gss)
}

func updateOffset() {
    shardIds := []string{"0", "1", "2"}
    oss, err := dh.OpenSubscriptionSession(projectName, topicName, subId, shardIds)
    if err != nil {
        fmt.Println("open session failed")
        fmt.Println(err)
    }
    fmt.Println("open session successful")
    fmt.Println(oss)

    offset := oss.Offsets["0"]

    // set offset message
    offset.Sequence = 900
    offset.Timestamp = 1565593166690

    offsetMap := map[string]datahub.SubscriptionOffset{
        "0": offset,
    }
    if _, err := dh.CommitSubscriptionOffset(projectName, topicName, subId, offsetMap); err != nil {
        if _, ok := err.(*datahub.SubscriptionOfflineError); ok {
            fmt.Println("the subscription has offline")
        } else if _, ok := err.(*datahub.SubscriptionSessionInvalidError); ok {
            fmt.Println("the subscription is open elsewhere")
        } else if _, ok := err.(*datahub.SubscriptionOffsetResetError); ok {
            fmt.Println("the subscription is reset elsewhere")
        } else {
            fmt.Println(err)
        }
        fmt.Println("update offset failed")
        return
    }
    fmt.Println("update offset successful")
}

func resetOffset() {

    offset := datahub.SubscriptionOffset{
        Timestamp: 1565593166690,
    }
    offsetMap := map[string]datahub.SubscriptionOffset{
        "1": offset,
    }

    if _, err := dh.ResetSubscriptionOffset(projectName, topicName, subId, offsetMap); err != nil {
        fmt.Println("reset offset failed")
        fmt.Println(err)
        return
    }
    fmt.Println("reset offset successful")
}
