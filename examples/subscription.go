package main

import (
    "fmt"
    "github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
    dh = datahub.New(accessId, accessKey, endpoint)
}

func createSubscription() {
    csr, err := dh.CreateSubscription(projectName, topicName, "sub comment")
    if err != nil {
        fmt.Println("create subscription failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create subscription successful")
    fmt.Println(*csr)
}

func getSubscription() {
    gs, err := dh.GetSubscription(projectName, topicName, subId)
    if err != nil {
        fmt.Println("get subscription failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get subscription successful")
    fmt.Println(gs)
}

func delSubscription() {
    if _, err := dh.DeleteSubscription(projectName, topicName, subId); err != nil {
        if _, ok := err.(*datahub.ResourceNotFoundError); ok {
            fmt.Println("subscription not found")
        } else {
            fmt.Println("delete subscription failed")
            return
        }
    }
    fmt.Println("delete subscription successful")
}

func listSubscription() {
    pageIndex := 1
    pageSize := 5
    ls, err := dh.ListSubscription(projectName, topicName, pageIndex, pageSize)
    if err != nil {
        fmt.Println("get subscription list failed")
        fmt.Println(err)
        return
    }
    fmt.Println("get subscription list successful")
    for _, sub := range ls.Subscriptions {
        fmt.Println(sub)
    }
}

func updateSubscription() {
    if _, err := dh.UpdateSubscription(projectName, topicName, subId, "new sub comment"); err != nil {
        fmt.Println("update subscription comment failed")
        fmt.Println(err)
        return
    }
    fmt.Println("update subscription comment successful")
}

func updateSubState() {
    if _, err := dh.UpdateSubscriptionState(projectName, topicName, subId, datahub.SUB_OFFLINE); err != nil {
        fmt.Println("update subscription state failed")
        fmt.Println(err)
        return
    }
    defer dh.UpdateSubscriptionState(projectName, topicName, subId, datahub.SUB_ONLINE)
    fmt.Println("update subscription state successful")
}
