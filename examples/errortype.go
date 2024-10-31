package main

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func example_error() {
	maxRetry := 3
	dh = datahub.New(accessId, accessKey, endpoint)

	if _, err := dh.CreateProject(projectName, "project comment"); err != nil {
		if _, ok := err.(*datahub.InvalidParameterError); ok {
			fmt.Println("invalid parameter,please check your input parameter")
		} else if _, ok := err.(*datahub.ResourceExistError); ok {
			fmt.Println("project already exists")
		} else if _, ok := err.(*datahub.AuthorizationFailedError); ok {
			fmt.Println("accessId or accessKey err,please check your accessId and accessKey")
		} else if _, ok := err.(*datahub.LimitExceededError); ok {
			fmt.Println("limit exceed, so retry")
			for i := 0; i < maxRetry; i++ {
				// wait 5 seconds
				time.Sleep(5 * time.Second)
				if _, err := dh.CreateProject(projectName, "project comment"); err != nil {
					fmt.Println("create project failed")
					fmt.Println(err)
				} else {
					fmt.Println("create project successful")
					break
				}
			}
		} else {
			fmt.Println("unknown error")
			fmt.Println(err)
		}
	} else {
		fmt.Println("create project successful")
	}
}
