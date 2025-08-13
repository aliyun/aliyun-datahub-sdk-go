package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
	dh = datahub.New(accessId, accessKey, endpoint)

	listShard()

	spiltShard()

	mergeShard()
}

func listShard() {
	ls, err := dh.ListShard(projectName, topicName)
	if err != nil {
		fmt.Println("get shard list failed")
		fmt.Println(err)
	}
	fmt.Println("get shard list successful")
	for _, shard := range ls.Shards {
		fmt.Println(shard)
	}
}

func spiltShard() {
	ss, err := dh.SplitShard(projectName, topicName, spiltShardId)
	if err != nil {
		fmt.Println("split shard failed")
		fmt.Println(err)
	}
	fmt.Println("split shard successful")
	fmt.Println(ss)

	// After splitting, you need to wait for all shard states to be ready
	// before you can perform related operations.
	dh.WaitAllShardsReady(projectName, topicName)
}

func mergeShard() {
	ms, err := dh.MergeShard(projectName, topicName, mergeShardId, mergeAdjacentShardId)
	if err != nil {
		fmt.Println("merge shard failed")
		fmt.Println(err)
	}
	fmt.Println("merge shard successful")
	fmt.Println(ms)

	// After splitting, you need to wait for all shard states to be ready
	// before you can perform related operations.
	dh.WaitAllShardsReady(projectName, topicName)
}
