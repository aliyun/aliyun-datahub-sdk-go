package main

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func listProjects(dh datahub.DataHubApi) {
	projects, err := dh.ListProject()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(projects)
}

func getProject(name string, dh datahub.DataHubApi) {
	project, err := dh.GetProject(name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(project)
	fmt.Println("last modify time  ", project.LastModifyTime)
}

func createProject(projectName, comment string, dh datahub.DataHubApi) {
	if _, err := dh.CreateProject(projectName, comment); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func updateProject(projectName, comment string, dh datahub.DataHubApi) {
	dh.UpdateProject(projectName, comment)
}

func deleteProject(projectName string, dh datahub.DataHubApi) {
	if _, err := dh.DeleteProject(projectName); err != nil {
		fmt.Println(err.Error())
	}
	fmt.Println("del " + projectName + " suc")

}

func createTupleTopic(projectName, topicName string, dh datahub.DataHubApi) {
	recordSchema := datahub.NewRecordSchema()
	recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT}).
		AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP}).
		AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
		AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
		AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN})
	_, err := dh.CreateTupleTopic(projectName, topicName, "go sdk test topic", 3, 7, recordSchema)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("create topic [" + topicName + "] suc")
	if ready := dh.WaitAllShardsReadyWithTime(projectName, topicName, 1); ready {
		fmt.Printf("all shard ready? %v\n", ready)
	}
}

func createBlobTopic(projectName, topicName string, dh datahub.DataHubApi) {
	_, err := dh.CreateBlobTopic(projectName, topicName, "go sdk test topic", 3, 7)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("create topic [" + topicName + "] suc")
}

func getTopic(projectName, topicName string, dh datahub.DataHubApi) {
	topic, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(topic)
}

func listTopics(projectName string, dh datahub.DataHubApi) {
	topics, err := dh.ListTopic(projectName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(topics)
}

func updateTopic(projectName, topicName string, lifecycle int, comment string, dh datahub.DataHubApi) {
	_, err := dh.UpdateTopic(projectName, topicName, comment)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("update %s suc\n", topicName)
}

func deleteTopic(projectName, topicName string, dh datahub.DataHubApi) {
	_, err := dh.DeleteTopic(projectName, topicName)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("del %s suc\n", topicName)
}

func listShards(projectName, topicName string, dh datahub.DataHubApi) {
	shards, err := dh.ListShard(projectName, topicName)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, shard := range shards.Shards {
		fmt.Println(shard)
	}
}

func mergeShard(projectName, topicName, shardId, adjShardId string, dh datahub.DataHubApi) {
	newShards, err := dh.MergeShard(projectName, topicName, shardId, adjShardId)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(newShards)
}

func splitShard(projectName, topicName, shardId, splitKey string, dh datahub.DataHubApi) {
	newShards, err := dh.SplitShardBySplitKey(projectName, topicName, shardId, splitKey)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, shard := range newShards.NewShards {
		fmt.Println(shard)
	}
}

func getCursor(projectName, topicName, shardId string, ct datahub.CursorType, systemTime int64, dh datahub.DataHubApi) {
	cursor, err := dh.GetCursor(projectName, topicName, shardId, ct, systemTime)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(cursor)
}

func putTupleRecords(projectName, topicName string, dh datahub.DataHubApi) {
	topic, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println(err)
		return
	}

	records := make([]datahub.IRecord, 3)
	record1 := datahub.NewTupleRecord(topic.RecordSchema, 0)
	record1.ShardId = "0"
	record1.SetValueByIdx(0, 1)
	record1.SetValueByIdx(1, uint(123456))
	record1.SetValueByName("string_field", "TEST")
	record1.SetValueByName("double_field", 1.0)
	record1.SetValueByIdx(4, true)
	records[0] = record1

	record2 := datahub.NewTupleRecord(topic.RecordSchema, 0)
	record2.ShardId = "1"
	record2.SetValueByIdx(0, datahub.Bigint(2))
	record2.SetValueByIdx(1, datahub.Timestamp(123456))
	record2.SetValueByName("string_field", datahub.String("TEST2"))
	record2.SetValueByName("double_field", datahub.Double(1.0))
	record2.SetValueByIdx(4, datahub.Boolean(true))
	records[1] = record2

	record3 := datahub.NewTupleRecord(topic.RecordSchema, 0)
	record3.ShardId = "2"
	record3.SetValueByIdx(0, datahub.Bigint(3))
	record3.SetValueByIdx(1, datahub.Timestamp(133456))
	record3.SetValueByName("string_field", datahub.String("TEST3"))
	record3.SetValueByName("double_field", datahub.Double(1.0))
	record3.SetValueByIdx(4, datahub.Boolean(true))
	records[2] = record3

	result, err := dh.PutRecords(projectName, topicName, records)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func getTupleRecords(projectName, topicName, shardId string, dh datahub.DataHubApi) {
	topic, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println(err)
		return
	}

	cursor, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := dh.GetTupleRecords(projectName, topicName, shardId, cursor.Cursor, 10, topic.RecordSchema)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, record := range result.Records {
		if br, ok := record.(*datahub.TupleRecord); ok {
			// do some tuple record
			fmt.Println(br)
		}
	}
}

func putBlobRecords(projectName, topicName string, dh datahub.DataHubApi) {
	records := make([]datahub.IRecord, 3)
	record1 := datahub.NewBlobRecord([]byte("blob test1"), 0)
	record1.ShardId = "0"
	records[0] = record1

	record2 := datahub.NewBlobRecord([]byte("blob test2"), 0)
	record2.ShardId = "1"
	records[1] = record2

	record3 := datahub.NewBlobRecord([]byte("blob test3"), 0)
	record3.ShardId = "2"
	records[2] = record3

	result, err := dh.PutRecords(projectName, topicName, records)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func getBlobRecords(projectName, topicName, shardId string, dh datahub.DataHubApi) {
	cursor, err := dh.GetCursor(projectName, topicName, shardId, datahub.OLDEST)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := dh.GetBlobRecords(projectName, topicName, shardId, cursor.Cursor, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, record := range result.Records {
		if br, ok := record.(*datahub.BlobRecord); ok {
			// do some blob record
			fmt.Println(br)
		}
	}
}

func createSubscription(projectName, topicName, comment string, dh datahub.DataHubApi) string {
	subId, err := dh.CreateSubscription(projectName, topicName, comment)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("create subscription, id: " + subId.SubId)
	}
	return subId.SubId
}

func updateSubscription(projectName, topicName, subId, comment string, dh datahub.DataHubApi) {
	_, err := dh.UpdateSubscription(projectName, topicName, subId, comment)
	if err != nil {
		fmt.Println("update subscription error: " + err.Error())
	} else {
		fmt.Println("update subscription, id: " + subId)
	}
}

func updateSubscriptionState(projectName, topicName, subId string, state datahub.SubscriptionState, dh datahub.DataHubApi) {
	_, err := dh.UpdateSubscriptionState(projectName, topicName, subId, state)
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("update subscription state, id: " + subId)
	}
}

func getSubscription(projectName, topicName, subId string, dh datahub.DataHubApi) {
	subscription, err := dh.GetSubscription(projectName, topicName, subId)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(subscription)
}

func deleteSubscription(projectName, topicName, subId string, dh datahub.DataHubApi) {
	_, err := dh.DeleteSubscription(projectName, topicName, subId)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("del subscription [" + subId + "] suc")
}

func listSubscriptions(projectName, topicName string, dh datahub.DataHubApi) {
	subscriptions, err := dh.ListSubscription(projectName, topicName, 1, 5)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, subscription := range subscriptions.Subscriptions {
		fmt.Println(subscription)
	}
}

func main() {
	accessId := "your access id"
	accessKey := "your access key"
	endpoint := "the datahub server endpoint"
	projectName := "your project name"

	dh := datahub.New(accessId, accessKey, endpoint)

	// list projects
	listProjects(dh)

	// create project
	createProject(projectName, "comment", dh)

	// get project
	getProject(projectName, dh)

	// update project
	updateProject(projectName, "new comment", dh)

	// get project
	getProject(projectName, dh)

	// create tuple topic
	createTupleTopic(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// create blob topic
	createBlobTopic(projectName, "go_sdk_blob_topic_test_v2", dh)

	// list topics
	listTopics(projectName, dh)

	// get topic
	getTopic(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// list shards
	listShards(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// update topic
	updateTopic(projectName, "go_sdk_tuple_topic_test_v2", 5, "update test", dh)
	getTopic(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// put records
	putTupleRecords(projectName, "go_sdk_tuple_topic_test_v2", dh)
	putBlobRecords(projectName, "go_sdk_blob_topic_test_v2", dh)

	// get records
	fmt.Println("=======================")
	getTupleRecords(projectName, "go_sdk_tuple_topic_test_v2", "0", dh)
	fmt.Println("=======================")
	getBlobRecords(projectName, "go_sdk_blob_topic_test_v2", "0", dh)
	fmt.Println("=======================")

	// split shard
	time.Sleep(time.Second * 5)
	splitShard(projectName, "go_sdk_tuple_topic_test_v2", "1", "88888888888888888888888888888888", dh)
	listShards(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// merge shard
	time.Sleep(time.Second * 5)
	mergeShard(projectName, "go_sdk_tuple_topic_test_v2", "3", "4", dh)
	listShards(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// create subscription
	subId := createSubscription(projectName, "go_sdk_tuple_topic_test_v2", "comment", dh)

	// get subscription
	getSubscription(projectName, "go_sdk_tuple_topic_test_v2", subId, dh)

	// update subscription
	updateSubscription(projectName, "go_sdk_tuple_topic_test_v2", subId, "new comment", dh)

	// list subscriptions
	listSubscriptions(projectName, "go_sdk_tuple_topic_test_v2", dh)

	// update subscription state
	updateSubscriptionState(projectName, "go_sdk_tuple_topic_test_v2", subId, datahub.SUB_OFFLINE, dh)

	// get subscription
	getSubscription(projectName, "go_sdk_tuple_topic_test_v2", subId, dh)

	//delete subscription
	deleteSubscription(projectName, "go_sdk_tuple_topic_test_v2", subId, dh)

	// delete topic
	deleteTopic(projectName, "go_sdk_tuple_topic_test_v2", dh)
	deleteTopic(projectName, "go_sdk_blob_topic_test_v2", dh)

	//delete project
	deleteProject(projectName, dh)
}
