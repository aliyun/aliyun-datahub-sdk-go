package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/models"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

func list_projects(dh *datahub.DataHub) {
	projects, err := dh.ListProjects()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(projects)
}

func get_project(name string, dh *datahub.DataHub) {
	project, err := dh.GetProject(name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(project)
}

func create_tuple_topic(name, project_name string, dh *datahub.DataHub) {
	t := &models.Topic{
		Name:        name,
		ProjectName: project_name,
		ShardCount:  3,
		Lifecycle:   7,
		Comment:     "go sdk test topic",
	}
	t.RecordType = types.TUPLE
	t.RecordSchema = models.NewRecordSchema()
	t.RecordSchema.AddField(models.Field{Name: "bigint_field", Type: types.BIGINT}).
		AddField(models.Field{Name: "timestamp_field", Type: types.TIMESTAMP}).
		AddField(models.Field{Name: "string_field", Type: types.STRING}).
		AddField(models.Field{Name: "double_field", Type: types.DOUBLE}).
		AddField(models.Field{Name: "boolean_field", Type: types.BOOLEAN})
	err := dh.CreateTopic(t)
	if err != nil {
		fmt.Println(err)
		return
	}
	if ready := dh.WaitAllShardsReady(project_name, name, 1); ready {
		fmt.Printf("all shard ready? %v\n", ready)
	}
}

func create_blob_topic(name, project_name string, dh *datahub.DataHub) {
	t := &models.Topic{
		Name:        name,
		ProjectName: project_name,
		ShardCount:  3,
		Lifecycle:   7,
		Comment:     "go sdk test blob topic",
	}
	t.RecordType = types.BLOB
	err := dh.CreateTopic(t)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func get_topic(name, project_name string, dh *datahub.DataHub) {
	topic, err := dh.GetTopic(name, project_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(topic)
}

func list_topics(project_name string, dh *datahub.DataHub) {
	topics, err := dh.ListTopics(project_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(topics)
}

func update_topic(name, project_name string, lifecycle int, comment string, dh *datahub.DataHub) {
	err := dh.UpdateTopic(name, project_name, lifecycle, comment)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("update %s suc\n", name)
}

func delete_topic(name, project_name string, dh *datahub.DataHub) {
	err := dh.DeleteTopic(name, project_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("del %s suc\n", name)
}

func list_shards(project_name, topic_name string, dh *datahub.DataHub) {
	shards, err := dh.ListShards(project_name, topic_name)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, shard := range shards {
		fmt.Println(shard)
	}
}

func merge_shard(project_name, topic_name, shard_id, adj_shard_id string, dh *datahub.DataHub) {
	newshard, err := dh.MergeShard(project_name, topic_name, shard_id, adj_shard_id)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(newshard)
}

func split_shard(project_name, topic_name, shard_id, split_key string, dh *datahub.DataHub) {
	newshards, err := dh.SplitShard(project_name, topic_name, shard_id, split_key)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, shard := range newshards {
		fmt.Println(shard)
	}
}

func get_cursor(project_name, topic_name, shard_id string, ct types.CursorType, systemtime int, dh *datahub.DataHub) {
	cursor, err := dh.GetCursor(project_name, topic_name, shard_id, ct, systemtime)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(cursor)
}

func put_tuple_records(project_name, topic_name string, dh *datahub.DataHub) {
	topic, err := dh.GetTopic(topic_name, project_name)
	if err != nil {
		fmt.Println(err)
		return
	}

	records := make([]models.IRecord, 3)
	record1 := models.NewTupleRecord(topic.RecordSchema)
	record1.ShardId = "0"
	record1.SetValueByIdx(0, 1)
	record1.SetValueByIdx(1, uint(123456))
	record1.SetValueByName("string_field", "TEST")
	record1.SetValueByName("double_field", 1.0)
	record1.SetValueByIdx(4, true)
	records[0] = record1

	record2 := models.NewTupleRecord(topic.RecordSchema)
	record2.ShardId = "1"
	record2.SetValueByIdx(0, types.Bigint(2))
	record2.SetValueByIdx(1, types.Timestamp(123456))
	record2.SetValueByName("string_field", types.String("TEST2"))
	record2.SetValueByName("double_field", types.Double(1.0))
	record2.SetValueByIdx(4, types.Boolean(true))
	records[1] = record2

	record3 := models.NewTupleRecord(topic.RecordSchema)
	record3.ShardId = "2"
	record3.SetValueByIdx(0, types.Bigint(3))
	record3.SetValueByIdx(1, types.Timestamp(133456))
	record3.SetValueByName("string_field", types.String("TEST3"))
	record3.SetValueByName("double_field", types.Double(1.0))
	record3.SetValueByIdx(4, types.Boolean(true))
	records[2] = record3

	result, err := dh.PutRecords(project_name, topic_name, records)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func get_tuple_records(project_name, topic_name, shard_id string, dh *datahub.DataHub) {
	topic, err := dh.GetTopic(topic_name, project_name)
	if err != nil {
		fmt.Println(err)
		return
	}

	cursor, err := dh.GetCursor(project_name, topic_name, shard_id, types.OLDEST, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := dh.GetRecords(topic, shard_id, cursor.Id, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, record := range result.Records {
		if br, ok := record.(*models.TupleRecord); ok {
			// do some tuple record
			fmt.Println(br)
		}
	}
}

func put_blob_records(project_name, topic_name string, dh *datahub.DataHub) {
	records := make([]models.IRecord, 3)
	record1 := models.NewBlobRecord([]byte("blob test1"))
	record1.ShardId = "0"
	records[0] = record1

	record2 := models.NewBlobRecord([]byte("blob test2"))
	record2.ShardId = "1"
	records[1] = record2

	record3 := models.NewBlobRecord([]byte("blob test3"))
	record3.ShardId = "2"
	records[2] = record3

	result, err := dh.PutRecords(project_name, topic_name, records)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(result)
}

func get_blob_records(project_name, topic_name, shard_id string, dh *datahub.DataHub) {
	topic, err := dh.GetTopic(topic_name, project_name)
	if err != nil {
		fmt.Println(err)
		return
	}

	cursor, err := dh.GetCursor(project_name, topic_name, shard_id, types.OLDEST, 0)
	if err != nil {
		fmt.Println(err)
		return
	}

	result, err := dh.GetRecords(topic, shard_id, cursor.Id, 10)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, record := range result.Records {
		if br, ok := record.(*models.BlobRecord); ok {
			// do some blob record
			fmt.Println(br)
		}
	}
}

func main() {
	accessid := "**your access id**"
	accesskey := "**your access key**"
	endpoint := "**the datahub server endpoint**"
	project_name := "**your project name**"

	dh := datahub.New(accessid, accesskey, endpoint)

	// list projects
	list_projects(dh)

	// get project
	get_project(project_name, dh)

	// create tuple topic
	create_tuple_topic("gosdk_tuple_topic_test_v2", project_name, dh)

	// create blob topic
	create_blob_topic("gosdk_blob_topic_test_v2", project_name, dh)

	// list topics
	list_topics(project_name, dh)

	// get topic
	get_topic("gosdk_tuple_topic_test_v2", project_name, dh)

	// list shards
	list_shards(project_name, "gosdk_tuple_topic_test_v2", dh)

	// update topic
	update_topic("gosdk_tuple_topic_test_v2", project_name, 5, "update test", dh)
	get_topic("gosdk_tuple_topic_test_v2", project_name, dh)

	// put records
	put_tuple_records(project_name, "gosdk_tuple_topic_test_v2", dh)
	put_blob_records(project_name, "gosdk_blob_topic_test_v2", dh)

	// get records
	fmt.Println("=======================")
	get_tuple_records(project_name, "gosdk_tuple_topic_test_v2", "0", dh)
	fmt.Println("=======================")
	get_blob_records(project_name, "gosdk_blob_topic_test_v2", "0", dh)
	fmt.Println("=======================")

	// split shard
	split_shard(project_name, "gosdk_tuple_topic_test_v2", "1", "88888888888888888888888888888888", dh)
	list_shards(project_name, "gosdk_tuple_topic_test_v2", dh)

	// merge shard
	merge_shard(project_name, "gosdk_tuple_topic_test_v2", "3", "4", dh)
	list_shards(project_name, "gosdk_tuple_topic_test_v2", dh)

	// delete topic
	delete_topic("gosdk_tuple_topic_test_v2", project_name, dh)
	delete_topic("gosdk_blob_topic_test_v2", project_name, dh)
}
