package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
	fmt.Println(topicName)
	dh = datahub.New(accessId, accessKey, endpoint)
	createTupleTopic()
	createBlobTopic()

	listTopic()

	getTopic()

	updateTopic()

	deleteTopic()

}

func createTupleTopic() {
	schema := datahub.NewRecordSchema()
	schema.AddField(*datahub.NewField("string_field", datahub.STRING))
	schema.AddField(*datahub.NewFieldWithProp("bigint_field", datahub.BIGINT, false, "test11"))
	schema.AddField(*datahub.NewField("double_field", datahub.DOUBLE))
	schema.AddField(*datahub.NewField("boolean_field", datahub.BOOLEAN))

	if _, err := dh.CreateTupleTopic(projectName, topicName, "topic comment", 7, 3, schema); err != nil {
		if _, ok := err.(*datahub.ResourceExistError); ok {
			fmt.Println("topic already exists")
		} else {
			fmt.Println("create topic failed")
			fmt.Println(err)
			return
		}
	}
	fmt.Println("create topic successful")
}

func createBlobTopic() {
	if _, err := dh.CreateBlobTopic(projectName, blobTopicName, "topic comment", 7, 3); err != nil {
		if _, ok := err.(*datahub.ResourceExistError); ok {
			fmt.Println("topic already exists")
		} else {
			fmt.Println("create topic failed")
			fmt.Println(err)
			return
		}
	}
	fmt.Println("create topic successful")
}

func listTopic() {
	lt, err := dh.ListTopic(projectName)
	if err != nil {
		fmt.Println("get topic list failed")
		return
	}
	fmt.Println("get topic list successful")
	fmt.Println(*lt)
}

func getTopic() {
	gt, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println("get topic failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get topic successful")
	fmt.Println(*gt)
}

func updateTopic() {
	if _, err := dh.UpdateTopic(projectName, topicName, "new topic comment"); err != nil {
		fmt.Println("update topic comment failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update topic comment successful")
}

func deleteTopic() {
	if _, err := dh.DeleteTopic(projectName, topicName); err != nil {
		if _, ok := err.(*datahub.ResourceNotFoundError); ok {
			fmt.Println("topic not found")
		} else {
			fmt.Println("delete failed")
			return
		}
	}
	fmt.Println("delete successful")

	if _, err := dh.DeleteTopic(projectName, blobTopicName); err != nil {
		if _, ok := err.(*datahub.ResourceNotFoundError); ok {
			fmt.Println("topic not found")
		} else {
			fmt.Println("delete failed")
			return
		}
	}
	fmt.Println("delete successful")
}

func appendField() {
	field := datahub.Field{
		Name:      "newField",
		Type:      datahub.STRING,
		AllowNull: true,
	}
	_, err := dh.AppendField(projectName, topicName, field)
	fmt.Println(err)
}

func getSchema(dh datahub.DataHub, projectName, topicName string) {
	gt, err := dh.GetTopic(projectName, topicName)
	if err != nil {
		fmt.Println("get topic failed")
		fmt.Println(err)
		return
	} else {
		schema := gt.RecordSchema
		fmt.Println(schema)
	}
}

func createSchema1() {
	schema := datahub.NewRecordSchema()
	schema.AddField(*datahub.NewField("field1", datahub.STRING))
	schema.AddField(*datahub.NewFieldWithProp("field2", datahub.BIGINT, false, "comment"))

	fmt.Println(schema)
}
func createSchema2() {
	recordSchema := datahub.NewRecordSchema()
	recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true})
	recordSchema.AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false})
	recordSchema.AddField(datahub.Field{Name: "string_field", Type: datahub.STRING})
	recordSchema.AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE})
	recordSchema.AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN})
	recordSchema.AddField(datahub.Field{Name: "decimal_field", Type: datahub.DECIMAL})
}

func createSchema3() {
	str := "{\"fields\":[{\"name\":\"field1\",\"type\":\"STRING\",\"notnull\":true,\"comment\":\"\"},{\"name\":\"field2\",\"type\":\"BIGINT\",\"notnull\":false,\"comment\":\"comment\"}]}"
	schema, err := datahub.NewRecordSchemaFromJson(str)
	if err != nil {
		fmt.Println("create recordSchema failed")
		fmt.Println(err)
		return
	}
	fmt.Println("create recordSchema successful")
	fmt.Println(schema)
}
