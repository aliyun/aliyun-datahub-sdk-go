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
    fields := []datahub.Field{
        {"string_field", datahub.STRING, true, "comment1"},
        {"bigint_field", datahub.BIGINT, false, ""},
        {"timestamp_field", datahub.TIMESTAMP, true, ""},
        {"double_field", datahub.DOUBLE, true, ""},
        {"boolean_field", datahub.BOOLEAN, true, ""},
        {"decimal_field", datahub.DECIMAL, true, ""},
    }
    schema := &datahub.RecordSchema{
        fields,
    }

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
    fields := []datahub.Field{
        {"field1", datahub.STRING, true, "comment"},
        {"field2", datahub.BIGINT, false, ""},
    }
    schema := datahub.RecordSchema{
        fields,
    }

    fmt.Println(schema)
}
func createSchema2() {
    recordSchema := datahub.NewRecordSchema()
    recordSchema.AddField(datahub.Field{Name: "bigint_field", Type: datahub.BIGINT, AllowNull: true}).
        AddField(datahub.Field{Name: "timestamp_field", Type: datahub.TIMESTAMP, AllowNull: false}).
        AddField(datahub.Field{Name: "string_field", Type: datahub.STRING}).
        AddField(datahub.Field{Name: "double_field", Type: datahub.DOUBLE}).
        AddField(datahub.Field{Name: "boolean_field", Type: datahub.BOOLEAN}).
        AddField(datahub.Field{Name: "decimal_field", Type: datahub.DECIMAL})
}

func createSchema3() {
    str := ""
    schema, err := datahub.NewRecordSchemaFromJson(str)
    if err != nil {
        fmt.Println("create recordSchema failed")
        fmt.Println(err)
        return
    }
    fmt.Println("create recordSchema successful")
    fmt.Println(schema)
}
