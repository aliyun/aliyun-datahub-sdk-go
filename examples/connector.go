package main

import (
	"fmt"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub"
)

func main() {
	dh = datahub.New(accessId, accessKey, endpoint)
}

func createConnector() {
	odpsTimeRange := 60
	odpsPartitionMode := datahub.SystemTimeMode
	connectorType := datahub.SinkOdps

	odpsPartitionConfig := datahub.NewPartitionConfig()
	odpsPartitionConfig.AddConfig("ds", "%Y%m%d")
	odpsPartitionConfig.AddConfig("hh", "%H")
	odpsPartitionConfig.AddConfig("mm", "%M")

	sinkOdpsConfig := &datahub.SinkOdpsConfig{
		Endpoint:        odpsEndpoint,
		Project:         odpsProject,
		Table:           odpsTable,
		AccessId:        odpsAccessId,
		AccessKey:       odpsAccessKey,
		TimeRange:       odpsTimeRange,
		PartitionMode:   odpsPartitionMode,
		PartitionConfig: *odpsPartitionConfig,
	}

	fileds := []string{"field1", "field2"}

	ccr, err := dh.CreateConnector(projectName, topicName, connectorType, fileds, *sinkOdpsConfig)
	if err != nil {
		fmt.Println("create odps connector failed")
		fmt.Println(err)
		return
	}
	fmt.Println("create odps connector successful")
	fmt.Println(*ccr)

}

func getConnector() {
	gcr, err := dh.GetConnector(projectName, topicName, connectorId)
	if err != nil {
		fmt.Println("get odps conector failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get odps conector successful")
	fmt.Println(*gcr)
}

func updateConnector() {
	gc, err := dh.GetConnector(projectName, topicName, connectorId)
	if err != nil {
		fmt.Println("get odps connector failed")
		fmt.Println(err)
		return
	}
	config, ok := gc.Config.(datahub.SinkOdpsConfig)
	if !ok {
		fmt.Println("convert config to SinkOdpsConfig failed")
		return
	}

	// modify the config
	config.TimeRange = 200

	if _, err := dh.UpdateConnector(projectName, topicName, connectorId, config); err != nil {
		fmt.Println("update odps config failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update odps config successful")
}

func listConnector() {
	lc, err := dh.ListConnector(projectName, topicName)
	if err != nil {
		fmt.Println("get connector list failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get connector list successful")
	fmt.Println(*lc)
}

func deleteConnector() {
	if _, err := dh.DeleteConnector(projectName, topicName, connectorId); err != nil {
		if _, ok := err.(*datahub.ResourceNotFoundError); ok {
			fmt.Println("odps connector not found")
		} else {
			fmt.Println("delete odps connector failed")
			fmt.Println(err)
			return
		}
	}
	fmt.Println("delete odps connector successful")
}

func reloadConnector() {
	if _, err := dh.ReloadConnector(projectName, topicName, connectorId); err != nil {
		fmt.Println("reload connector shard failed")
		fmt.Println(err)
		return
	}
	fmt.Println("reload connector shard successful")

	shardId := "2"
	if _, err := dh.ReloadConnectorByShard(projectName, topicName, connectorId, shardId); err != nil {
		fmt.Println("reload connector shard failed")
		fmt.Println(err)
		return
	}
	fmt.Println("reload connector shard successful")
}

func updateConnectorState() {
	if _, err := dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorStopped); err != nil {
		fmt.Println("update connector state failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update connector state successful")

	if _, err := dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorRunning); err != nil {
		fmt.Println("update connector state failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update connector state successful")
}

func getConnectorShardStatus() {
	gcs, err := dh.GetConnectorShardStatus(projectName, topicName, connectorId)
	if err != nil {
		fmt.Println("get connector shard status failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get connector shard status successful")
	for shard, status := range gcs.ShardStatus {
		fmt.Println(shard, status.State)
	}

	shardId := "0"
	gc, err := dh.GetConnectorShardStatusByShard(projectName, topicName, connectorId, shardId)
	if err != nil {
		fmt.Println("get connector shard status failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get connector shard status successful")
	fmt.Println(*gc)
}

func updateConnectorOffset() {
	shardId := "10"
	offset := datahub.ConnectorOffset{
		Timestamp: 1565864139000,
		Sequence:  104,
	}

	dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorStopped)
	defer dh.UpdateConnectorState(projectName, topicName, connectorId, datahub.ConnectorRunning)
	if err, _ := dh.UpdateConnectorOffset(projectName, topicName, connectorId, shardId, offset); err != nil {
		fmt.Println("update connector offset failed")
		fmt.Println(err)
		return
	}
	fmt.Println("update connector offset successful")
}

func doneTime() {

	gcd, err := dh.GetConnectorDoneTime(projectName, topicName, connectorId)
	if err != nil {
		fmt.Println("get connector done time failed")
		fmt.Println(err)
		return
	}
	fmt.Println("get connector done time successful")
	fmt.Println(gcd.DoneTime)
}

func appendConnectorField(dh datahub.DataHub, projectName, topicName, connectorId string) {
	if _, err := dh.AppendConnectorField(projectName, topicName, connectorId, "field2"); err != nil {
		fmt.Println("append filed failed")
		fmt.Println(err)
		return
	}
	fmt.Println("append filed successful")
}
