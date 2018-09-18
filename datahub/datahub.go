package datahub

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/account"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/models"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/rest"
	"github.com/aliyun/aliyun-datahub-sdk-go/datahub/types"
)

type DataHub struct {
	Client *rest.RestClient
}

func New(accessid, accesskey, endpoint string) *DataHub {
	return &DataHub{
		Client: rest.NewRestClient(endpoint, rest.DefaultUserAgent(), rest.DefaultHttpClient(), account.NewAliyunAccount(accessid, accesskey)),
	}
}

// ListProjects list all projects
// It returns all project names
func (datahub *DataHub) ListProjects() (projects *models.Projects, err error) {
	path := rest.PROJECTS
	projects = &models.Projects{}
	err = datahub.Client.Get(path, projects)
	return
}

// CreateProject create new project (Added at 2018.9)
func (datahub *DataHub) CreateProject(projectName, comment string) error {
	path := fmt.Sprintf(rest.PROJECT, projectName)
	project := &models.Project{
		Comment: comment,
	}
	err := datahub.Client.Post(path, project)
	return err
}

// UpdateProject update project (Added at 2018.9)
func (datahub *DataHub) UpdateProject(projectName, comment string) error {
	path := fmt.Sprintf(rest.PROJECT, projectName)
	project := &models.Project{
		Comment: comment,
	}
	err := datahub.Client.Put(path, project)
	return err
}

// DeleteProject delete project (Added at 2018.9)
func (datahub *DataHub) DeleteProject(projectName string) error {
	path := fmt.Sprintf(rest.PROJECT, projectName)
	project := &models.Project{}
	err := datahub.Client.Delete(path, project)
	return err
}

// GetProject get a project deatil named the given name
// It returns models.Project
func (datahub *DataHub) GetProject(projectName string) (project *models.Project, err error) {
	path := fmt.Sprintf(rest.PROJECT, projectName)
	project = &models.Project{}
	err = datahub.Client.Get(path, project)
	return
}

// ListTopics list all topic of the project named projectName
// It returns models.Topics
func (datahub *DataHub) ListTopics(projectName string) (topics *models.Topics, err error) {
	path := fmt.Sprintf(rest.TOPICS, projectName)
	topics = &models.Topics{}
	err = datahub.Client.Get(path, topics)
	return
}

// GetTopic get a topic detail named the given name of the project named projectName (Changed at 2018.9)
// It return models.Topic
func (datahub *DataHub) GetTopic(projectName, topicName string) (topic *models.Topic, err error) {
	path := fmt.Sprintf(rest.TOPIC, projectName, topicName)
	topic = &models.Topic{
		ProjectName: projectName,
		TopicName:   topicName,
	}
	err = datahub.Client.Get(path, topic)
	return
}

// CreateTopic create new topic
// It receives a models.Topic object
func (datahub *DataHub) CreateTopic(topic *models.Topic) error {
	path := fmt.Sprintf(rest.TOPIC, topic.ProjectName, topic.TopicName)
	err := datahub.Client.Post(path, topic)
	return err
}

// CreateTupleTopic create new tuple topic (Added at 2018.9)
func (datahub *DataHub) CreateTupleTopic(projectName, topicName, comment string, shardCount, lifecycle int, recordSchema *models.RecordSchema) error {
	path := fmt.Sprintf(rest.TOPIC, projectName, topicName)
	topic := &models.Topic{
		ProjectName:  projectName,
		TopicName:    topicName,
		ShardCount:   shardCount,
		Lifecycle:    lifecycle,
		RecordSchema: recordSchema,
		RecordType:   types.TUPLE,
		Comment:      comment,
	}
	err := datahub.Client.Post(path, topic)
	return err
}

// CreateBlobTopic create new blob topic (Added at 2018.9)
func (datahub *DataHub) CreateBlobTopic(projectName, topicName, comment string, shardCount, lifecycle int) error {
	path := fmt.Sprintf(rest.TOPIC, projectName, topicName)
	topic := &models.Topic{
		ProjectName: projectName,
		TopicName:   topicName,
		ShardCount:  shardCount,
		Lifecycle:   lifecycle,
		RecordType:  types.BLOB,
		Comment:     comment,
	}
	err := datahub.Client.Post(path, topic)
	return err
}

// UpdateTopic update a topic (Changed at 2018.9)
func (datahub *DataHub) UpdateTopic(projectName, topicName string, lifecycle int, comment string) error {
	path := fmt.Sprintf(rest.TOPIC, projectName, topicName)
	topic := &models.Topic{
		ProjectName: projectName,
		TopicName:   topicName,
		Lifecycle:   lifecycle,
		Comment:     comment,
	}
	err := datahub.Client.Put(path, topic)
	return err
}

// DeleteTopic delete a topic (Changed at 2018.9)
func (datahub *DataHub) DeleteTopic(projectName, topicName string) error {
	path := fmt.Sprintf(rest.TOPIC, projectName, topicName)
	topic := &models.Topic{
		ProjectName: projectName,
		TopicName:   topicName,
	}
	err := datahub.Client.Delete(path, topic)
	return err
}

// ListShards list all shards of the given topic
// It returns []models.Shard
func (datahub *DataHub) ListShards(projectName, topicName string) ([]models.Shard, error) {
	path := fmt.Sprintf(rest.SHARDS, projectName, topicName)
	shards := &models.Shards{}
	err := datahub.Client.Get(path, shards)
	if err != nil {
		return nil, err
	}
	return shards.ShardList, nil
}

// WaitAllShardsReady wait all shards ready util timeout
// If timeout < 0, it will block util all shards ready
func (datahub *DataHub) WaitAllShardsReady(projectName, topicName string, timeout int) bool {
	ready := make(chan bool)
	if timeout > 0 {
		go func(timeout int) {
			time.Sleep(time.Duration(timeout) * time.Second)
			ready <- false
		}(timeout)
	}
	go func(datahub *DataHub) {
		for {
			shards, err := datahub.ListShards(projectName, topicName)
			if err != nil {
				time.Sleep(1 * time.Microsecond)
				continue
			}
			ok := true
			for _, shard := range shards {
				switch shard.State {
				case types.ACTIVE, types.CLOSED:
					continue
				default:
					ok = false
					break
				}
			}
			if ok {
				break
			}
		}
		ready <- true
	}(datahub)

	return <-ready
}

// MergeShard merge two adjacent shards
// It returns the new shard after merged
func (datahub *DataHub) MergeShard(projectName, topicName, shardId, adjShardId string) (*models.ShardAbstract, error) {
	path := fmt.Sprintf(rest.SHARDS, projectName, topicName)
	mergedShards := &models.MergeShard{
		Id:              shardId,
		AdjacentShardId: adjShardId,
	}
	err := datahub.Client.Post(path, mergedShards)
	if err != nil {
		return nil, err
	}
	return &mergedShards.NewShard, nil
}

// SplitShard split a shard to two adjacent shards
// It returns two new shards after split
func (datahub *DataHub) SplitShard(projectName, topicName, shardId, splitKey string) ([]models.ShardAbstract, error) {
	path := fmt.Sprintf(rest.SHARDS, projectName, topicName)
	splitedShards := &models.SplitShard{
		Id:       shardId,
		SplitKey: splitKey,
	}
	err := datahub.Client.Post(path, splitedShards)
	if err != nil {
		return nil, err
	}
	return splitedShards.NewShards, nil
}

// GetCursor get cursor of given shard, if cursor type is "SYSTEM_TIME", the sysTime parameter must be set
// It returns models.Cursor
func (datahub *DataHub) GetCursor(projectName, topicName, shardId string, ct types.CursorType, sysTime uint64) (cursor *models.Cursor, err error) {
	path := fmt.Sprintf(rest.SHARD, projectName, topicName, shardId)
	cursor = &models.Cursor{
		Type:       ct,
		SystemTime: sysTime,
	}
	err = datahub.Client.Post(path, cursor)
	return
}

// PutRecords put records
func (datahub *DataHub) PutRecords(projectName, topicName string, records []models.IRecord) (*models.PutResult, error) {
	path := fmt.Sprintf(rest.SHARDS, projectName, topicName)
	recordsToPut := &models.PutRecords{
		Records: make([]models.IRecord, 0, len(records)),
	}
	for _, r := range records {
		if r != nil {
			recordsToPut.Records = append(recordsToPut.Records, r)
		}
	}
	err := datahub.Client.Post(path, recordsToPut)
	return recordsToPut.Result, err
}

// GetRecords get records
func (datahub *DataHub) GetRecords(topic *models.Topic, shardId, cursor string, limitNum int) (*models.GetResult, error) {
	path := fmt.Sprintf(rest.SHARD, topic.ProjectName, topic.TopicName, shardId)
	records := &models.GetRecords{
		Cursor:       cursor,
		Limit:        limitNum,
		RecordSchema: topic.RecordSchema,
	}
	err := datahub.Client.Post(path, records)
	return records.Result, err
}

// ListSubscriptions list all subscriptions of specified topic
// It returns all subscriptions of specified topic
func (datahub *DataHub) ListSubscriptions(projectName, topicName string) (subscriptions *models.Subscriptions, err error) {
	path := fmt.Sprintf(rest.SUBSCRIPTIONS, projectName, topicName)
	subscriptions = &models.Subscriptions{}
	err = datahub.Client.Post(path, subscriptions)
	return
}

// CreateSubscription create new subscription (Added at 2018.9)
// It returns subId
func (datahub *DataHub) CreateSubscription(projectName, topicName, comment string) (SubId string, err error) {
	path := fmt.Sprintf(rest.SUBSCRIPTIONS, projectName, topicName)
	subscription := &models.Subscription{
		Comment: comment,
	}
	err = datahub.Client.Post(path, subscription)
	SubId = subscription.SubId
	return
}

// UpdateSubscription update subscription (Added at 2018.9)
func (datahub *DataHub) UpdateSubscription(projectName, topicName, subId, comment string) error {
	path := fmt.Sprintf(rest.SUBSCRIPTION, projectName, topicName, subId)
	subscription := &models.Subscription{
		SubId:   subId,
		Comment: comment,
	}
	err := datahub.Client.Put(path, subscription)
	return err
}

// UpdateSubscription update subscription state (Added at 2018.9)
func (datahub *DataHub) UpdateSubscriptionState(projectName, topicName, subId string, state types.SubscriptionState) error {
	path := fmt.Sprintf(rest.SUBSCRIPTION, projectName, topicName, subId)
	subscription := &models.Subscription{
		SubId: subId,
		State: state,
	}
	err := datahub.Client.Put(path, subscription)
	return err
}

// DeleteSubscription delete subscription (Added at 2018.9)
func (datahub *DataHub) DeleteSubscription(projectName, topicName, subId string) error {
	path := fmt.Sprintf(rest.SUBSCRIPTION, projectName, topicName, subId)
	subscription := &models.Subscription{
		SubId: subId,
	}
	err := datahub.Client.Delete(path, subscription)
	return err
}

// GetSubscription get a subscription detail (Added at 2018.9)
// It returns models.Subscription
func (datahub *DataHub) GetSubscription(projectName, topicName, subId string) (subscription *models.Subscription, err error) {
	path := fmt.Sprintf(rest.SUBSCRIPTION, projectName, topicName, subId)
	subscription = &models.Subscription{
		SubId: subId,
	}
	err = datahub.Client.Get(path, subscription)
	return
}
