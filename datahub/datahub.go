package datahub

import (
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
func (datahub *DataHub) ListProjects() (ps *models.Projects, err error) {
	ps = &models.Projects{}
	err = datahub.Client.Get(ps)
	return
}

// GetProject get a project deatil named the given name
// It returns models.Project
func (datahub *DataHub) GetProject(name string) (p *models.Project, err error) {
	p = &models.Project{
		Name: name,
	}
	err = datahub.Client.Get(p)
	return
}

// ListTopics list all topic of the project named project_name
// It returns models.Topics
func (datahub *DataHub) ListTopics(project_name string) (ts *models.Topics, err error) {
	ts = &models.Topics{
		ProjectName: project_name,
	}
	err = datahub.Client.Get(ts)
	return
}

// GetTopic get a topic detail named the given name of the project named project_name
// It return models.Topic
func (datahub *DataHub) GetTopic(name, project_name string) (t *models.Topic, err error) {
	t = &models.Topic{
		Name:        name,
		ProjectName: project_name,
	}
	err = datahub.Client.Get(t)
	return
}

// CreateTopic create new topic
// It receives a models.Topic object
func (datahub *DataHub) CreateTopic(t *models.Topic) error {
	err := datahub.Client.Post(t)
	return err
}

// UpdateTopic update a topic
func (datahub *DataHub) UpdateTopic(name, project_name string, lifecycle int, comment string) error {
	t := &models.Topic{
		Name:        name,
		ProjectName: project_name,
		Lifecycle:   lifecycle,
		Comment:     comment,
	}
	err := datahub.Client.Put(t)
	return err
}

// DeleteTopic delete a topic
func (datahub *DataHub) DeleteTopic(name, project_name string) error {
	t := &models.Topic{
		Name:        name,
		ProjectName: project_name,
	}
	err := datahub.Client.Delete(t)
	return err
}

// ListShards list all shards of the given topic
// It returns []models.Shard
func (datahub *DataHub) ListShards(project_name, topic_name string) ([]models.Shard, error) {
	ss := &models.Shards{
		ProjectName: project_name,
		TopicName:   topic_name,
	}
	err := datahub.Client.Get(ss)
	if err != nil {
		return nil, err
	}
	return ss.ShardList, nil
}

// WaitAllShardsReady wait all shards ready util timeout
// If timeout < 0, it will block util all shards ready
func (datahub *DataHub) WaitAllShardsReady(project_name, topic_name string, timeout int) bool {
	ready := make(chan bool)
	if timeout > 0 {
		go func(timeout int) {
			time.Sleep(time.Duration(timeout) * time.Second)
			ready <- false
		}(timeout)
	}
	go func(datahub *DataHub) {
		for {
			shards, err := datahub.ListShards(project_name, topic_name)
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
func (datahub *DataHub) MergeShard(project_name, topic_name, shard_id, adj_shard_id string) (*models.ShardAbstract, error) {
	ms := &models.MergeShard{
		Id:              shard_id,
		ProjectName:     project_name,
		TopicName:       topic_name,
		AdjacentShardId: adj_shard_id,
	}
	err := datahub.Client.Post(ms)
	if err != nil {
		return nil, err
	}
	return &ms.NewShard, nil
}

// SplitShard split a shard to two adjacent shards
// It returns two new shards after split
func (datahub *DataHub) SplitShard(project_name, topic_name, shard_id, split_key string) ([]models.ShardAbstract, error) {
	ss := &models.SplitShard{
		Id:          shard_id,
		ProjectName: project_name,
		TopicName:   topic_name,
		SplitKey:    split_key,
	}
	err := datahub.Client.Post(ss)
	if err != nil {
		return nil, err
	}
	return ss.NewShards, nil
}

// GetCursor get cursor of given shard, if cursor type is "SYSTEM_TIME", the systime parameter must be set
// It returns models.Cursor
func (datahub *DataHub) GetCursor(project_name, topic_name, shard_id string, ct types.CursorType, systime int) (c *models.Cursor, err error) {
	c = &models.Cursor{
		ProjectName: project_name,
		TopicName:   topic_name,
		ShardId:     shard_id,
		Type:        ct,
		SystemTime:  systime,
	}
	err = datahub.Client.Post(c)
	return
}

// PutRecords put records
func (datahub *DataHub) PutRecords(project_name, topic_name string, records []models.IRecord) (*models.PutResult, error) {
	pr := &models.PutRecords{
		ProjectName: project_name,
		TopicName:   topic_name,
		Records:     make([]models.IRecord, 0, len(records)),
	}
	for _, r := range records {
		if r != nil {
			pr.Records = append(pr.Records, r)
		}
	}
	err := datahub.Client.Post(pr)
	return pr.Result, err
}

// GetRecords get records
func (datahub *DataHub) GetRecords(topic *models.Topic, shard_id, cursor string, limit_num int) (*models.GetResult, error) {
	br := &models.GetRecords{
		ProjectName:  topic.ProjectName,
		TopicName:    topic.Name,
		ShardId:      shard_id,
		Cursor:       cursor,
		Limit:        limit_num,
		RecordSchema: topic.RecordSchema,
	}
	err := datahub.Client.Post(br)
	return br.Result, err
}
