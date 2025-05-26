package datahub

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type Producer interface {
	Init() error

	Send(records []IRecord) (string, error)

	SendByShard(records []IRecord, shardId string) error

	// GetSchema return the schema for the specified topic.
	// If enable multi-version schema, it returns the latest version of the schema.
	// Otherwise, it returns the topic schema.
	GetSchema() (*RecordSchema, error)

	GetSchemaByVersionId(versionId int) (*RecordSchema, error)

	GetActiveShards() []string

	Close() error
}

func newSchemaManager(project, topic string, dh DataHubApi, listSchemaInterval time.Duration) (*schemaManager, error) {
	res := &schemaManager{
		project:       project,
		topic:         topic,
		client:        dh,
		interval:      listSchemaInterval,
		nextFreshTime: time.Now(),
		schemaMap:     make(map[int]*RecordSchema),
	}

	err := res.freshSchema()
	if err != nil {
		return nil, err
	}
	return res, nil
}

type schemaManager struct {
	project       string
	topic         string
	client        DataHubApi
	interval      time.Duration
	nextFreshTime time.Time
	mutex         sync.RWMutex
	schemaMap     map[int]*RecordSchema
}

func (sm *schemaManager) GetSchema(versionId int) (*RecordSchema, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	schema, ok := sm.schemaMap[versionId]
	if ok {
		return schema, nil
	} else {
		return nil, fmt.Errorf("schema version %d not found", versionId)
	}
}

func (sm *schemaManager) freshSchema() error {
	if time.Now().Before(sm.nextFreshTime) {
		return nil
	}

	res, err := sm.client.ListTopicSchema(sm.project, sm.topic)
	if err != nil {
		return err
	}

	newMap := make(map[int]*RecordSchema)
	newVersions := make([]int, 0)
	for _, schema := range res.SchemaInfoList {
		newMap[schema.VersionId] = &schema.RecordSchema
		newVersions = append(newVersions, schema.VersionId)
	}

	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	sm.schemaMap = newMap
	sm.nextFreshTime = time.Now().Add(sm.interval)
	log.Infof("%s/%s schema fresh success, versionIds:%v", sm.project, sm.topic, newVersions)
	return nil
}

type producerImpl struct {
	config             *ProducerConfig
	project            string
	topic              string
	shards             []string
	index              int32
	freshShardInterval time.Duration
	nextFreshShardTime time.Time
	mutex              sync.RWMutex
	client             DataHubApi
	schemaMngr         *schemaManager
	stop               chan bool
}

func NewProducer(cfg *ProducerConfig) Producer {
	return &producerImpl{
		config:             cfg,
		project:            cfg.Project,
		topic:              cfg.Topic,
		shards:             make([]string, 0),
		index:              0,
		freshShardInterval: time.Minute,
		// lastFreshShardTime: time.Now(),
		stop: make(chan bool),
	}
}

func (pi *producerImpl) initMeta() error {
	tmpClient := NewClientWithConfig(pi.config.Endpoint, NewDefaultConfig(), pi.config.Account)
	res, err := tmpClient.GetTopic(pi.project, pi.topic)
	if err != nil {
		return err
	}

	if res.extraConfig.listShardInterval != 0 {
		pi.freshShardInterval = res.extraConfig.listShardInterval
	}

	config := &Config{
		UserAgent:      defaultClientAgent(),
		CompressorType: LZ4,
		Protocol:       Protobuf,
		HttpClient:     DefaultHttpClient(),
	}

	if res.extraConfig.compressType != NOCOMPRESS {
		config.CompressorType = res.extraConfig.compressType
	}

	if res.EnableSchema {
		config.Protocol = Batch
		// TODO
		// config.CompressorType = res.extraConfig.compressType
	} else {
		config.Protocol = res.extraConfig.protocol
	}

	pi.client = NewClientWithConfig(pi.config.Endpoint, config, pi.config.Account)
	pi.schemaMngr, err = newSchemaManager(pi.project, pi.topic, tmpClient, res.extraConfig.listSchemaInterval)
	if err != nil {
		return err
	}

	err = pi.freshShard(true)
	if err != nil {
		return err
	}

	log.Infof("Init %s/%s producer success", pi.project, pi.topic)
	return nil
}

func (pi *producerImpl) Init() error {
	return pi.initMeta()
}

func (pi *producerImpl) Send(records []IRecord) (string, error) {
	shardId := pi.getNextShard()
	if shardId == "" {
		return "", fmt.Errorf("cannot get valid shard")
	}

	err := pi.SendByShard(records, shardId)
	if IsShardSealedError(err) {
		pi.freshShard(true)
		shardId = pi.getNextShard()
		if shardId == "" {
			return "", fmt.Errorf("cannot get valid shard")
		}

		err = pi.SendByShard(records, shardId)
	}

	if err != nil {
		return "", err
	}

	return shardId, nil
}

func (pi *producerImpl) SendByShard(records []IRecord, shardId string) error {
	return pi.sendWithRetry(records, shardId)
}

func (pi *producerImpl) sendWithRetry(records []IRecord, shardId string) error {
	var returnErr error = nil
	for i := 0; pi.config.MaxRetry <= 0 || i < pi.config.MaxRetry; i++ {
		now := time.Now()
		res, err := pi.client.PutRecordsByShard(pi.project, pi.topic, shardId, records)
		if err == nil {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("%s/%s/%s send records %d success, cost: %v, rid:%s",
					pi.project, pi.topic, shardId, len(records), time.Since(now), res.RequestId)
			}
			return nil
		}

		if !IsRetryableError(err) {
			log.Errorf("%s/%s/%s send records %d failed, cost:%v, error:%v",
				pi.project, pi.topic, shardId, len(records), time.Since(now), err)
			return err
		}

		returnErr = err
		sleepTime := pi.config.RetryInterval
		if IsNetworkError(err) {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("%s/%s/%s send records %d success, cost: %v, rid:%s",
					pi.project, pi.topic, shardId, len(records), time.Since(now), res.RequestId)
			}
		} else if IsLimitExceedError(err) {
			sleepTime = 100 * time.Millisecond
			log.Warnf("%s/%s/%s send records %d exceed limit, cost:%v, error:%v",
				pi.project, pi.topic, shardId, len(records), time.Since(now), err)
		} else if IsRetryableError(err) {
			log.Warnf("%s/%s/%s send records %d failed, cost:%v, error:%v",
				pi.project, pi.topic, shardId, len(records), time.Since(now), err)
		}
		time.Sleep(sleepTime)
	}
	return returnErr
}

func (pi *producerImpl) getNextIndex() int {
	if pi.config.SendStrategy == RoundRobin {
		return int(atomic.AddInt32(&pi.index, 1))
	} else {
		return rand.Int()
	}
}

func (pi *producerImpl) getNextShard() string {
	pi.freshShard(false)
	index := pi.getNextIndex()
	pi.mutex.RLock()
	defer pi.mutex.RUnlock()

	if len(pi.shards) == 0 {
		return ""
	}

	idx := index % len(pi.shards)
	return pi.shards[idx]
}

func shardsEqual(shards1, shards2 []string) bool {
	if len(shards1) != len(shards2) {
		return false
	}

	if len(shards1) == 0 {
		return true
	}

	for idx := range shards1 {
		if shards1[idx] != shards2[idx] {
			return false
		}
	}

	return true
}

func (pi *producerImpl) freshShard(force bool) error {
	if !force && time.Now().Before(pi.nextFreshShardTime) {
		return nil
	}

	res, err := pi.client.ListShard(pi.project, pi.topic)
	if err != nil {
		return err
	}

	newShards := make([]string, 0)
	for _, shard := range res.Shards {
		if shard.State == ACTIVE {
			newShards = append(newShards, shard.ShardId)
		}
	}

	if len(newShards) == 0 {
		log.Warnf("%s/%s fresh shard list failed, no active shard, rid:%s",
			pi.project, pi.topic, res.RequestId)
		return fmt.Errorf("no active shard")
	}

	sort.Strings(newShards)

	pi.mutex.Lock()
	defer pi.mutex.Unlock()

	if shardsEqual(pi.shards, newShards) {
		log.Infof("%s/%s fresh shard success, no shard update, current:%s",
			pi.project, pi.topic, newShards)
	} else {
		pi.shards = newShards
		log.Infof("%s/%s fresh shard list success, newShards:%v",
			pi.project, pi.topic, newShards)
	}

	pi.nextFreshShardTime = time.Now().Add(pi.freshShardInterval)
	return nil
}

func (pi *producerImpl) GetSchema() (*RecordSchema, error) {
	return pi.GetSchemaByVersionId(0)
}

func (pi *producerImpl) GetSchemaByVersionId(versionId int) (*RecordSchema, error) {
	return pi.schemaMngr.GetSchema(versionId)
}

func (pi *producerImpl) GetActiveShards() []string {
	pi.mutex.RLock()
	defer pi.mutex.RUnlock()

	dst := make([]string, len(pi.shards))
	copy(dst, pi.shards)
	return dst
}

func (pi *producerImpl) Close() error {
	return nil
}
