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

type producerImpl struct {
	config             *ProducerConfig
	project            string
	topic              string
	shards             []string
	index              int32
	freshShardInterval time.Duration
	nextFreshShardTime atomic.Value
	mutex              sync.RWMutex
	client             DataHubApi
	schemaCache        topicSchemaCache
}

func NewProducer(cfg *ProducerConfig) Producer {
	var now atomic.Value
	now.Store(time.Now())
	return &producerImpl{
		config:             cfg,
		project:            cfg.Project,
		topic:              cfg.Topic,
		shards:             make([]string, 0),
		index:              0,
		freshShardInterval: time.Minute,
		nextFreshShardTime: now,
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
		CompressorType: LZ4,
		Protocol:       Protobuf,
		HttpClient:     DefaultHttpClient(),
	}

	if res.extraConfig.compressType != NOCOMPRESS {
		config.CompressorType = res.extraConfig.compressType
	}

	if res.EnableSchema {
		config.Protocol = Batch
	} else {
		config.Protocol = res.extraConfig.protocol
	}

	userAgent := defaultClientAgent()
	if len(pi.config.UserAgent) > 0 {
		userAgent = userAgent + " " + pi.config.UserAgent
	}

	pi.client = NewClientWithConfig(pi.config.Endpoint, config, pi.config.Account)
	pi.client.setUserAgent(userAgent)
	pi.schemaCache = schemaClientInstance().getTopicSchemaCache(pi.project, pi.topic, pi.client)

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
	for i := 0; pi.config.MaxRetry < 0 || i <= pi.config.MaxRetry; i++ {
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
				log.Debugf("%s/%s/%s send records %d with network error, cost: %v, error:%v",
					pi.project, pi.topic, shardId, len(records), time.Since(now), err)
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
	nextTime := pi.nextFreshShardTime.Load().(time.Time)
	if !force && time.Now().Before(nextTime) {
		return nil
	}

	// pervent fresh shard by multi goroutine
	newNextTime := time.Now().Add(pi.freshShardInterval)
	if !pi.nextFreshShardTime.CompareAndSwap(nextTime, newNextTime) {
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

	return nil
}

func (pi *producerImpl) GetSchema() (*RecordSchema, error) {
	return pi.GetSchemaByVersionId(-1)
}

func (pi *producerImpl) GetSchemaByVersionId(versionId int) (*RecordSchema, error) {
	schema := pi.schemaCache.getSchemaByVersionId(versionId)
	if schema != nil {
		return schema, nil
	}

	return nil, fmt.Errorf("%s/%s schema not found, version:%d", pi.project, pi.topic, versionId)
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
