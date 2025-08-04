package datahub

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hamba/avro/v2"

	log "github.com/sirupsen/logrus"
)

const (
	invalidSchemaVersionId = math.MinInt32
	blobSchemaVersionId    = -1
)

var (
	sSchemaOnce   sync.Once
	sSchemaClient schemaClient
)

type topicSchemaCache interface {
	getMaxSchemaVersionId() int
	getSchemaByVersionId(versionId int) *RecordSchema
	getVersionIdBySchema(schema *RecordSchema) int
	getAvroSchema(schema *RecordSchema) avro.Schema
	getAvroSchemaByVersionId(versionId int) avro.Schema
}

type topicSchemaItem struct {
	accessTime atomic.Value
	cache      topicSchemaCache
}

func NewTopicSchemaCache(project string, topic string, client DataHubApi) *topicSchemaItem {
	var now atomic.Value
	now.Store(time.Now())
	return &topicSchemaItem{
		accessTime: now,
		cache: &topicSchemaCacheImpl{
			client:             client,
			project:            project,
			topic:              topic,
			maxSchemaVersionId: -1,
			nextFreshTime:      now,
		},
	}
}

func (tsi *topicSchemaItem) getSchemaCache() topicSchemaCache {
	tsi.accessTime.Store(time.Now())
	return tsi.cache
}

type schemaClient struct {
	lock       sync.RWMutex
	topicCache map[string]*topicSchemaItem
}

func schemaClientInstance() *schemaClient {
	sSchemaOnce.Do(func() {
		sSchemaClient = schemaClient{
			topicCache: map[string]*topicSchemaItem{},
		}
	})

	return &sSchemaClient
}

func getTopicKey(project, topic string) string {
	return fmt.Sprintf("%s/%s", project, topic)
}

func (sc *schemaClient) addTopicSchemaCache(project, topic string, client DataHubApi) topicSchemaCache {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	// only ensure not to continue growing
	for k, v := range sc.topicCache {
		if time.Since(v.accessTime.Load().(time.Time)) > time.Duration(5)*time.Minute {
			delete(sc.topicCache, k)
		}
	}

	cache := NewTopicSchemaCache(project, topic, client)
	sc.topicCache[getTopicKey(project, topic)] = cache
	return cache.getSchemaCache()
}

func (sc *schemaClient) getTopicSchemaCache(project, topic string, client DataHubApi) topicSchemaCache {
	sc.lock.RLock()

	cache, exists := sc.topicCache[getTopicKey(project, topic)]
	if exists {
		defer sc.lock.RUnlock()
		return cache.getSchemaCache()
	}

	sc.lock.RUnlock()
	return sc.addTopicSchemaCache(project, topic, client)
}

type SchemaItem struct {
	dhSchema   *RecordSchema
	avroSchema avro.Schema
}

type topicSchemaCacheImpl struct {
	client             DataHubApi
	project            string
	topic              string
	topicResult        *GetTopicResult
	maxSchemaVersionId int
	schemaMap          map[uint32]int
	versionMap         map[int]SchemaItem
	nextFreshTime      atomic.Value
	lock               sync.RWMutex
}

func (tsc *topicSchemaCacheImpl) freshSchema(force bool) error {
	nextTime := tsc.nextFreshTime.Load().(time.Time)
	if !force && time.Now().Before(nextTime) {
		return nil
	}

	// pervent fresh shard by multi goroutine
	newNextTime := time.Now().Add(time.Duration(5) * time.Minute)
	if !tsc.nextFreshTime.CompareAndSwap(nextTime, newNextTime) {
		return nil
	}

	var err error
	tsc.topicResult, err = tsc.client.GetTopic(tsc.project, tsc.topic)
	if err != nil {
		return err
	}

	res, err := tsc.client.ListTopicSchema(tsc.project, tsc.topic)
	if err != nil {
		return err
	}

	newSchemaList := make([]int, 0)
	newSchemaMap := map[uint32]int{}
	newVersionMap := map[int]SchemaItem{}
	maxVersion := -1
	for _, schema := range res.SchemaInfoList {
		avroSchema, err := getAvroSchema(&schema.RecordSchema)
		if err != nil {
			log.Errorf("%s/%s fresh schema failed, error:%v", tsc.project, tsc.topic, err)
			return err
		}

		if schema.VersionId > maxVersion {
			maxVersion = schema.VersionId
		}

		newSchemaList = append(newSchemaList, schema.VersionId)
		newVersionMap[schema.VersionId] = SchemaItem{
			avroSchema: avroSchema,
			dhSchema:   &schema.RecordSchema,
		}
		newSchemaMap[schema.RecordSchema.hashCode()] = schema.VersionId
	}

	update := false
	tsc.lock.RLock()
	if len(newVersionMap) != len(tsc.versionMap) {
		update = true
	} else {
		for versionId := range tsc.versionMap {
			if _, ok := newVersionMap[versionId]; !ok {
				update = true
				break
			}
		}
	}
	tsc.lock.RUnlock()

	if !update {
		log.Infof("%s/%s fresh schema success, no schema change", tsc.project, tsc.topic)
	} else {
		tsc.lock.Lock()
		defer tsc.lock.Unlock()
		tsc.maxSchemaVersionId = maxVersion
		tsc.schemaMap = newSchemaMap
		tsc.versionMap = newVersionMap
		log.Infof("%s/%s fresh schema success, newSchemaVersions:%v", tsc.project, tsc.topic, newSchemaList)
	}
	return nil
}

func (tsc *topicSchemaCacheImpl) getMaxSchemaVersionId() int {
	tsc.freshSchema(false)
	tsc.lock.RLock()
	defer tsc.lock.RUnlock()

	return tsc.maxSchemaVersionId
}

func (tsc *topicSchemaCacheImpl) getSchemaByVersionId(versionId int) *RecordSchema {
	tsc.freshSchema(false)

	if versionId >= 0 {
		tsc.lock.RLock()
		defer tsc.lock.RUnlock()

		if schemaItem, ok := tsc.versionMap[versionId]; ok {
			return schemaItem.dhSchema
		}
	}

	return nil
}

func (tsc *topicSchemaCacheImpl) getVersionIdBySchema(schema *RecordSchema) int {
	if schema == nil {
		return blobSchemaVersionId
	}

	tsc.freshSchema(false)
	tsc.lock.RLock()
	defer tsc.lock.RUnlock()

	if version, ok := tsc.schemaMap[schema.hashCode()]; ok {
		return version
	}

	return invalidSchemaVersionId
}

func (tsc *topicSchemaCacheImpl) getAvroSchema(schema *RecordSchema) avro.Schema {
	if schema == nil {
		return getAvroBlobSchema()
	}

	tsc.freshSchema(false)

	tsc.lock.RLock()
	defer tsc.lock.RUnlock()
	if version, ok := tsc.schemaMap[schema.hashCode()]; ok {
		return tsc.versionMap[version].avroSchema
	}

	return nil
}

func (tsc *topicSchemaCacheImpl) getAvroSchemaByVersionId(versionId int) avro.Schema {
	if versionId < 0 {
		return getAvroBlobSchema()
	}

	tsc.freshSchema(false)

	tsc.lock.RLock()
	defer tsc.lock.RUnlock()
	if item, ok := tsc.versionMap[versionId]; ok {
		return item.avroSchema
	}

	return nil
}
