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
	init()
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
			schemaMap:          make(map[uint32]*SchemaItem),
			versionMap:         make(map[int]*SchemaItem),
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

/*
* Creating the cache first and then adding a lock may result in multiple cache creations,
* but init function will trigger network requests, If a large number of topics are started simultaneously,
* locking before init may result in slower startup times.
 */
func (sc *schemaClient) addTopicSchemaCache(project, topic string, client DataHubApi) topicSchemaCache {
	cache := NewTopicSchemaCache(project, topic, client)
	cache.cache.init()

	sc.lock.Lock()
	defer sc.lock.Unlock()

	// ensure not to continue growing
	for k, v := range sc.topicCache {
		if time.Since(v.accessTime.Load().(time.Time)) > time.Duration(5)*time.Minute {
			delete(sc.topicCache, k)
		}
	}

	sc.topicCache[getTopicKey(project, topic)] = cache
	return cache.getSchemaCache()
}

func (sc *schemaClient) findTopicSchemaCache(project, topic string) topicSchemaCache {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	cache, exists := sc.topicCache[getTopicKey(project, topic)]
	if exists {
		return cache.getSchemaCache()
	}
	return nil
}

func (sc *schemaClient) getTopicSchemaCache(project, topic string, client DataHubApi) topicSchemaCache {
	cache := sc.findTopicSchemaCache(project, topic)
	if cache == nil {
		cache = sc.addTopicSchemaCache(project, topic, client)
	}
	return cache
}

// for test
func (sc *schemaClient) clean() {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	for k := range sc.topicCache {
		delete(sc.topicCache, k)
	}
}

type SchemaItem struct {
	versionId  int
	dhSchema   *RecordSchema
	avroSchema avro.Schema
}

type topicSchemaCacheImpl struct {
	client             DataHubApi
	project            string
	topic              string
	topicResult        *GetTopicResult
	maxSchemaVersionId int
	schemaMap          map[uint32]*SchemaItem
	versionMap         map[int]*SchemaItem
	nextFreshTime      atomic.Value
	lock               sync.RWMutex
}

func (tsc *topicSchemaCacheImpl) init() {
	err := tsc.freshSchema(true)
	if err != nil {
		log.Warnf("%s/%s init schema cache failed, error:%v", tsc.project, tsc.topic, err)
	}
}

func (tsc *topicSchemaCacheImpl) freshNomalSchema(topicResult *GetTopicResult) error {
	tsc.lock.RLock()
	needUpdate := false
	oldItem := tsc.versionMap[0]
	if oldItem == nil || oldItem.dhSchema.HashCode() != topicResult.RecordSchema.hashCode() {
		needUpdate = true
	}
	tsc.lock.RUnlock()
	if !needUpdate {
		log.Infof("%s/%s fresh schema success, no schema change", tsc.project, tsc.topic)
		return nil
	}

	newAvroSchema, err := getAvroSchema(topicResult.RecordSchema)
	if err != nil {
		log.Errorf("%s/%s fresh schema failed, error:%v", tsc.project, tsc.topic, err)
		return err
	}

	tsc.lock.Lock()
	defer tsc.lock.Unlock()

	newItem := &SchemaItem{
		versionId:  0,
		dhSchema:   topicResult.RecordSchema,
		avroSchema: newAvroSchema,
	}

	tsc.maxSchemaVersionId = 0
	tsc.versionMap[0] = newItem
	// the old schema is not directly cleared because it might still be in use for writing.
	tsc.schemaMap[newItem.dhSchema.HashCode()] = newItem
	oldSchema := "nil"
	if oldItem != nil {
		oldSchema = oldItem.dhSchema.String()
	}
	log.Infof("%s/%s fresh schema success, old: %s, new: %s",
		tsc.project, tsc.topic, oldSchema, newItem.dhSchema.String())
	return nil
}

// for enable schema topic
func (tsc *topicSchemaCacheImpl) freshMultiSchema() error {
	res, err := tsc.client.ListTopicSchema(tsc.project, tsc.topic)
	if err != nil {
		return err
	}

	newSchemaList := make([]int, 0)
	newSchemaMap := make(map[uint32]*SchemaItem)
	newVersionMap := make(map[int]*SchemaItem)
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
		item := &SchemaItem{
			versionId:  schema.VersionId,
			avroSchema: avroSchema,
			dhSchema:   &schema.RecordSchema,
		}
		newVersionMap[schema.VersionId] = item
		newSchemaMap[schema.RecordSchema.HashCode()] = item
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

	if tsc.topicResult.RecordType == BLOB {
		return nil
	}

	if !tsc.topicResult.EnableSchema {
		return tsc.freshNomalSchema(tsc.topicResult)
	} else {
		return tsc.freshMultiSchema()
	}
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

	// maybe schema has been freshed after append field,
	// but the old schema is still in use for writing, so return 0 directly
	if !tsc.topicResult.EnableSchema {
		return 0
	}

	if schemaItem, ok := tsc.schemaMap[schema.hashCode()]; ok {
		return schemaItem.versionId
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
	if item, ok := tsc.schemaMap[schema.hashCode()]; ok {
		return item.avroSchema
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
