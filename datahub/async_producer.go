package datahub

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

// ProduceError is the result of single request,
// It means that the request exceed the max retry times
// or meet non-retryable errors, you can check Err fo specific reason,
// you can choose to discard the data or try again.
type ProduceError struct {
	ShardId string
	Records []IRecord
	Latency time.Duration
	Err     error
}

func newProduceError(shardId string, records []IRecord, latency time.Duration, err error) *ProduceError {
	return &ProduceError{
		ShardId: shardId,
		Records: records,
		Latency: latency,
		Err:     err,
	}
}

// ProduceSuccess is the result of single request,
// It means that the records has been sent to the server.
type ProduceSuccess struct {
	ShardId   string
	RequestId string
	Records   []IRecord
	Latency   time.Duration
}

func newProduceSuccess(shardId, rid string, records []IRecord, latency time.Duration) *ProduceSuccess {
	return &ProduceSuccess{
		ShardId:   shardId,
		RequestId: rid,
		Records:   records,
		Latency:   latency,
	}
}

// the input []ShardEntry is order by hashkey or shardId
// return "" means the record will be written to a random shard
type PartitionFunc func(*GetTopicResult, []ShardEntry, IRecord) string

func DefaultPartitionFunc(topic *GetTopicResult, shards []ShardEntry, record IRecord) string {
	if len(record.GetBaseRecord().ShardId) > 0 {
		return record.GetBaseRecord().ShardId
	}

	if topic.ExpandMode == ONLY_EXTEND {
		if len(record.GetBaseRecord().PartitionKey) > 0 {
			val, _ := calculateHashCode(record.GetBaseRecord().PartitionKey)
			return shards[val%uint32(len(shards))].ShardId
		}
	} else {
		if len(record.GetBaseRecord().PartitionKey) == 0 {
			return ""
		}

		key, err := calculateMD5(record.GetBaseRecord().PartitionKey)
		if err != nil {
			panic(err) // There should be no error returned here
		}

		ukey := strings.ToUpper(key)
		idx := sort.Search(len(shards), func(i int) bool {
			return ukey < shards[i].BeginHashKey
		})

		// generally, idx cannot be 0, beacause the first shard beginHashKey is "00...00"
		if idx > 0 {
			idx = idx - 1
		}
		return shards[idx].ShardId
	}

	return ""
}

type AsyncProducer interface {
	Init() error

	// Input is the input channel for the user to write record
	Input() chan<- IRecord

	// Successes is the successful request output channel back to the user when
	// ProducerConfig.EnableSuccessCh is true. If ProducerConfig.EnableSuccessCh is true,
	// you MUST read from this channel or the Producer will deadlock.
	// It is suggested that you send and read messages together in a single select statement.
	Successes() <-chan *ProduceSuccess

	// Errors is the error output channel back to the user. You MUST read from this
	// channel or the Producer will deadlock when the channel is full. Alternatively,
	// you can set ProducerConfig.EnableErrorCh to false, which prevents
	// errors to be returned.
	Errors() <-chan *ProduceError

	// GetSchema return the schema for the specified topic.
	// If enable multi-version schema, it returns the latest version of the schema.
	// Otherwise, it returns the topic schema.
	GetSchema() (*RecordSchema, error)

	GetSchemaByVersionId(versionId int) (*RecordSchema, error)

	GetActiveShards() []string

	// Close current producer, it will write all buffer to server before closed,
	// you also need to handle all errors if write to server failed.
	Close() error
}

type asyncProducerImpl struct {
	config             *ProducerConfig
	project            string
	topic              string
	freshShardInterval time.Duration
	client             DataHubApi
	topicMeta          *GetTopicResult
	schemaCache        topicSchemaCache
	shards             []ShardEntry
	writers            map[string]*shardWriter
	mutex              sync.RWMutex
	buffer             *bufferHelper
	input              chan IRecord
	retries            chan []IRecord
	success            chan *ProduceSuccess
	errors             chan *ProduceError
	updateShardCh      chan bool
	wg                 sync.WaitGroup
}

func NewAsyncProducer(cfg *ProducerConfig) AsyncProducer {
	ap := &asyncProducerImpl{
		config:             cfg,
		project:            cfg.Project,
		topic:              cfg.Topic,
		freshShardInterval: time.Minute * 5,
		buffer:             newBufferHelper(cfg.MaxAsyncBufferNum, cfg.MaxAsyncFlightingNum, cfg.MaxAsyncBufferTime),
		input:              make(chan IRecord, cfg.MaxAsyncBufferNum*2),
		retries:            make(chan []IRecord, 64),
		success:            make(chan *ProduceSuccess, 64),
		errors:             make(chan *ProduceError, 64),
		updateShardCh:      make(chan bool, 8),
	}
	return ap
}

func (ap *asyncProducerImpl) Init() error {
	if err := ap.initMeta(); err != nil {
		return err
	}

	if err := ap.freshShard(); err != nil {
		return err
	}

	go withRecover(fmt.Sprintf("%s/%s-update-shard-task", ap.project, ap.topic), ap.updateShardRun)
	ap.wg.Add(2)
	go ap.dispatch()
	go ap.dispatchBatch()
	return nil
}

func (ap *asyncProducerImpl) initMeta() error {
	tmpClient := NewClientWithConfig(ap.config.Endpoint, NewDefaultConfig(), ap.config.Account)
	var err error
	ap.topicMeta, err = tmpClient.GetTopic(ap.project, ap.topic)
	if err != nil {
		return err
	}

	if ap.topicMeta.extraConfig.listShardInterval != 0 {
		ap.freshShardInterval = ap.topicMeta.extraConfig.listShardInterval
	}

	config := NewDefaultConfig()

	if ap.topicMeta.extraConfig.compressType != NOCOMPRESS {
		config.CompressorType = ap.topicMeta.extraConfig.compressType
	}

	if ap.topicMeta.EnableSchema {
		config.Protocol = Batch
	} else {
		if ap.topicMeta.extraConfig.protocol != unknownProtocol {
			config.Protocol = ap.topicMeta.extraConfig.protocol
		} else {
			config.Protocol = ap.config.Protocol
		}
	}

	userAgent := defaultClientAgent()
	if len(ap.config.UserAgent) > 0 {
		userAgent = userAgent + " " + ap.config.UserAgent
	}

	ap.client = NewClientWithConfig(ap.config.Endpoint, config, ap.config.Account)
	ap.client.setUserAgent(userAgent)
	ap.schemaCache = schemaClientInstance().getTopicSchemaCache(ap.project, ap.topic, ap.client)

	log.Infof("Init %s/%s async producer success", ap.project, ap.topic)
	return nil
}

func (ap *asyncProducerImpl) Input() chan<- IRecord {
	return ap.input
}

func (ap *asyncProducerImpl) Successes() <-chan *ProduceSuccess {
	return ap.success
}

func (ap *asyncProducerImpl) Errors() <-chan *ProduceError {
	return ap.errors
}

func (ap *asyncProducerImpl) GetSchema() (*RecordSchema, error) {
	return ap.GetSchemaByVersionId(-1)
}

func (ap *asyncProducerImpl) GetSchemaByVersionId(versionId int) (*RecordSchema, error) {
	if versionId < 0 {
		versionId = ap.schemaCache.getMaxSchemaVersionId()
	}

	if versionId < 0 { // blob
		return nil, nil
	}

	schema := ap.schemaCache.getSchemaByVersionId(versionId)
	if schema != nil {
		return schema, nil
	}

	return nil, fmt.Errorf("%s/%s schema not found, version:%d", ap.project, ap.topic, versionId)
}

func (ap *asyncProducerImpl) GetActiveShards() []string {
	shards := make([]string, 0)
	res, err := ap.client.ListShard(ap.project, ap.topic)
	if err == nil {
		return shards
	}

	for _, se := range res.Shards {
		if se.State == ACTIVE {
			shards = append(shards, se.ShardId)
		}
	}
	return shards
}

func (ap *asyncProducerImpl) Close() error {
	start := time.Now()
	// 1. stop input
	close(ap.input)

	// 2. wait dispatch finish, it will flush all
	// buffer to server and close all writers
	ap.wg.Wait()

	// 3. flush retry buffer to errors channel
	close(ap.retries)
	for batch := range ap.retries {
		ap.errors <- newProduceError("", batch, time.Duration(0),
			fmt.Errorf("%s/%s writer has been closed", ap.project, ap.topic))
	}

	// 4. close all channel
	close(ap.errors)
	close(ap.success)
	close(ap.updateShardCh)

	log.Infof("%s/%s producer closed, cost: %v", ap.project, ap.topic, time.Since(start))
	return nil
}

func (ap *asyncProducerImpl) freshShard() error {
	lsr, err := ap.client.ListShard(ap.project, ap.topic)
	if err != nil {
		log.Errorf("%s/%s update shard failed, get shard info failed, error: %v", ap.project, ap.topic, err)
		return err
	}

	newShards := make([]ShardEntry, 0, len(lsr.Shards))
	newShardMap := make(map[string]bool)
	for _, se := range lsr.Shards {
		if se.State == ACTIVE {
			newShards = append(newShards, se)
			newShardMap[se.ShardId] = true
		}
	}

	if len(newShards) == 0 {
		log.Errorf("%s/%s update shard failed, no valid shard", ap.project, ap.topic)
		return fmt.Errorf("%s/%s no valid shard", ap.project, ap.topic)
	}

	if ap.topicMeta.ExpandMode != ONLY_EXTEND {
		sort.Slice(newShards, func(i, j int) bool {
			return newShards[i].BeginHashKey < newShards[j].BeginHashKey
		})
	}

	addShards := make([]string, 0)
	ap.mutex.RLock()
	for _, nse := range newShards {
		exists := false
		if ap.shards != nil {
			for _, se := range ap.shards {
				if nse.ShardId == se.ShardId {
					exists = true
					break
				}
			}
		}

		if !exists {
			addShards = append(addShards, nse.ShardId)
		}
	}

	for _, se := range ap.shards {
		if _, ok := newShardMap[se.ShardId]; !ok {
			addShards = append(addShards, se.ShardId)
		}
	}

	ap.mutex.RUnlock()
	if len(addShards) == 0 {
		log.Infof("%s/%s update shard success, no shard change", ap.project, ap.topic)
		return nil
	}

	// Prevent shard state change for schedule, so do not delete writer
	ap.mutex.Lock()
	defer ap.mutex.Unlock()

	newWriters := make([]string, 0)
	if ap.writers == nil {
		ap.writers = make(map[string]*shardWriter)
	}

	for _, shardId := range addShards {
		writer := ap.writers[shardId]
		if writer == nil {
			writer := newShardWriter(ap.config, shardId, ap.client,
				ap.updateShardCh, ap.retries, ap.success, ap.errors)
			writer.start()
			ap.writers[shardId] = writer
			newWriters = append(newWriters, shardId)
		}
	}
	ap.shards = newShards
	log.Infof("%s/%s update shard success, current shard num:%d, new shard writers:%v",
		ap.project, ap.topic, len(newShards), newWriters)
	return nil
}

func (ap *asyncProducerImpl) updateShardRun() {
	log.Infof("%s/%s update shard task started", ap.project, ap.topic)
	rm := rand.IntN(int(ap.freshShardInterval.Milliseconds()))
	nextFreshTime := time.Now().Add(ap.freshShardInterval).Add(time.Duration(rm) * time.Millisecond)
	timer := time.NewTicker(ap.freshShardInterval)
	var running atomic.Bool
	running.Store(false)

	for {
		select {
		case <-timer.C:
			if running.CompareAndSwap(false, true) {
				if time.Now().After(nextFreshTime) {
					if err := ap.freshShard(); err == nil {
						nextFreshTime = time.Now().Add(ap.freshShardInterval)
					} else {
						nextFreshTime = time.Now().Add(time.Second * 30)
					}
				}
				running.Store(false)
			}
		case _, ok := <-ap.updateShardCh:
			if !ok {
				log.Infof("%s/%s update shard task stopped", ap.project, ap.topic)
				return
			}

			if running.CompareAndSwap(false, true) {
				if err := ap.freshShard(); err == nil {
					nextFreshTime = time.Now().Add(ap.freshShardInterval)
				} else {
					nextFreshTime = time.Now().Add(time.Second * 30)
				}
				running.Store(false)
			}
		}
	}
}

func (ap *asyncProducerImpl) dispatchBatch() {
	defer ap.wg.Done()

	index := rand.Int()
	for batch := range ap.buffer.output() {
		ap.mutex.RLock()
		index = (index + 1) % len(ap.shards)
		writer := ap.writers[ap.shards[index].ShardId]
		writer.writeBatch(batch)
		ap.mutex.RUnlock()
	}

	// ensure all buffer send to server
	for _, writer := range ap.writers {
		writer.close()
	}

	log.Warnf("%s/%s dispatch batch exit", ap.project, ap.topic)
}

func (ap *asyncProducerImpl) dispatch() {
	defer ap.wg.Done()
	defer log.Warnf("%s/%s dispatch exit", ap.project, ap.topic)
	defer ap.buffer.close() // ensure all buffer flush to writer

	for {
		select {
		case record, ok := <-ap.input:
			if !ok {
				return
			}

			if record == nil {
				log.Warnf("%s/%s record is nil, ingore it", ap.project, ap.topic)
				continue
			}

			ap.writeRecord(record)
		case batch := <-ap.retries:
			for _, record := range batch {
				ap.writeRecord(record)
			}
		}
	}
}

func (ap *asyncProducerImpl) writeRecord(record IRecord) {
	ap.mutex.RLock()
	defer ap.mutex.RUnlock()

	shardId := ""
	if ap.config.Parittioner != nil {
		shardId = ap.config.Parittioner(ap.topicMeta, ap.shards, record)
	}

	if len(shardId) == 0 {
		ap.buffer.input() <- record
	} else {
		writer := ap.writers[shardId]
		writer.writeRecord(record)
	}
}

type shardWriter struct {
	config        *ProducerConfig
	project       string
	topic         string
	shardId       string
	metaKey       string
	client        DataHubApi
	updateShardCh chan bool
	parentRetrys  chan []IRecord
	parentSuccess chan *ProduceSuccess
	parentErrors  chan *ProduceError
	buffer        *bufferHelper
	wg            sync.WaitGroup
}

func newShardWriter(config *ProducerConfig, shardId string,
	client DataHubApi, shardCh chan bool, retrys chan []IRecord,
	success chan *ProduceSuccess, errors chan *ProduceError) *shardWriter {
	ss := &shardWriter{
		config:        config,
		project:       config.Project,
		topic:         config.Topic,
		shardId:       shardId,
		metaKey:       fmt.Sprintf("%s/%s/%s", config.Project, config.Topic, shardId),
		client:        client,
		updateShardCh: shardCh,
		parentSuccess: success,
		parentRetrys:  retrys,
		parentErrors:  errors,
		buffer:        newBufferHelper(config.MaxAsyncBufferNum, config.MaxAsyncFlightingNum, config.MaxAsyncBufferTime),
	}
	return ss
}

func (ss *shardWriter) start() {
	ss.wg.Add(1)
	go withRecover(fmt.Sprintf("%s-send-task", ss.metaKey), ss.sendRun)
	log.Infof("%s writer start", ss.metaKey)
}

func (ss *shardWriter) close() {
	ss.buffer.close() // ensure all buffer flush to send channel
	ss.wg.Wait()
	log.Infof("%s writer stop", ss.metaKey)
}

func (ss *shardWriter) writeRecord(record IRecord) {
	ss.buffer.input() <- record
}

func (ss *shardWriter) writeBatch(batch []IRecord) {
	ss.buffer.batchInput() <- batch
}

func (ss *shardWriter) sendRun() {
	defer ss.wg.Done()

	for batch := range ss.buffer.output() {
		res, latency, err := ss.sendWithRetry(batch)
		if err == nil && ss.config.EnableSuccessCh {
			ss.parentSuccess <- newProduceSuccess(ss.shardId, res.RequestId, batch, latency)
		} else {
			if IsShardSealedError(err) {
				ss.updateShardCh <- true
				ss.parentRetrys <- batch // maybe out of order
			} else {
				if ss.config.EnableErrorCh {
					ss.parentErrors <- newProduceError(ss.shardId, batch, latency, err)
				}
			}
		}
	}
}

func (ss *shardWriter) sendWithRetry(records []IRecord) (*PutRecordsByShardResult, time.Duration, error) {
	var returnErr error = nil
	var latency time.Duration = 0
	for i := 0; ss.config.MaxRetry < 0 || i <= ss.config.MaxRetry; i++ {
		start := time.Now()
		res, err := ss.client.PutRecordsByShard(ss.project, ss.topic, ss.shardId, records)
		latency = time.Since(start)
		if err == nil {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("%s send records %d success, cost: %v, rid:%s",
					ss.metaKey, len(records), latency, res.RequestId)
			}
			return res, latency, nil
		}

		if !IsRetryableError(err) {
			log.Errorf("%s send records %d failed, cost:%v, error:%v",
				ss.metaKey, len(records), latency, err)
			return nil, latency, err
		}

		returnErr = err
		sleepTime := ss.config.RetryInterval
		if IsNetworkError(err) {
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("%s send records %d with network error, cost: %v, error:%v",
					ss.metaKey, len(records), latency, err)
			}
		} else if IsLimitExceedError(err) {
			sleepTime = 100 * time.Millisecond
			log.Warnf("%s send records %d exceed limit, cost:%v, error:%v",
				ss.metaKey, len(records), latency, err)
		} else if IsRetryableError(err) {
			log.Warnf("%s send records %d failed, cost:%v, error:%v",
				ss.metaKey, len(records), latency, err)
		}
		time.Sleep(sleepTime)
	}
	return nil, latency, returnErr
}

type bufferHelper struct {
	bufferNum  int
	bufferTime time.Duration
	wg         sync.WaitGroup
	batchCh    chan []IRecord
	recordCh   chan IRecord
}

func newBufferHelper(bufferNum, flightingNum int, bufferTime time.Duration) *bufferHelper {
	bh := &bufferHelper{
		bufferNum:  bufferNum,
		bufferTime: bufferTime,
		recordCh:   make(chan IRecord, bufferNum),
		batchCh:    make(chan []IRecord, flightingNum),
	}

	bh.wg.Add(1)
	go withRecover("buffer-helper-task", bh.runInner)
	return bh
}

func (bh *bufferHelper) runInner() {
	defer bh.wg.Done()
	batch := make([]IRecord, 0, bh.bufferNum)
	var timer *time.Timer
	var timerCh <-chan time.Time

	for {
		select {
		case record, ok := <-bh.recordCh:
			if !ok {
				// channel has closed, flush remained buffer
				if len(batch) > 0 {
					bh.batchCh <- batch
				}
				if timer != nil {
					timer.Stop()
				}
				return
			}

			if len(batch) == 0 {
				timer = time.NewTimer(bh.bufferTime)
				timerCh = timer.C
			}

			batch = append(batch, record)

			if len(batch) >= bh.bufferNum {
				if timer != nil {
					timer.Stop()
					timer = nil
					timerCh = nil
				}

				bh.batchInput() <- batch
				batch = make([]IRecord, 0, bh.bufferNum)
			}
		case <-timerCh:
			if timer != nil {
				timer.Stop()
				timer = nil
				timerCh = nil
			}

			if len(batch) > 0 {
				bh.batchCh <- batch
				batch = make([]IRecord, 0, bh.bufferNum)
			}
		}
	}
}

func (bh *bufferHelper) input() chan<- IRecord {
	return bh.recordCh
}

func (bh *bufferHelper) batchInput() chan<- []IRecord {
	return bh.batchCh
}

func (bh *bufferHelper) output() <-chan []IRecord {
	return bh.batchCh
}

func (bh *bufferHelper) close() {
	close(bh.recordCh)
	bh.wg.Wait()
	close(bh.batchCh)
}
