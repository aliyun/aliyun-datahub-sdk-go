package datahub

import (
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const emptyFetchDelay = 500 * time.Millisecond

type shardReader struct {
	project       string
	topic         string
	shardId       string
	client        DataHubApi
	offsetManager *offsetManager
	config        *ConsumerConfig

	cursor         string
	sealed         atomic.Bool // close shard read end
	fetching       atomic.Bool
	lastSystemTime atomic.Int64
	nextReadyTime  atomic.Int64 // timestamp when reader is ready after empty fetch
}

func newShardReader(project, topic, shardId string, client DataHubApi,
	offsetManager *offsetManager, config *ConsumerConfig) *shardReader {
	return &shardReader{
		project:       project,
		topic:         topic,
		shardId:       shardId,
		client:        client,
		offsetManager: offsetManager,
		config:        config,
	}
}

func (sr *shardReader) start(offset SubscriptionOffset) error {
	cursorType := LATEST
	var cursorResult *GetCursorResult
	var err error

	if offset.Sequence >= 0 {
		cursorResult, err = sr.client.GetCursor(sr.project, sr.topic, sr.shardId, SEQUENCE, offset.Sequence+1)
	} else {
		cursorResult, err = sr.client.GetCursor(sr.project, sr.topic, sr.shardId, cursorType)
	}

	if err != nil {
		return err
	}

	sr.cursor = cursorResult.Cursor
	sr.sealed.Store(false)

	log.Infof("%s/%s/%s ShardReader started, timestamp: %d, sequence:%d",
		sr.project, sr.topic, sr.shardId, cursorResult.RecordTime, cursorResult.Sequence)
	return nil
}

func (sr *shardReader) stop() {
	// nothing to do
}

func (sr *shardReader) tryFetch() ([]IRecord, error) {
	// Check if already fetching
	if !sr.fetching.CompareAndSwap(false, true) {
		return nil, nil
	}

	defer sr.fetching.Store(false)

	var lastErr error
	for i := 0; sr.config.MaxRetry < 0 || i <= sr.config.MaxRetry; i++ {
		records, err := sr.fetch()
		if err == nil {
			return records, nil
		}

		if IsShardSealedError(err) {
			return nil, err
		}

		if !IsRetryableError(err) {
			return nil, err
		}

		lastErr = err
		if i < sr.config.MaxRetry || sr.config.MaxRetry < 0 {
			time.Sleep(sr.config.RetryInterval)
		}
	}

	return nil, lastErr
}

func (sr *shardReader) fetch() ([]IRecord, error) {
	cursor := sr.cursor

	// getSchemaByVersionId(0): returns nil for blob, schema for tuple
	schemaCache := schemaClientInstance().getTopicSchemaCache(sr.project, sr.topic, sr.client)
	schema := schemaCache.getSchemaByVersionId(0)

	var result *GetRecordsResult
	var err error

	if schema != nil {
		result, err = sr.client.GetTupleRecords(sr.project, sr.topic, sr.shardId, cursor, sr.config.FetchNumber, schema)
	} else {
		result, err = sr.client.GetBlobRecords(sr.project, sr.topic, sr.shardId, cursor, sr.config.FetchNumber)
	}

	if err != nil {
		if IsShardSealedError(err) {
			sr.sealed.Store(true)
			log.Infof("%s/%s/%s Shard sealed", sr.project, sr.topic, sr.shardId)
		}
		return nil, err
	}

	if len(result.Records) == 0 {
		sr.nextReadyTime.Store(time.Now().Add(emptyFetchDelay).UnixMilli())
		return nil, nil
	}

	// Reset nextReadyTime since we got data
	sr.nextReadyTime.Store(0)

	// Update last system time from the last record
	lastRecord := result.Records[len(result.Records)-1]
	sr.lastSystemTime.Store(lastRecord.GetSystemTime())

	sr.cursor = result.NextCursor

	records := make([]IRecord, len(result.Records))
	for i, record := range result.Records {
		rk := newRecordKey(sr.shardId, record.GetSequence(), record.GetBatchIndex(), record.GetSystemTime())
		sr.offsetManager.appendRecordKey(rk)
		record.setRecordKey(rk)
		records[i] = record
	}

	return records, nil
}

func (sr *shardReader) isSealed() bool {
	return sr.sealed.Load()
}

func (sr *shardReader) isReady() bool {
	if sr.sealed.Load() || sr.fetching.Load() {
		return false
	}
	nextReady := sr.nextReadyTime.Load()
	if nextReady > 0 && time.Now().UnixMilli() < nextReady {
		return false
	}
	return true
}

func (sr *shardReader) getLastSystemTime() int64 {
	return sr.lastSystemTime.Load()
}
