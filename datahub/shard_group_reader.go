package datahub

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

const lowWaterMarkRatio = 0.5

type shardGroupReader struct {
	project       string
	topic         string
	client        DataHubApi
	offsetManager *offsetManager
	config        *ConsumerConfig

	mu      sync.RWMutex
	readers []*shardReader

	recordChan chan IRecord

	index  atomic.Int32
	flying atomic.Int32
	closed atomic.Bool

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newShardGroupReader(project, topic string, client DataHubApi,
	offsetManager *offsetManager, config *ConsumerConfig) *shardGroupReader {
	sgr := &shardGroupReader{
		project:       project,
		topic:         topic,
		client:        client,
		offsetManager: offsetManager,
		config:        config,
		readers:       make([]*shardReader, 0),
		recordChan:    make(chan IRecord, config.BufferNumber),
		stopCh:        make(chan struct{}),
	}
	return sgr
}

func (sgr *shardGroupReader) start() {
	sgr.wg.Add(1)
	go sgr.run()
}

func (sgr *shardGroupReader) run() {
	defer sgr.wg.Done()

	fetchInterval := time.Millisecond * 100
	ticker := time.NewTicker(fetchInterval)
	defer ticker.Stop()

	for {
		select {
		case <-sgr.stopCh:
			return
		case <-ticker.C:
			sgr.doFetch()
		}
	}
}

func (sgr *shardGroupReader) addShards(shardIds []string, offsets map[string]SubscriptionOffset) {
	sgr.mu.Lock()
	defer sgr.mu.Unlock()

	// 构建已有 shard 的 map
	existing := make(map[string]bool)
	for _, r := range sgr.readers {
		existing[r.shardId] = true
	}

	for _, shardId := range shardIds {
		if existing[shardId] {
			continue
		}

		offset := offsets[shardId]
		reader := newShardReader(
			sgr.project, sgr.topic, shardId,
			sgr.client, sgr.offsetManager,
			sgr.config,
		)

		if err := reader.start(offset); err != nil {
			log.Errorf("%s/%s/%s Start shardReader failed: %v",
				sgr.project, sgr.topic, shardId, err)
			continue
		}

		sgr.readers = append(sgr.readers, reader)
		existing[shardId] = true
		log.Infof("%s/%s/%s ShardReader added", sgr.project, sgr.topic, shardId)
	}
}

func (sgr *shardGroupReader) removeShards(shardIds []string) {
	sgr.mu.Lock()
	defer sgr.mu.Unlock()

	removeSet := make(map[string]bool)
	for _, shardId := range shardIds {
		removeSet[shardId] = true
	}

	newReaders := make([]*shardReader, 0, len(sgr.readers))
	for _, reader := range sgr.readers {
		if removeSet[reader.shardId] {
			reader.stop()
			log.Infof("%s/%s/%s ShardReader removed", sgr.project, sgr.topic, reader.shardId)
		} else {
			newReaders = append(newReaders, reader)
		}
	}
	sgr.readers = newReaders
}

func (sgr *shardGroupReader) read(timeout time.Duration) (IRecord, error) {
	if sgr.closed.Load() {
		return nil, fmt.Errorf("shardGroupReader closed")
	}

	// Check water mark and trigger fetch if needed
	if len(sgr.recordChan) < int(float64(sgr.config.BufferNumber)*lowWaterMarkRatio) {
		sgr.doFetch()
	}

	// Non-blocking mode
	if timeout == 0 {
		select {
		case record := <-sgr.recordChan:
			sgr.handleRecord(record)
			return record, nil
		default:
			return nil, nil
		}
	}

	// Blocking with timeout
	select {
	case record := <-sgr.recordChan:
		sgr.handleRecord(record)
		return record, nil
	case <-time.After(timeout):
		return nil, nil
	}
}

func (sgr *shardGroupReader) doFetch() {
	// Check if closed
	if sgr.closed.Load() {
		return
	}

	// Check inflight limit
	if int(sgr.flying.Load()) >= sgr.config.MaxInflightFetch {
		return
	}

	// Check buffer capacity
	if len(sgr.recordChan) >= sgr.config.BufferNumber {
		return
	}

	reader := sgr.selectShardReader()
	if reader == nil {
		return
	}

	// Fetch asynchronously
	go func(r *shardReader) {
		// Increment flying
		sgr.flying.Add(1)
		defer sgr.flying.Add(-1)

		records, err := r.tryFetch()
		if err != nil {
			if !IsShardSealedError(err) {
				log.Warnf("%s/%s/%s Fetch failed: %v", sgr.project, sgr.topic, r.shardId, err)
			}
		} else if len(records) > 0 {
			sgr.pushToChannel(records)
		}
	}(reader)
}

func (sgr *shardGroupReader) selectShardReader() *shardReader {
	sgr.mu.RLock()
	defer sgr.mu.RUnlock()

	if len(sgr.readers) == 0 {
		return nil
	}

	if sgr.config.FetchStrategy == FetchRoundRobin {
		// FetchRoundRobin: select by round-robin, skip not ready shards
		start := int(sgr.index.Add(1)) % len(sgr.readers)
		for i := 0; i < len(sgr.readers); i++ {
			idx := (start + i) % len(sgr.readers)
			reader := sgr.readers[idx]
			if reader.isReady() {
				return reader
			}
		}
		return nil
	}

	// FetchBalance: select shard with oldest SystemTime
	var reader *shardReader
	var minTime int64 = -1
	for _, r := range sgr.readers {
		if !r.isReady() {
			continue
		}
		t := r.getLastSystemTime()
		if minTime < 0 || t < minTime {
			minTime = t
			reader = r
		}
	}
	return reader
}

func (sgr *shardGroupReader) pushToChannel(records []IRecord) {
	for _, record := range records {
		select {
		case sgr.recordChan <- record:
		case <-sgr.stopCh:
			return
		}
	}
}

func (sgr *shardGroupReader) handleRecord(record IRecord) {
	if sgr.config.AutoRecordAck && record.GetRecordKey() != nil {
		record.GetRecordKey().Ack()
	}
}

func (sgr *shardGroupReader) getHoldShards() []string {
	sgr.mu.RLock()
	defer sgr.mu.RUnlock()

	shards := make([]string, 0, len(sgr.readers))
	for _, r := range sgr.readers {
		shards = append(shards, r.shardId)
	}
	return shards
}

func (sgr *shardGroupReader) getSealedShards() []string {
	sgr.mu.RLock()
	defer sgr.mu.RUnlock()

	shards := make([]string, 0)
	for _, r := range sgr.readers {
		if r.isSealed() {
			shards = append(shards, r.shardId)
		}
	}
	return shards
}

func (sgr *shardGroupReader) stop() {
	if !sgr.closed.CompareAndSwap(false, true) {
		return
	}

	close(sgr.stopCh)
	sgr.wg.Wait()

	// Wait for all inflight fetch goroutines to finish
	for sgr.flying.Load() > 0 {
		time.Sleep(10 * time.Millisecond)
	}

	t2 := time.Now()
	sgr.mu.Lock()
	defer sgr.mu.Unlock()

	for _, reader := range sgr.readers {
		reader.stop()
	}
	sgr.readers = nil
	close(sgr.recordChan)
	log.Infof("%s/%s close readers and channel took %v", sgr.project, sgr.topic, time.Since(t2))
}
