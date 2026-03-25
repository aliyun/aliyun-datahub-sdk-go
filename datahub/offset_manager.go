package datahub

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type recordKeyImpl struct {
	shardId    string
	sequence   int64
	batchIndex uint32
	timestamp  int64
	acked      atomic.Bool
}

func (rk *recordKeyImpl) Ack() {
	rk.acked.Store(true)
}

func (rk *recordKeyImpl) isAcked() bool {
	return rk.acked.Load()
}

func newRecordKey(shardId string, sequence int64, batchIndex uint32, timestamp int64) *recordKeyImpl {
	return &recordKeyImpl{
		shardId:    shardId,
		sequence:   sequence,
		batchIndex: batchIndex,
		timestamp:  timestamp,
	}
}

type shardOffsetInfo struct {
	mu           sync.Mutex
	offset       SubscriptionOffset
	pendingQueue []*recordKeyImpl
	lastCommit   struct {
		sequence   int64
		batchIndex uint32
		timestamp  int64
	}
}

type offsetManager struct {
	project        string
	topic          string
	subId          string
	client         DataHubApi
	commitInterval time.Duration

	mu         sync.RWMutex
	shardInfos map[string]*shardOffsetInfo

	commitChan chan struct{}
	closeChan  chan struct{}
	wg         sync.WaitGroup
}

func newOffsetManager(project, topic, subId string, client DataHubApi,
	commitInterval time.Duration) *offsetManager {
	return &offsetManager{
		project:        project,
		topic:          topic,
		subId:          subId,
		client:         client,
		commitInterval: commitInterval,
		shardInfos:     make(map[string]*shardOffsetInfo),
		commitChan:     make(chan struct{}, 1),
		closeChan:      make(chan struct{}),
	}
}

func (om *offsetManager) start() {
	om.wg.Add(1)
	go om.run()
}

func (om *offsetManager) stop() {
	close(om.closeChan)
	om.wg.Wait()
	log.Infof("%s/%s offsetManager stopped", om.project, om.topic)
}

func (om *offsetManager) run() {
	defer om.wg.Done()

	ticker := time.NewTicker(om.commitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			om.doCommit()
		case <-om.commitChan:
			om.doCommit()
		case <-om.closeChan:
			om.doCommit()
			return
		}
	}
}

func (om *offsetManager) addShards(shardIds []string) map[string]SubscriptionOffset {
	// OpenSubscriptionSession to get offsets
	result, err := om.client.OpenSubscriptionSession(om.project, om.topic, om.subId, shardIds)
	if err != nil {
		log.Errorf("%s/%s OpenSubscriptionSession failed: %v", om.project, om.topic, err)
		return nil
	}

	om.mu.Lock()
	defer om.mu.Unlock()

	for _, shardId := range shardIds {
		offset, ok := result.Offsets[shardId]
		if !ok {
			offset = SubscriptionOffset{
				Timestamp:  0,
				Sequence:   -1,
				BatchIndex: 0,
				VersionId:  -1,
			}
		}

		om.shardInfos[shardId] = &shardOffsetInfo{
			offset:       offset,
			pendingQueue: make([]*recordKeyImpl, 0),
		}
		om.shardInfos[shardId].lastCommit.sequence = offset.Sequence
		om.shardInfos[shardId].lastCommit.batchIndex = offset.BatchIndex
		om.shardInfos[shardId].lastCommit.timestamp = offset.Timestamp

		log.Infof("%s/%s/%s Add shard, timestamp: %d, sequence: %d, batchIndex: %d, ",
			om.project, om.topic, shardId, offset.Timestamp, offset.Sequence, offset.BatchIndex)
	}

	return result.Offsets
}

func (om *offsetManager) removeShards(shardIds []string) {
	om.mu.Lock()
	defer om.mu.Unlock()

	for _, shardId := range shardIds {
		if info, ok := om.shardInfos[shardId]; ok {
			om.commitShardOffset(shardId, info)
			delete(om.shardInfos, shardId)
			log.Infof("%s/%s/%s Remove shard", om.project, om.topic, shardId)
		}
	}
}

func (om *offsetManager) appendRecordKey(rk *recordKeyImpl) {
	om.mu.RLock()
	info, ok := om.shardInfos[rk.shardId]
	om.mu.RUnlock()

	if !ok {
		return
	}

	info.mu.Lock()
	info.pendingQueue = append(info.pendingQueue, rk)
	info.mu.Unlock()
}

func (om *offsetManager) doCommit() {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if len(om.shardInfos) == 0 {
		return
	}

	offsets := make(map[string]SubscriptionOffset)
	hasChange := false

	for shardId, info := range om.shardInfos {
		info.mu.Lock()
		newSeq, newBatch, newTimestamp := om.calculateCommitOffset(info)
		if newSeq > info.lastCommit.sequence ||
			(newSeq == info.lastCommit.sequence && newBatch > info.lastCommit.batchIndex) {
			offsets[shardId] = SubscriptionOffset{
				Timestamp:  newTimestamp,
				Sequence:   newSeq,
				BatchIndex: newBatch,
				VersionId:  info.offset.VersionId,
				SessionId:  info.offset.SessionId,
			}
			info.lastCommit.sequence = newSeq
			info.lastCommit.batchIndex = newBatch
			info.lastCommit.timestamp = newTimestamp
			hasChange = true
		}
		info.mu.Unlock()
	}

	if !hasChange {
		log.Infof("%s/%s No offset change, skip commit", om.project, om.topic)
		return
	}

	_, err := om.client.CommitSubscriptionOffset(om.project, om.topic, om.subId, offsets)
	if err != nil {
		log.Errorf("%s/%s CommitOffset failed: %v", om.project, om.topic, err)
		return
	}

	// Format offsets as "shardId:timestamp-sequence-batchIndex,..."
	var parts []string
	for shardId, offset := range offsets {
		parts = append(parts, fmt.Sprintf("%s:%d-%d-%d", shardId, offset.Timestamp, offset.Sequence, offset.BatchIndex))
	}

	log.Infof("%s/%s CommitOffset success, offsets: %v", om.project, om.topic, strings.Join(parts, ", "))
}

func (om *offsetManager) calculateCommitOffset(info *shardOffsetInfo) (sequence int64, batchIndex uint32, timestamp int64) {
	sequence = info.lastCommit.sequence
	batchIndex = info.lastCommit.batchIndex
	timestamp = info.lastCommit.timestamp
	removed := 0

	for _, rk := range info.pendingQueue {
		if rk.isAcked() {
			sequence = rk.sequence
			batchIndex = rk.batchIndex
			timestamp = rk.timestamp
			removed++
		} else {
			break // 遇到未 ack 的就停止
		}
	}

	// 移除已处理的项
	if removed > 0 {
		info.pendingQueue = info.pendingQueue[removed:]
	}

	return sequence, batchIndex, timestamp
}

func (om *offsetManager) commitShardOffset(shardId string, info *shardOffsetInfo) {
	info.mu.Lock()
	newSeq, newBatch, newTimestamp := om.calculateCommitOffset(info)
	info.mu.Unlock()

	if newSeq < info.lastCommit.sequence ||
		(newSeq == info.lastCommit.sequence && newBatch <= info.lastCommit.batchIndex) {
		return
	}

	offsets := map[string]SubscriptionOffset{
		shardId: {
			Timestamp:  newTimestamp,
			Sequence:   newSeq,
			BatchIndex: newBatch,
			VersionId:  info.offset.VersionId,
			SessionId:  info.offset.SessionId,
		},
	}

	_, err := om.client.CommitSubscriptionOffset(om.project, om.topic, om.subId, offsets)
	if err != nil {
		log.Errorf("%s/%s/%s CommitOffset failed: %v", om.project, om.topic, shardId, err)
		return
	}

	info.lastCommit.sequence = newSeq
	info.lastCommit.batchIndex = newBatch
	info.lastCommit.timestamp = newTimestamp
	log.Infof("%s/%s/%s CommitOffset success, timestamp: %d, sequence: %d, batchIndex: %d",
		om.project, om.topic, shardId, newTimestamp, newSeq, newBatch)
}

func (om *offsetManager) getOffset(shardId string) SubscriptionOffset {
	om.mu.RLock()
	defer om.mu.RUnlock()

	if info, ok := om.shardInfos[shardId]; ok {
		info.mu.Lock()
		defer info.mu.Unlock()
		return info.offset
	}
	return SubscriptionOffset{Timestamp: 0, Sequence: -1, BatchIndex: 0}
}
