package datahub

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type groupManager struct {
	project        string
	topic          string
	subId          string
	client         DataHubApi
	sessionTimeout time.Duration

	mu               sync.RWMutex
	consumerId       string
	versionId        int64
	joined           bool
	offsetManager    *offsetManager
	shardGroupReader *shardGroupReader

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func newGroupManager(project, topic, subId string, client DataHubApi, sessionTimeout time.Duration) *groupManager {
	return &groupManager{
		project:        project,
		topic:          topic,
		subId:          subId,
		client:         client,
		sessionTimeout: sessionTimeout,
		stopCh:         make(chan struct{}),
	}
}

func (gm *groupManager) setManagers(offsetManager *offsetManager, shardGroupReader *shardGroupReader) {
	gm.offsetManager = offsetManager
	gm.shardGroupReader = shardGroupReader
}

func (gm *groupManager) start() error {
	if err := gm.joinGroup(); err != nil {
		return err
	}
	gm.wg.Add(1)
	go gm.run()
	return nil
}

func (gm *groupManager) stop() {
	close(gm.stopCh)
	gm.wg.Wait()
	if gm.joined {
		gm.leaveGroup()
	}
}

func (gm *groupManager) run() {
	defer gm.wg.Done()

	heartbeatInterval := gm.sessionTimeout * 2 / 3
	heartbeatTimer := time.NewTicker(heartbeatInterval)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-heartbeatTimer.C:
			gm.doHeartbeat()
		case <-gm.stopCh:
			return
		}
	}
}

func (gm *groupManager) doHeartbeat() {
	holdShards := gm.shardGroupReader.getHoldShards()
	sealedShards := gm.shardGroupReader.getSealedShards()

	result, err := gm.heartbeat(holdShards, sealedShards)
	if err != nil {
		log.Errorf("%s/%s Heartbeat failed: %v", gm.project, gm.topic, err)
		return
	}

	changed, toAdd, toRemove := gm.shardChanged(result.ShardList, holdShards)
	if !changed {
		log.Infof("%s/%s Heartbeat success, no shard change, hold shards: %v", gm.project, gm.topic, holdShards)
	} else {
		log.Infof("%s/%s Heartbeat detected shard change, add: %v, remove: %v", gm.project, gm.topic, toAdd, toRemove)
		gm.handleShardChange(toAdd, toRemove)
	}
}

func (gm *groupManager) shardChanged(newShards, holdShards []string) (bool, []string, []string) {
	newShardSet := make(map[string]bool)
	for _, s := range newShards {
		newShardSet[s] = true
	}

	holdShardSet := make(map[string]bool)
	for _, s := range holdShards {
		holdShardSet[s] = true
	}

	var toAdd, toRemove []string
	for _, s := range newShards {
		if !holdShardSet[s] {
			toAdd = append(toAdd, s)
		}
	}

	for _, s := range holdShards {
		if !newShardSet[s] {
			toRemove = append(toRemove, s)
		}
	}

	if len(toAdd) == 0 && len(toRemove) == 0 {
		return false, nil, nil
	}
	return true, toAdd, toRemove
}

func (gm *groupManager) handleShardChange(toAdd, toRemove []string) {
	if len(toRemove) > 0 {
		gm.offsetManager.removeShards(toRemove)
		gm.shardGroupReader.removeShards(toRemove)
		gm.syncGroup(toRemove, nil)
	}

	if len(toAdd) > 0 {
		offsets := gm.offsetManager.addShards(toAdd)
		if offsets != nil {
			gm.shardGroupReader.addShards(toAdd, offsets)
		}
	}
}

func (gm *groupManager) joinGroup() error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	var result *JoinGroupResult
	var err error

	for i := 0; i < 3; i++ {
		result, err = gm.client.JoinGroup(gm.project, gm.topic, gm.subId, int64(gm.sessionTimeout/time.Millisecond))
		if err == nil {
			break
		}

		if IsServiceInProcessError(err) && i < 2 {
			log.Warnf("%s/%s JoinGroup got ServiceInProcessError, retrying... (%d/3)", gm.project, gm.topic, i+1)
			time.Sleep(time.Second)
			continue
		}
		return fmt.Errorf("JoinGroup failed: %w", err)
	}

	gm.consumerId = result.ConsumerId
	gm.versionId = result.VersionId
	gm.joined = true

	log.Infof("%s/%s JoinGroup success, consumerId: %s, versionId: %d",
		gm.project, gm.topic, gm.consumerId, gm.versionId)
	return nil
}

func (gm *groupManager) heartbeat(holdShards, readEndShards []string) (*HeartbeatResult, error) {
	gm.mu.RLock()
	consumerId := gm.consumerId
	versionId := gm.versionId
	gm.mu.RUnlock()

	if !gm.joined {
		return nil, fmt.Errorf("not joined to consumer group")
	}

	result, err := gm.client.Heartbeat(
		gm.project, gm.topic, gm.subId, consumerId,
		versionId, holdShards, readEndShards,
	)
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (gm *groupManager) syncGroup(releaseShards, readEndShards []string) (*SyncGroupResult, error) {
	gm.mu.RLock()
	consumerId := gm.consumerId
	versionId := gm.versionId
	gm.mu.RUnlock()

	if !gm.joined {
		return nil, fmt.Errorf("not joined to consumer group")
	}

	result, err := gm.client.SyncGroup(
		gm.project, gm.topic, gm.subId, consumerId,
		versionId, releaseShards, readEndShards,
	)
	if err != nil {
		log.Errorf("%s/%s SyncGroup failed: %v", gm.project, gm.topic, err)
		return nil, err
	}

	log.Infof("%s/%s SyncGroup success, release: %v, readEnd: %v",
		gm.project, gm.topic, releaseShards, readEndShards)
	return result, nil
}

func (gm *groupManager) leaveGroup() error {
	gm.mu.Lock()
	defer gm.mu.Unlock()

	if !gm.joined {
		return nil
	}

	_, err := gm.client.LeaveGroup(gm.project, gm.topic, gm.subId, gm.consumerId, gm.versionId)
	if err != nil {
		log.Errorf("%s/%s LeaveGroup failed: %v", gm.project, gm.topic, err)
		return err
	}

	gm.joined = false
	log.Infof("%s/%s LeaveGroup success", gm.project, gm.topic)
	return nil
}
