package datahub

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

// Consumer provides high-level consumption API.
type Consumer interface {
	Init() error
	Read(timeout time.Duration) (IRecord, error)
	GetCurrentShards() []string
	Close() error
}

type consumerImpl struct {
	config           *ConsumerConfig
	project          string
	topic            string
	client           DataHubApi
	groupManager     *groupManager
	offsetManager    *offsetManager
	shardGroupReader *shardGroupReader
}

func NewConsumer(cfg *ConsumerConfig) Consumer {
	return &consumerImpl{
		config:  cfg,
		project: cfg.Project,
		topic:   cfg.Topic,
	}
}

func (ci *consumerImpl) Init() error {

	tmpClient := NewClientWithConfig(ci.config.Endpoint, NewDefaultConfig(), ci.config.Account)
	res, err := tmpClient.GetTopic(ci.project, ci.topic)
	fmt.Println(err)
	if err != nil {
		return err
	}

	config := NewDefaultConfig()

	if res.extraConfig.compressType != NOCOMPRESS {
		config.CompressorType = res.extraConfig.compressType
	}

	if res.EnableSchema {
		config.Protocol = Batch
	} else {
		if res.extraConfig.protocol != unknownProtocol {
			config.Protocol = res.extraConfig.protocol
		} else {
			config.Protocol = ci.config.Protocol
		}
	}

	userAgent := defaultClientAgent()
	if len(ci.config.UserAgent) > 0 {
		userAgent = userAgent + " " + ci.config.UserAgent
	}

	ci.client = NewClientWithConfig(ci.config.Endpoint, config, ci.config.Account)
	ci.client.setUserAgent(userAgent)

	ci.groupManager = newGroupManager(ci.project, ci.topic, ci.config.SubId, ci.client, ci.config.SessionTimeout)
	ci.offsetManager = newOffsetManager(ci.project, ci.topic, ci.config.SubId, ci.client,
		ci.config.CommitInterval)
	ci.shardGroupReader = newShardGroupReader(ci.project, ci.topic, ci.client,
		ci.offsetManager, ci.config)

	ci.groupManager.setManagers(ci.offsetManager, ci.shardGroupReader)

	if err := ci.groupManager.start(); err != nil {
		return fmt.Errorf("init group manager failed: %w", err)
	}
	ci.offsetManager.start()
	ci.shardGroupReader.start()

	log.Infof("%s/%s Consumer initialized success", ci.project, ci.topic)
	return nil
}

func (ci *consumerImpl) Read(timeout time.Duration) (IRecord, error) {
	return ci.shardGroupReader.read(timeout)
}

func (ci *consumerImpl) GetCurrentShards() []string {
	return ci.shardGroupReader.getHoldShards()
}

func (ci *consumerImpl) Close() error {
	log.Infof("%s/%s Consumer closing...", ci.project, ci.topic)
	start := time.Now()

	if ci.shardGroupReader != nil {
		ci.shardGroupReader.stop()
	}
	if ci.offsetManager != nil {
		ci.offsetManager.stop()
	}
	if ci.groupManager != nil {
		_ = ci.groupManager.leaveGroup()
		ci.groupManager.stop()
	}

	log.Infof("%s/%s Consumer closed, total took %v", ci.project, ci.topic, time.Since(start))
	return nil
}
