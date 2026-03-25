package datahub

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestConsumerConfigDefaults(t *testing.T) {
	cfg := NewConsumerConfig()

	assert.Equal(t, true, cfg.AutoRecordAck)
	assert.Equal(t, 500, cfg.FetchNumber)
	assert.Equal(t, 500, cfg.BufferNumber)
	assert.Equal(t, Batch, cfg.Protocol)
	assert.Equal(t, FetchRoundRobin, cfg.FetchStrategy)
	assert.Equal(t, 30*time.Second, cfg.CommitInterval)
	assert.Equal(t, 60*time.Second, cfg.SessionTimeout)
	assert.Equal(t, 3, cfg.MaxRetry)
	assert.Equal(t, 500*time.Millisecond, cfg.RetryInterval)
}

func TestConsumerConfigCustom(t *testing.T) {
	cfg := NewConsumerConfig()
	cfg.AutoRecordAck = false
	cfg.FetchNumber = 1000
	cfg.BufferNumber = 1000
	cfg.FetchStrategy = FetchBalance
	cfg.CommitInterval = 5 * time.Second
	cfg.SessionTimeout = 30 * time.Second
	cfg.Project = "my-project"
	cfg.Topic = "my-topic"
	cfg.SubId = "my-sub"

	assert.Equal(t, false, cfg.AutoRecordAck)
	assert.Equal(t, 1000, cfg.FetchNumber)
	assert.Equal(t, 1000, cfg.BufferNumber)
	assert.Equal(t, FetchBalance, cfg.FetchStrategy)
	assert.Equal(t, 5*time.Second, cfg.CommitInterval)
	assert.Equal(t, 30*time.Second, cfg.SessionTimeout)
	assert.Equal(t, "my-project", cfg.Project)
	assert.Equal(t, "my-topic", cfg.Topic)
	assert.Equal(t, "my-sub", cfg.SubId)
}
