package kafkaadmin

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

// before running test start local kafka at localhost:9092
func TestTopicCreation(t *testing.T) {
	ctx := context.Background()

	rand.Seed(time.Now().UnixNano())
	topic := strconv.FormatUint(rand.Uint64(), 10)

	config := DefaultConfig(topic)
	config.ReplicationFactor = 1

	err := EnsureTopicExistsWithConfig(ctx, "localhost:9092", nil, config)

	assert.Nil(t, err)

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
	})

	assert.Nil(t, err, "could create adminClient")

	configResources, err := adminClient.DescribeConfigs(ctx, []kafka.ConfigResource{{
		Type: kafka.ResourceTopic,
		Name: topic,
	}})

	assert.Nil(t, err, "DescribeConfigs failed")
	assert.NotNil(t, configResources)

	for _, cr := range configResources {
		logrus.Infof("config resource %s", cr)
		for _, c := range cr.Config {
			logrus.Infof("config %s", c)
		}
	}

	assert.Equal(t, 1, len(configResources))
	assert.Equal(t, "compact", configResources[0].Config["cleanup.policy"].Value)

	// ensure it still works if topic is present
	err = EnsureTopicExistsWithConfig(ctx, "localhost:9092", nil, config)
	assert.Nil(t, err)
}