package kafkaadmin

import (
	"context"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	kafkatesting "github.com/tmstff/kafka-testing-go"
)

func TestTopicCreation(t *testing.T) {

	ctx := context.Background()

	kafkaUrl, terminateKafka, err := kafkatesting.StartKafkaWithEnv(ctx, map[string]string{"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "true"})
	if err != nil {
		assert.Fail(t, err.Error())
	} else {
		defer terminateKafka(ctx)
	}

	rand.Seed(time.Now().UnixNano())
	topic := strconv.FormatUint(rand.Uint64(), 10)

	config := CompactedTopicConfig(topic)
	config.ReplicationFactor = 1

	err = EnsureTopicExistsWithConfig(ctx, kafkaUrl, nil, config)

	assert.Nil(t, err)

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaUrl,
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
	err = EnsureTopicExistsWithConfig(ctx, kafkaUrl, nil, config)
	assert.Nil(t, err)
}
