package kafkaadmin

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"github.com/fvosberg/errtypes"
	"github.com/segmentio/kafka-go"
)

// EnsureCompactedTopicExists checks if the topic with the given name is missing
// if it doesn't exist, it creates it with the default configuration
// The configuration can't be altered currently, because we want to avoid conflicts
// by different services creating the same topic
func EnsureCompactedTopicExists(ctx context.Context, kafkaURL string, tlsConfig *tls.Config, topicName string) error {
	return EnsureTopicExistsWithConfig(ctx, kafkaURL, tlsConfig, CompactedTopicConfig(topicName))
}

func EnsureTopicExistsWithConfig(ctx context.Context, kafkaURL string, tlsConfig *tls.Config, topicConfig kafka.TopicConfig) error {
	errs := make(chan error, 1)
	var lastErr error
	for {
		go func() {
			err := ensureTopicExistsWithConfig(kafkaURL, tlsConfig, topicConfig)
			errs <- err
		}()

		select {
		case err := <-errs:
			if err == nil {
				return err
			}
			lastErr = err
		case <-ctx.Done():
			if lastErr != nil {
				return fmt.Errorf("%s: %w", lastErr, ctx.Err())
			}
			return ctx.Err()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func CompactedTopicConfig(topicName string) kafka.TopicConfig {
	return kafka.TopicConfig{
		Topic:             topicName,
		ReplicationFactor: 3,
		NumPartitions:     32,
		ConfigEntries: []kafka.ConfigEntry{
			{
				ConfigName:  "cleanup.policy",
				ConfigValue: "compact",
			},
		},
	}
}

func ensureTopicExistsWithConfig(kafkaURL string, tlsConfig *tls.Config, topicConfig kafka.TopicConfig) error {
	conn, err := open(kafkaURL, tlsConfig)

	if err != nil {
		return fmt.Errorf("connection to Kafka failed: %w", err)
	}

	err = conn.conn.CreateTopics(topicConfig)
	if err != nil {
		return fmt.Errorf("creation of topic failed: %w", err)
	}

	err = conn.waitForTopicExists(topicConfig.Topic)
	if err != nil {
		return fmt.Errorf("waiting for topic creation failed: %w", err)
	}

	return nil
}

type conn struct {
	conn *kafka.Conn
}

func open(kafkaURL string, tlsConfig *tls.Config) (*conn, error) {
	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true, // IPv4 and IPv6
		TLS:       tlsConfig,
	}

	c, err := dialer.Dial("tcp", kafkaURL)
	if err != nil {
		return nil, err
	}

	broker, err := c.Controller()
	if err != nil {
		return nil, fmt.Errorf("determining controller: %w", err)
	}
	c.Close()

	controllerUrl := fmt.Sprintf("%s:%d", broker.Host, broker.Port)
	if controllerUrl != kafkaURL {
		c, err = dialer.Dial("tcp", controllerUrl)
		if err != nil {
			return nil, err
		}
	}
	return &conn{c}, nil
}

func (c *conn) hasTopic(name string) error {
	partitions, err := c.conn.ReadPartitions(name)
	if isUnknownTopicOrPartitionError(err) {
		return errtypes.NewNotFoundf("not found: %s", err)
	} else if err != nil {
		return fmt.Errorf("reading partitions for topic creation verification failed: %w", err)
	}
	if len(partitions) == 0 {
		return errtypes.NewNotFound("topic doesn't exist")
	}
	return nil
}

func (c *conn) waitForTopicExists(name string) error {
	for retries := 100; retries > 0; retries-- {
		err := c.hasTopic(name)
		if !errtypes.IsNotFound(err) && err != nil {
			return fmt.Errorf("topic check failed: %w", err)
		} else if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errtypes.NewNotFound("topic doesn't exist")
}

func isUnknownTopicOrPartitionError(err error) bool {
	v, ok := err.(kafka.Error)
	return ok && v == kafka.UnknownTopicOrPartition
}
