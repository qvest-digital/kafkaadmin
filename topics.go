package kafkaadmin

import (
	"context"
	"fmt"
	"github.com/fvosberg/errtypes"
	"github.com/segmentio/kafka-go"
	"time"
)

func EnsureTopicExists(ctx context.Context, zookeeperURL, kafkaURL, name string) error {
	for {
		err := ensureTopicExists(zookeeperURL, kafkaURL, name, 32)
		if err == nil || ctx.Err() != nil {
			return err
		}
	}
}

func ensureTopicExists(zookeeperURL, kafkaURL, name string, numPartitions int) error {
	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true, // IPv4 and IPv6
	}
	conn, err := dialer.Dial("tcp", kafkaURL)
	if err != nil {
		return err
	}
	err = hasTopic(kafkaURL, name)

	if err != nil && !errtypes.IsNotFound(err) {
		return fmt.Errorf("topic existence check failed: %w", err)
	}

	dialer = &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true, // IPv4 and IPv6
	}
	conn, err = dialer.Dial("tcp", kafkaURL)
	if err != nil {
		return err
	}
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             name,
		ReplicationFactor: 3,
		NumPartitions:     numPartitions,
	})
	if err != nil {
		return fmt.Errorf("creation of topic failed: %w", err)
	}

	err = waitForTopicExists(kafkaURL, name)
	if err != nil {
		return fmt.Errorf("waiting for topic creation failed: %w", err)
	}

	return nil
}

func waitForTopicExists(kafkaURL, name string) error {
	for retries := 100; retries > 0; retries-- {
		err := hasTopic(kafkaURL, name)
		if !errtypes.IsNotFound(err) && err != nil {
			return fmt.Errorf("topic check failed: %w", err)
		} else if err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errtypes.NewNotFound("topic doesn't exist")
}

func hasTopic(kafkaURL, name string) error {
	dialer := &kafka.Dialer{
		Timeout:   3 * time.Second,
		DualStack: true, // IPv4 and IPv6
	}
	conn, err := dialer.Dial("tcp", kafkaURL)
	if err != nil {
		return err
	}
	partitions, err := conn.ReadPartitions(name)
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

func isUnknownTopicOrPartitionError(err error) bool {
	v, ok := err.(kafka.Error)
	return ok && v == kafka.UnknownTopicOrPartition
}
