package kafkaadmin

import (
	"context"
	"fmt"
	"github.com/docker/go-connections/nat"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/tmstff/kafka-testing-go"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"testing"
)

func TestTopicCreation(t *testing.T) {

	ctx := context.Background()

	kafkaURL, terminateKafka, err := StartKafkaContainer(ctx)
	defer terminateKafka(ctx)

	assert.Nil(t, err, "could not start kafka")

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaURL,
	})

	assert.Nil(t, err, "could create adminClient")

	configs, err := adminClient.DescribeConfigs(ctx, []kafka.ConfigResource{{
		Type: kafka.ResourceTopic,
		Name: "test",
	}})

	assert.Nil(t, err, "DescribeConfigs failed")

	assert.NotNil(t, configs)

}

type TestLogConsumer struct {
	containerName string
}

func (g *TestLogConsumer) Accept(l testcontainers.Log) {
	logrus.Infof("%s: %s", g.containerName, string(l.Content))
}

func StartKafkaContainer(ctx context.Context) (kafkaUrl string, terminateFunction func(context.Context) error, err error) {

	var containerHost string
	var kafkaPortExpositionHost string
	if val, exists := os.LookupEnv("HOST_IP"); exists {
		containerHost = val
		kafkaPortExpositionHost = val
	} else {
		containerHost = kafkatesting.GetOutboundIP()
		kafkaPortExpositionHost = "0.0.0.0"
	}

	zookeeperConnect, shutdownZookeeper, err := startZookeeperContainer(ctx, containerHost, kafkaPortExpositionHost)

	logrus.Infof("container host: %s, kafkaPortExpositionHost: %s ", containerHost, kafkaPortExpositionHost)

	hostPortInt, err := kafkatesting.GetFreePort(kafkaPortExpositionHost)
	if err != nil {
		return "", nil, err
	}

	hostPort := fmt.Sprintf("%d", hostPortInt)

	logrus.Infof("kafka host port: %s", hostPort)

	natContainerPort := "9092/tcp"

	// hostPort:containerPort/protocol
	portExposition := kafkaPortExpositionHost + ":" + hostPort + ":" + natContainerPort

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-kafka:5.4.0",
		ExposedPorts: []string{portExposition},
		WaitingFor:   wait.ForListeningPort(nat.Port(natContainerPort)),
		Env: map[string]string{
			"KAFKA_LISTENERS": fmt.Sprintf("DEFAULT://%s:%s", containerHost, hostPort),
			"KAFKA_ADVERTISED_LISTENERS": fmt.Sprintf("DEFAULT://%s:%s", containerHost, hostPort),
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "DEFAULT:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME": "DEFAULT",
			"KAFKA_ZOOKEEPER_CONNECT": zookeeperConnect,
		},
	}

	logrus.Info("Starting up kafka container")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, err
	}

	err = container.StartLogProducer(ctx)
	if err != nil {
		logrus.Errorf("could not start log producer: %s", err)
	}

	logConsumer := TestLogConsumer {containerName: "kafka"}

	container.FollowOutput(&logConsumer)

	address := fmt.Sprintf("%s:%s", containerHost, hostPort)

	logrus.Infof("kafka container started at %s", address)

	return address, func(ctx context.Context) error {

		err = container.StopLogProducer()
		if err != nil {
			logrus.Errorf("could not stop log producer: %s", err)
		}

		err := container.Terminate(ctx)

		if err != nil {
			return err
		}

		err = shutdownZookeeper(ctx)
		if err != nil {
			return err
		}

		return nil
	}, nil
}


func startZookeeperContainer(ctx context.Context, containerHost, portExpositionHost string) (zookeeperConnect string, terminateFunction func(context.Context) error, err error) {

	hostPortInt, err := kafkatesting.GetFreePort(portExpositionHost)
	if err != nil {
		return "", nil, err
	}


	hostPort := fmt.Sprintf("%d", hostPortInt)

	logrus.Infof("zookeeper host port: %s", hostPort)

	natContainerPort := "2181/tcp"

	// hostPort:containerPort/protocol
	portExposition := portExpositionHost + ":" + hostPort + ":" + natContainerPort

	req := testcontainers.ContainerRequest{
		Image:        "confluentinc/cp-zookeeper:5.4.0",
		ExposedPorts: []string{portExposition},
		WaitingFor:   wait.ForListeningPort(nat.Port(natContainerPort)),
		Env: map[string]string{
			"ZOOKEEPER_CLIENT_PORT": "2181",
		},
	}

	logrus.Info("Starting up zookeeper container")
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return "", nil, err
	}

	address := fmt.Sprintf("%s:%s", containerHost, hostPort)

	err = container.StartLogProducer(ctx)
	if err != nil {
		logrus.Errorf("could not start log producer: %s", err)
	}

	logConsumer := TestLogConsumer {containerName: "zookeeper"}

	container.FollowOutput(&logConsumer)

	logrus.Infof("kafka container started at %s", address)

	return address, func(ctx context.Context) error {

		err = container.StopLogProducer()
		if err != nil {
			logrus.Errorf("could not stop log producer: %s", err)
		}

		err := container.Terminate(ctx)

		if err != nil {
			return err
		}
		return nil
	}, nil
}