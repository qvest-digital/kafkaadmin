package kafkaadmin

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTopicCreation(t *testing.T) {

	ctx := context.Background()

	kafkaUrl, terminateKafka, err := StartKafkaWithEnv(ctx, map[string]string{"KAFKA_AUTO_CREATE_TOPICS_ENABLE": "false"})
	if err != nil {
		assert.Fail(t, err.Error())
	} else {
		defer terminateKafka(ctx)
	}

	rand.Seed(time.Now().UnixNano())
	topic := strconv.FormatUint(rand.Uint64(), 10)

	config := DefaultConfig(topic)
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

type TestLogConsumer struct {
	containerName string
}

func (g *TestLogConsumer) Accept(l testcontainers.Log) {
	logrus.Infof("%s: %s", g.containerName, string(l.Content))
}

// deprecated
func StartKafkaContainer(ctx context.Context) (kafkaUrl string, terminateFunction func(context.Context) error, err error) {
	return StartKafkaWithEnv(ctx, map[string]string{})
}

func StartKafka(ctx context.Context) (kafkaUrl string, terminateFunction func(context.Context) error, err error) {
	return StartKafkaWithEnv(ctx, map[string]string{})
}

func StartKafkaWithEnv(ctx context.Context, env map[string]string) (kafkaUrl string, terminateFunction func(context.Context) error, err error) {
	var containerHost string
	var kafkaPortExpositionHost string
	if val, exists := os.LookupEnv("HOST_IP"); exists {
		containerHost = val
		kafkaPortExpositionHost = val
	} else {
		containerHost = GetOutboundIP()
		kafkaPortExpositionHost = "0.0.0.0"
	}

	hostPortInt, err := GetFreePort(kafkaPortExpositionHost)
	if err != nil {
		return "", nil, err
	}

	logrus.Infof("containerHost: %s, kafkaPortExpositionHost: %s ", containerHost, kafkaPortExpositionHost)

	hostPort := fmt.Sprintf("%d", hostPortInt)
	kafkaExternalAdvertisedListener := containerHost + ":" + hostPort
	logrus.Debugf("kafkaExternalAdvertisedListener: %s", kafkaExternalAdvertisedListener)

	// hostPort:containerPort/protocol
	portExpositionString := kafkaPortExpositionHost + ":" + hostPort + ":9092"
	logrus.Debugf("kafka port exposition: %s", portExpositionString)

	identifier := strings.ToLower(uuid.New().String())

	composeFilePaths := []string{"docker-compose.yml"}

	compose := testcontainers.NewLocalDockerCompose(composeFilePaths, identifier)

	envWithDefaults := map[string]string{
		"KAFKA_PORT_EXPOSITION":              portExpositionString,
		"KAFKA_EXTERNAL_ADVERTISED_LISTENER": kafkaExternalAdvertisedListener,
	}

	for k,v := range env {
		envWithDefaults[k] = v
	}

	execError := compose.
		WithCommand([]string{"up", "-d"}).
		WithEnv(envWithDefaults).
		Invoke()
	if execError.Error != nil {
		logrus.Errorf("%s failed with\nstdout:\n%v\nstderr:\n%v", execError.Command, execError.Stdout, execError.Stderr)
		return "", nil, fmt.Errorf("could not run compose file: %v - %w", composeFilePaths, execError.Error)
	}

	logrus.Infof("kafka started at %s", kafkaExternalAdvertisedListener)

	return kafkaExternalAdvertisedListener, func(ctx context.Context) error {
		execError := compose.Down()
		if execError.Error != nil {
			logrus.Errorf("%s failed with\nstdout:\n%v\nstderr:\n%v", execError.Command, execError.Stdout, execError.Stderr)
			return fmt.Errorf("'docker-compose down' failed for compose file: %v - %w", composeFilePaths, err)
		}
		return nil
	}, nil
}

// inspired by https://github.com/phayes/freeport
func GetFreePort(host string) (int, error) {
	hostWithPort := fmt.Sprintf("%s:0", host)
	addr, err := net.ResolveTCPAddr("tcp", hostWithPort)
	if err != nil {
		fmt.Printf("could not resolve %s: %s", hostWithPort, err)
		return GetFreePortFallback(host)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		fmt.Printf("could not listen to %s: %s", hostWithPort, err)
		return GetFreePortFallback(host)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func GetFreePortFallback(host string) (int, error) {
	port := getRandomPort()
	for isOpen(host, port) {
		port = getRandomPort()
	}
	return port, nil
}

func getRandomPort() int {
	rand.Seed(time.Now().UnixNano())
	min := 49152
	max := 65535
	return rand.Intn(max - min) + min
}

func isOpen(host string, port int) bool {
	return raw_connect(host, strconv.Itoa(port)) == nil
}

// see https://stackoverflow.com/questions/56336168/golang-check-tcp-port-open
func raw_connect(host string, port string) error {
	timeout := time.Second
	hostAndPort := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", hostAndPort, timeout)
	if err != nil {
		fmt.Printf("could not connect to %s: %s - (which is good, because we are looking for an unused port ;-) )", hostAndPort, err)
		return err
	}
	defer conn.Close()
	return nil
}

// Get preferred outbound ip of this machine
// see https://stackoverflow.com/questions/23558425/how-do-i-get-the-local-ip-address-in-go
func GetOutboundIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}
