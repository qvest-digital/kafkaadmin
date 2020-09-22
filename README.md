# KafkaAdmin Library

Is used to ensure that a topic exists. It's API doesn't support configuration of
the topics to avoid conflicts between different services using the same topic.
Each service, should call this lib on booting. It doesn't matter whether it
reads or writes to the topic.

Every topic created has the cleanup.policy compact, a replication of 3 and 32
partitions, as this is a sane default for our platform.

## Usage

In code you can use it like this:

```
err := kafkaadmin.EnsureCompactedTopicExists(ctx, kafkaURL, tlsConfig, topicName)
if err != nil {
	return fmt.Errorf("ensuring topic %q failed: %w", topicName, err)
}
```

Made with ♥ by Team Hasselhoff
