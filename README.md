# KafkaAdmin Library

Is used to ensure that a topic exists. It's API doesn't support configuration of
the topics to avoid conflicts between different services using the same topic.
Each service, should call this lib on booting. It doesn't matter whether it
reads or writes to the topic.

Every topic created has the cleanup.policy compact, a replication of 3 and 32
partitions, as this is a sane default for our platform.

## Usage

In order to be able to fetch this library, you have to configure your machine,
because this lib is fetched from a private repository.

// TODO
1. add .gitconfig for translating https to git/ssh

And Gitlab

// TODO

In code you can use it like this:

```
err := kafkaadmin.EnsureTopicExists(ctx, zookeeperURL, kafkaURL, topicName)
if err != nil {
	return fmt.Errorf("ensuring topic %q failed: %w", topicName, err)
}
```

## Development

// TODO vendoring

Made with ♥ by Team Hasselhoff
