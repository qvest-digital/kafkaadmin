# KafkaAdmin Library

Is used to ensure that a topic exists. It's API doesn't support configuration of
the topics to avoid conflicts between different services using the same topic.
Each service, should call this lib on booting. It doesn't matter whether it
reads or writes to the topic.

Every topic created has the cleanup.policy compact, a replication of 3 and 32
partitions, as this is a sane default for our platform.

## Usage

Because of the protected setup of the Bank11 it is quite tedious to use private
libs. We solve this problem for now by using GIT submodules.

### Add to Project

```
git submodule add https://b11pgitlab.bankelf.de/smive/kafkaadmin.git ./internal/kafkaadmin
```

In code you can use it like this:

```
err := kafkaadmin.EnsureTopicExists(ctx, kafkaURL, tlsConfig, name)
if err != nil {
	return fmt.Errorf("ensuring topic %q failed: %w", topicName, err)
}
```

### Clone / Update a Project with Submodules

If you clone a project using submodules for the first time, add a ```--recursive``` to the git command, e.g.

    git clone --recursive https://b11pgitlab.bankelf.de:4443/smive/offer-manager.git

If you already have a copy of the project (and the submodules have not been checked out yet), use

    git submodule init
    git submodule update

To get the latest changes of all submodules use

    git submodule update

Made with ♥ by Team Hasselhoff
