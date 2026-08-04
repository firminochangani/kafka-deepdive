package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const topicName = "marketplace.listings.v1"

func main() {
	client, err := kgo.NewClient(kgo.SeedBrokers([]string{"localhost:9092"}...))
	if err != nil {
		panic(err)
	}

	adminClient := kadm.NewClient(client)
	defer adminClient.Close()

	resp, err := adminClient.CreateTopic(
		context.Background(),
		6,
		1,
		map[string]*string{
			"cleanup.policy": new("delete"),
		},
		topicName,
	)
	if errors.Is(err, kerr.TopicAlreadyExists) {
		fmt.Printf("topic '%s' already exists\n", topicName)
		return
	}
	if err != nil {
		panic(err)
	}

	fmt.Printf("topic '%s' created successfully\n", topicName)
	fmt.Printf(
		"topic: ID: '%s' - Number of partitions: '%d' - Replication factor: '%d'\n",
		resp.ID,
		resp.NumPartitions,
		resp.ReplicationFactor,
	)
}
