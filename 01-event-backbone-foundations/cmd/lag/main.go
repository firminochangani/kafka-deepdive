package main

import (
	"context"
	"fmt"

	"github.com/firminochangani/kafka-deepdive/01-event-backbone-foundations/internal/common"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 8. **Lag reporting.** A command that, for a given group, prints per-partition: current offset,
// log end offset, and lag. Do not shell out to `kafka-consumer-groups.sh` — read it from the
// Admin API.

func main() {
	err := run()
	if err != nil {
		fmt.Println(err)
	}
}

func run() error {
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumerGroup(common.ListingIndexerV2ConsumerGroup),
	)
	if err != nil {
		return err
	}

	kafkaAdminClient := kadm.NewClient(kafkaClient)

	ctx := context.Background()
	kafkaAdminClient.DescribeConsumerGroups()

	return nil
}
