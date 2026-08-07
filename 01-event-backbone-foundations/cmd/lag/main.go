package main

import (
	"context"
	"fmt"

	"github.com/firminochangani/kafka-deepdive/01-event-backbone-foundations/internal/common"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// CLI: ./opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group listing-indexer-v1
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
	resp, err := kafkaAdminClient.FetchOffsets(ctx, common.ListingIndexerV2ConsumerGroup)
	if err != nil {
		return err
	}

	resp.Each(func(o kadm.OffsetResponse) {
		fmt.Println(o.Partition, o.At)
	})

	lagResp, err := kafkaAdminClient.Lag(ctx, common.ListingIndexerV2ConsumerGroup)
	if err != nil {
		return err
	}
	lagResp.Each(func(l kadm.DescribedGroupLag) {
		fmt.Println("#")
		fmt.Println(l.Lag)
	})

	return nil
}
