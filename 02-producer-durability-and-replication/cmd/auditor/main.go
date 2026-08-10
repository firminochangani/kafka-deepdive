package main

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"

	"kafka-deepdive/02-producer-durability-and-replication/internal/common"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	err := run(ctx)
	if err != nil {
		panic(err)
	}
}

func run(ctx context.Context) error {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(common.SeedBrokers...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeTopics(common.TopicMarketplacePaymentsV1),
	)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			fetches := client.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				if errors.Is(errs[0].Err, context.Canceled) {
					break
				}

				err = fmt.Errorf("error consuming messages from kafka: %v", errs)
				break
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				record := iter.Next()
				fmt.Printf("New event consumed. partition: '%d' event: %s\n", record.Partition, string(record.Value))
			}
		}
	}
}
