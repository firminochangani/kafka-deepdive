package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/firminochangani/kafka-deepdive/01-event-backbone-foundations/internal/common"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	fmt.Println("indexer is running")
	err := run(context.Background())
	if err != nil {
		fmt.Println("error running indexer-svc:", err)
		os.Exit(1)
	}

	fmt.Println("indexer exited gracefully")
}

type Indexer struct {
	lock *sync.RWMutex
	data map[string]*kgo.Record
}

func (i *Indexer) ApplyEvent(record *kgo.Record) {
	i.lock.Lock()
	defer i.lock.Unlock()

	i.data[string(record.Key)] = record
}

func run(ctx context.Context) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumeTopics(common.MarketplaceListingV1Topic),
		kgo.ConsumerGroup(common.ListingIndexerV2ConsumerGroup),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return err
	}

	go func() {
		<-ctx.Done()
		kafkaClient.Close()
		fmt.Println("Closed kafka client")
	}()

	indexer := &Indexer{
		data: make(map[string]*kgo.Record),
		lock: &sync.RWMutex{},
	}

	fmt.Println("Consuming events")
	for {
		fmt.Println("Polling fetches")
		fetches := kafkaClient.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			if errors.Is(errs[0].Err, context.Canceled) {
				break
			}

			err = fmt.Errorf("error consuming messages from kafka: %v", errs)
			break
		}

		fmt.Println("Iterating over records")
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			indexer.ApplyEvent(record)
			fmt.Printf("New event applied to the indexer: %s\n", string(record.Value))
		}

		err = kafkaClient.CommitUncommittedOffsets(ctx)
		if err != nil {
			fmt.Println("Error committing offsets")
			break
		}
	}

	tCtx, tCancel := context.WithTimeout(context.Background(), time.Second*5)
	defer tCancel()

	exitErr := kafkaClient.LeaveGroupContext(tCtx)
	if exitErr != nil {
		return errors.Join(exitErr, err)
	}
	fmt.Printf("\nLeft group consumer gracefully\n")

	if err != nil {
		return err
	}

	return nil
}
