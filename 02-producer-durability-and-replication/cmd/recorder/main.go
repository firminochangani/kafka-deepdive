package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	"kafka-deepdive/02-producer-durability-and-replication/internal/common"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	if err := run(ctx); err != nil {
		panic(err)
	}
}

func run(ctx context.Context) error {
	durabilityProfile := flag.String(
		"durability-profile",
		"fire-and-forget",
		"Options: fire-and-forget, leader-ack, safe",
	)
	flag.Parse()

	options := []kgo.Opt{
		kgo.SeedBrokers(common.SeedBrokers...),
		kgo.DisableIdempotentWrite(),
	}

	switch *durabilityProfile {
	case "fire-and-forget":
		options = append(options, kgo.RequiredAcks(kgo.NoAck()))
	case "leader-ack":
		options = append(options, kgo.RequiredAcks(kgo.LeaderAck()))
	case "safe":
		options = append(options, kgo.RequiredAcks(kgo.AllISRAcks()))
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return err
	}

	fmt.Printf("recorder started with durability-profile set to '%s'\n", *durabilityProfile)
	fmt.Println("publishing messages...")
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Context cancelled. Exiting.")
			return nil
		default:
			err = publish(ctx, client)
			if err != nil {
				return err
			}

			time.Sleep(time.Millisecond * 250)
		}
	}
}

/*var partitionKeys = []string{
	"99271e8d-004e-4286-bee1-29ef2bb2c9d6",
	"c5fb60b6-c548-4b5d-ab4a-c5f57b63379a",
	"3bc6f6ff-5d81-4d6e-ae31-5207701387e8",
	"b827e751-53cd-48eb-96a0-2cad67d1969f",
}
partitionKey := partitionKeys[gofakeit.Number(0, 3)]*/

func publish(ctx context.Context, client *kgo.Client) error {
	events, paymentID := generatePaymentEvents()

	for _, event := range events {
		err := client.ProduceSync(ctx, &kgo.Record{
			Key:   []byte(paymentID),
			Value: event,
			Topic: common.TopicMarketplacePaymentsV1,
		}).FirstErr()
		if err != nil {
			return err
		}

		fmt.Printf("message published successfully: %s\n", string(event))
	}

	return nil
}

func generatePaymentEvents() ([][]byte, string) {
	var events [][]byte
	paymentID := gofakeit.UUID()

	paymentCaptured := common.PaymentCapturedV1{
		PaymentID:  paymentID,
		Version:    1,
		OccurredAt: time.Now(),
		EventName:  "PaymentCapturedV1",
	}
	events = append(events, mustMarshall(paymentCaptured))

	shouldFail := gofakeit.Bool()
	if shouldFail {
		events = append(events, mustMarshall(common.PaymentFailedV1{
			PaymentID:  paymentID,
			Version:    2,
			OccurredAt: time.Now(),
			EventName:  "PaymentFailedV1",
		}))

		return events, paymentID
	}

	events = append(events, mustMarshall(common.PaymentAuthorizedV1{
		PaymentID:  paymentID,
		Version:    2,
		OccurredAt: time.Now(),
		EventName:  "PaymentAuthorizedV1",
	}))

	shouldRefund := gofakeit.Bool()
	if shouldRefund {
		events = append(events, mustMarshall(common.PaymentRefundedV1{
			PaymentID:  paymentID,
			Version:    3,
			OccurredAt: time.Now(),
			EventName:  "PaymentRefundedV1",
		}))
	}

	return events, paymentID
}

func mustMarshall[T any](event T) []byte {
	r, err := json.Marshal(event)
	if err != nil {
		panic(err)
	}

	return r
}
