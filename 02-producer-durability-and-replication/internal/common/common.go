package common

import "time"

const (
	TopicMarketplacePaymentsV1 = "marketplace.payments.v1"
	TopicBenchThroughputV1     = "bench.throughput.v1"
	TopicBenchRiskyV1          = "bench.risky.v1"
)

var SeedBrokers = []string{"localhost:9092", "localhost:9093", "localhost:9094"}

type PaymentAuthorizedV1 struct {
	PaymentID  string    `json:"payment_id"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	EventName  string    `json:"event_name"`
}

type PaymentCapturedV1 struct {
	PaymentID  string    `json:"payment_id"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	EventName  string    `json:"event_name"`
}

type PaymentRefundedV1 struct {
	PaymentID  string    `json:"payment_id"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	EventName  string    `json:"event_name"`
}

type PaymentFailedV1 struct {
	PaymentID  string    `json:"payment_id"`
	Version    int       `json:"version"`
	OccurredAt time.Time `json:"occurred_at"`
	EventName  string    `json:"event_name"`
}
