package common

import "github.com/twmb/franz-go/pkg/kgo"

const (
	ListingIndexerV2ConsumerGroup = "listing-indexer-v1"
	MarketplaceListingV1Topic     = "marketplace.listings.v1"
)

func NewKafkaClient() (*kgo.Client, error) {
	return kgo.NewClient(
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumerGroup(ListingIndexerV2ConsumerGroup),
	)
}
