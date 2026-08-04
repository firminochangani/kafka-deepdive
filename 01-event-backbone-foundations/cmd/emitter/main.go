package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/labstack/echo/v5/middleware"
	"github.com/oklog/ulid/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/firminochangani/kafka-deepdive/01-event-backbone-foundations/internal/common"
)

func main() {
	err := run()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run() error {
	router := echo.New()

	router.Use(middleware.RequestLogger())

	kafkaClient, err := common.NewKafkaClient()
	if err != nil {
		return err
	}

	h := handlers{
		kafkaClient: kafkaClient,
	}
	router.POST("/listing", h.createListing)
	router.DELETE("/listing/:listingID", h.deleteListing)

	fmt.Println("emitter running on port 8085")
	return router.Start(":8085")
}

type handlers struct {
	kafkaClient *kgo.Client
}

type CreateListingResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

func (h *handlers) createListing(c *echo.Context) error {
	listingID := ulid.Make().String()
	msg := fmt.Sprintf(`{ "id": "%s" }`, listingID)

	record := kgo.Record{
		Key:       []byte(listingID),
		Value:     []byte(msg),
		Timestamp: time.Now(),
		Topic:     common.MarketplaceListingV1Topic,
		Headers:   NewHeaders("listing.created", "v1", "emitter-svc", ""),
	}

	resp, err := h.kafkaClient.ProduceSync(c.Request().Context(), &record).First()
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, CreateListingResponse{
		Partition: resp.Partition,
		Offset:    resp.Offset,
	})
}

func (h *handlers) deleteListing(c *echo.Context) error {
	listingID := c.Param("listingID")
	msg := fmt.Sprintf(`{ "id": "%s" }`, listingID)

	record := kgo.Record{
		Key:       []byte(listingID),
		Value:     []byte(msg),
		Timestamp: time.Now(),
		Topic:     common.MarketplaceListingV1Topic,
		Headers:   NewHeaders("listing.deleted", "v1", "emitter-svc", ""),
	}

	h.kafkaClient.Produce(context.Background(), &record, func(record *kgo.Record, err error) {
		if err != nil {
			fmt.Printf("Error publishing event 'listing.deleted' to topic '%s': %v\n", common.MarketplaceListingV1Topic, err)
			return
		}

		fmt.Printf("Event 'listing.deleted' successfully published to topic '%s'\n", common.MarketplaceListingV1Topic)
	})

	return c.NoContent(http.StatusOK)
}

func NewHeaders(event, version, service, correlationID string) []kgo.RecordHeader {
	return []kgo.RecordHeader{
		{Key: "event_name", Value: []byte(event)},
		{Key: "event_version", Value: []byte(version)},
		{Key: "correlation_id", Value: []byte(correlationID)},
		{Key: "service_name", Value: []byte(service)},
	}
}
