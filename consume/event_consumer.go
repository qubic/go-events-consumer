package consume

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type ElasticEventClient interface {
	IndexEvent(data []byte) error
}

type EventConsumer struct {
	eventClient   *kgo.Client
	elasticClient ElasticEventClient
	metrics       *Metrics
	currentTick   uint32
}

type Event struct {
	Id              string `json:"_id"`
	Epoch           uint32 `json:"epoch"`
	Tick            uint32 `json:"tick"`
	EventId         uint64 `json:"eventId"`
	EventDigest     uint64 `json:"eventDigest"`
	TransactionHash string `json:"transactionHash"`
	EventType       uint32 `json:"eventType"`
	EventSize       uint32 `json:"eventSize"`
	EventData       string `json:"eventData"`
}

func NewEventConsumer(client *kgo.Client, elasticClient ElasticEventClient, metrics *Metrics) *EventConsumer {

	return &EventConsumer{
		eventClient:   client,
		metrics:       metrics,
		elasticClient: elasticClient,
	}
}

func (c *EventConsumer) Consume() {
	for {
		count, err := c.ConsumeEvents()
		if err == nil {
			log.Printf("Successfully processed [%d] events...", count)
		} else {
			log.Printf("Error consuming events: %v", err)
		}
		time.Sleep(time.Second)
	}
}

func (c *EventConsumer) ConsumeEvents() (int, error) {
	ctx := context.Background()
	fetches := c.eventClient.PollRecords(ctx, 100) // batch process max 100 events in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retryable errors are
		// returned from polls so that users can notice and take action.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return -1, errors.New("Error fetching records")
	}

	var processed int

	// We can iterate through a record iterator...
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		var event Event
		err := json.Unmarshal(record.Value, &event)
		if err != nil {
			return -1, errors.Wrap(err, "failed to unmarshal event")
		}

		err = c.elasticClient.IndexEvent(record.Value)
		if err != nil {
			return -1, err
		}

		// within one partition order within ticks (key) is guaranteed. Tick order is not guaranteed, but we assume
		// that messages are in order here. Worst case we have some minor metric deviations.
		if event.Tick > c.currentTick {
			c.currentTick = event.Tick
			c.metrics.IncProcessedTicks()
			c.metrics.SetProcessedTick(event.Epoch, event.Tick)
		}

		// events should be ordered by tick (not 100% but close enough, as order is only guaranteed within one tick)
		c.metrics.IncProcessedMessages()
		processed++
		// log.Printf("event: %+v", event)
	}
	err := c.eventClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "Error committing offsets")
	}
	return processed, nil
}
