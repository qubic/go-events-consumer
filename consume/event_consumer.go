package consume

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type EventConsumer struct {
	eventClient *kgo.Client
}

type Event struct {
	Id              string
	Epoch           uint32
	Tick            uint32
	EventId         uint64
	EventDigest     uint64
	TransactionHash string
	EventType       uint32
	EventSize       uint32
	EventData       string
}

func NewEventConsumer(client *kgo.Client) *EventConsumer {
	return &EventConsumer{
		eventClient: client,
	}
}

func (c *EventConsumer) Consume() {

	for {
		err := c.ConsumeEvents()
		if err != nil {
			log.Printf("Error consuming events: %v", err)
		}
		time.Sleep(time.Second)
	}

}

func (c *EventConsumer) ConsumeEvents() error {
	ctx := context.Background()
	fetches := c.eventClient.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retryable errors are
		// returned from polls so that users can notice and take action.
		panic(fmt.Sprint(errs)) // TODO error handling
	}

	// We can iterate through a record iterator...
	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		var event Event
		err := json.Unmarshal(record.Value, &event)
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal event")
		}
		log.Printf("event: %+v", event)
	}

	c.eventClient.MarkCommitRecords()

	return nil
}
