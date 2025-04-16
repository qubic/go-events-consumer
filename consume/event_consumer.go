package consume

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/qubic/go-qubic/common"
	"github.com/twmb/franz-go/pkg/kgo"
	"log"
	"time"
)

type EventConsumer struct {
	eventClient   *kgo.Client
	elasticClient ElasticEventClient
	metrics       *Metrics
	currentTick   uint32
	currentEpoch  uint32
}

type Event struct {
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

func (c *EventConsumer) Consume() error {
	for {
		count, err := c.ConsumeEvents()
		if err == nil {
			log.Printf("Processed [%d] events. Latest tick: [%d]", count, c.currentTick)
		} else {
			// if there is an error consuming we abort. We need to fix the error before trying again.
			log.Fatalf("Error consuming events: %v", err) // exits
			// TODO return error
		}
		time.Sleep(time.Second)
	}
}

func (c *EventConsumer) ConsumeEvents() (int, error) {
	ctx := context.Background()
	fetches := c.eventClient.PollRecords(ctx, 1000) // batch process max 100 events in one run
	if errs := fetches.Errors(); len(errs) > 0 {
		// All errors are retried internally when fetching, but non-retryable errors are
		// returned from polls so that users can notice and take action.
		for _, err := range errs {
			log.Printf("Error: %v", err)
		}
		return -1, errors.New("Error fetching records")
	}

	var documents []EsDocument
	// We can iterate through a record iterator...
	iter := fetches.RecordIter()
	for !iter.Done() {

		record := iter.Next()

		var event Event
		err := json.Unmarshal(record.Value, &event)
		if err != nil {
			return -1, errors.Wrapf(err, "Error unmarshalling event %s", string(record.Value))
		}

		documentId, err := createUniqueId(&event)
		if err != nil {
			return -1, errors.Wrapf(err, "Error creating id for event: %s", string(record.Value))
		}

		documents = append(documents, EsDocument{
			id:      documentId,
			payload: record.Value,
		})

		// events should be ordered by tick (not 100% but close enough, as order is only guaranteed within one tick)
		if event.Tick > c.currentTick {
			c.currentTick = event.Tick
			c.currentEpoch = event.Epoch
			c.metrics.IncProcessedTicks()

		}
		c.metrics.IncProcessedMessages()
	}

	err := c.elasticClient.BulkIndexEvents(ctx, documents)
	if err != nil {
		return -1, errors.Wrapf(err, "Error bulk indexing [%d] documents.", len(documents))
	}
	c.metrics.SetProcessedTick(c.currentEpoch, c.currentTick)

	err = c.eventClient.CommitUncommittedOffsets(ctx)
	if err != nil {
		return -1, errors.Wrap(err, "Error committing offsets")
	}
	return len(documents), nil
}

func createUniqueId(event *Event) (string, error) {
	var buff bytes.Buffer
	err := binary.Write(&buff, binary.LittleEndian, event.Epoch)
	if err != nil {
		return "", errors.Wrap(err, "writing epoch to buffer")
	}
	err = binary.Write(&buff, binary.LittleEndian, event.Tick)
	if err != nil {
		return "", errors.Wrap(err, "writing tick to buffer")
	}
	err = binary.Write(&buff, binary.LittleEndian, event.EventId)
	if err != nil {
		return "", errors.Wrap(err, "writing event id to buffer")
	}
	err = binary.Write(&buff, binary.LittleEndian, event.EventDigest)
	if err != nil {
		return "", errors.Wrap(err, "writing event digest to buffer")
	}
	_, err = buff.Write([]byte(event.TransactionHash))
	if err != nil {
		return "", errors.Wrap(err, "writing transaction hash to buffer")
	}
	hash, err := common.K12Hash(buff.Bytes())
	if err != nil {
		return "", errors.Wrap(err, "failed to hash event")
	}
	return hex.EncodeToString(hash[:]), err
}
