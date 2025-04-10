package consume

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEvent_MarshalAndUnmarshal(t *testing.T) {
	event := &Event{
		Epoch:           153,
		Tick:            21679416,
		EventId:         13857,
		EventDigest:     1715952909454684526,
		TransactionHash: "wjydyydyoltqlfdvnldtqqargoiamutsfqjnojyjhemhbrckrvxeyjodnfil",
		EventType:       0,
		EventSize:       72,
		EventData:       "jXeSxIWWmtt45R7OZEdfBsCYwW27zUuCrIeQ/Y6ajDRKJ8b/lXtAmxLVMPI71cgnSdOdbDKXB6mJVUSbkG2ntgEAAAAAAAAA",
	}

	expectedJson := `{"epoch":153,"tick":21679416,"eventId":13857,"eventDigest":1715952909454684526,"transactionHash":"wjydyydyoltqlfdvnldtqqargoiamutsfqjnojyjhemhbrckrvxeyjodnfil","eventType":0,"eventSize":72,"eventData":"jXeSxIWWmtt45R7OZEdfBsCYwW27zUuCrIeQ/Y6ajDRKJ8b/lXtAmxLVMPI71cgnSdOdbDKXB6mJVUSbkG2ntgEAAAAAAAAA"}`
	marshalled, err := json.Marshal(event)
	assert.NoError(t, err)
	assert.Equal(t, expectedJson, string(marshalled))

	var unmarshalled Event
	err = json.Unmarshal(marshalled, &unmarshalled)
	assert.NoError(t, err)
	assert.Equal(t, event, &unmarshalled)
}
