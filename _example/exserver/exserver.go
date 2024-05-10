package exserver

import (
	"context"

	"github.com/luno/reflex"
)

// ExEvent is a reflex.Event
type ExEvent = reflex.Event

// ExEventType is the event type
type ExEventType int

// ReflexType returns the int
func (t ExEventType) ReflexType() int {
	return int(t)
}

const (
	// EventTypeUnknown is for unknown events
	EventTypeUnknown ExEventType = 0
	// EventTypeInsert is for events where we inserted a row
	EventTypeInsert ExEventType = 1
	// EventTypeUpdate is for events where we updated a row
	EventTypeUpdate ExEventType = 2
)

const (
	// ConsumerNameInternalLoop is the cursor name for the internal loop
	ConsumerNameInternalLoop = "internal_exserver_loop"
	// ConsumerNameInternalConsumer is the cursor name for the internal consumer
	ConsumerNameInternalConsumer = "internal_exclient_consumer"
	// ConsumerNameExternalConsumer is the cursor name for the external consumer
	ConsumerNameExternalConsumer = "external_exclient_consumer"
)

// Client is the interface for interacting with the example server
type Client interface {
	StreamEvents1(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
	StreamEvents2(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}
