package reflex

import (
	"context"
	"strconv"
	"time"

	"github.com/luno/fate"
)

type EventType interface {
	ReflexType() int
}

// IsType returns true if the source reflex type equals the target type.
func IsType(source, target EventType) bool {
	return source.ReflexType() == target.ReflexType()
}

// IsAnyType returns true if the source reflex type equals any of the target types.
func IsAnyType(source EventType, targets ...EventType) bool {
	for _, target := range targets {
		if source.ReflexType() == target.ReflexType() {
			return true
		}
	}
	return false
}

// eventType is the internal implementation of EventType interface.
type eventType int

func (t eventType) ReflexType() int {
	return int(t)
}

type ConsumerName string

func (c ConsumerName) String() string {
	return string(c)
}

type Event struct {
	ID        string
	Type      EventType
	ForeignID string
	Timestamp time.Time
	MetaData  []byte
}

func (e *Event) IDInt() int64 {
	i, _ := strconv.ParseInt(e.ID, 10, 64)
	return i
}

func (e *Event) IsIDInt() bool {
	_, err := strconv.ParseInt(e.ID, 10, 64)
	return err == nil
}

func (e *Event) ForeignIDInt() int64 {
	i, _ := strconv.ParseInt(e.ForeignID, 10, 64)
	return i
}

func (e *Event) IsForeignIDInt() bool {
	_, err := strconv.ParseInt(e.ForeignID, 10, 64)
	return err == nil
}

type Consumer interface {
	Name() ConsumerName
	Consume(context.Context, fate.Fate, *Event) error
}

// StreamClient is a stream interface providing subsequent events on calls to Recv.
type StreamClient interface {
	// Recv blocks until the next event is found. Either the event or error is non-nil.
	Recv() (*Event, error)
}

// StreamFunc is the main reflex stream interface that all implementations should provide.
// It returns a long lived StreamClient that will stream events from the source.
type StreamFunc func(ctx context.Context, after string, opts ...StreamOption) (StreamClient, error)

// ConsumeFunc is the main reflex consume interface. It blocks while events are
// streamed to consumer. It always returns a non-nil error. Cancel the context
// to return early.
type ConsumeFunc func(context.Context, Consumer, ...StreamOption) error

// Consumable is an interface for an object that provides a ConsumeFunc with the name Consume.
type Consumable interface {
	// Consume blocks while events are streamed to consumer. It always returns a non-nil error.
	// Cancel the context to return early.
	Consume(context.Context, Consumer, ...StreamOption) error
}

type CursorStore interface {
	GetCursor(ctx context.Context, consumerName string) (string, error)
	SetCursor(ctx context.Context, consumerName string, cursor string) error
	Flush(ctx context.Context) error
}
