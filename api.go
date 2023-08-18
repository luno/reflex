package reflex

import (
	"context"
	"strconv"
	"time"

	"github.com/luno/fate"
)

// Event is the reflex event. It is an immutable notification event that indicates that
// a change of a some type relating a foreign entity happened at a specific time. It may also
// contain metadata relating to the change.
type Event struct {
	ID        string
	Type      EventType
	ForeignID string
	Timestamp time.Time
	MetaData  []byte
	Trace     []byte
}

// IDInt returns the event id as an int64 or 0 if it is not an integer.
func (e *Event) IDInt() int64 {
	i, _ := strconv.ParseInt(e.ID, 10, 64)
	return i
}

// IsIDInt returns true if the event id is an integer.
func (e *Event) IsIDInt() bool {
	_, err := strconv.ParseInt(e.ID, 10, 64)
	return err == nil
}

// ForeignIDInt returns the foreign id as an int64 or 0 if it is not an integer.
func (e *Event) ForeignIDInt() int64 {
	i, _ := strconv.ParseInt(e.ForeignID, 10, 64)
	return i
}

// IsForeignIDInt returns true if the foreign id is an integer.
func (e *Event) IsForeignIDInt() bool {
	_, err := strconv.ParseInt(e.ForeignID, 10, 64)
	return err == nil
}

// EventType is an interface for enums that act as reflex event types.
type EventType interface {
	// ReflexType returns the type as an int.
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

// Spec specifies all the elements required to stream and consume reflex events
// for a specific purpose. StreamFunc is the source of the events. Consumer is
// the business logic consuming the events. CursorStore persists a cursor of
// consumed events. As long as the elements do not change the consumer is
// guaranteed at-least-once delivery of all events in the stream.
type Spec struct {
	stream   StreamFunc
	cstore   CursorStore
	consumer Consumer
	opts     []StreamOption
}

// Name returns the name of the spec which is the name of the consumer.
func (req Spec) Name() string {
	return req.consumer.Name()
}

// Stop stops the spec's consumer.
func (req Spec) Stop() error {
	if s, ok := req.consumer.(Stopper); ok {
		return s.Stop()
	}
	return nil
}

// NewSpec returns a new Spec.
func NewSpec(stream StreamFunc, cstore CursorStore, consumer Consumer,
	opts ...StreamOption,
) Spec {
	return Spec{
		stream:   stream,
		cstore:   cstore,
		consumer: consumer,
		opts:     opts,
	}
}

// Consumer represents a piece of business logic that consumes events.
// It consists of a name and the consume logic. Consumer logic should be idempotent
// since reflex provides at-least-once event delivery.
type Consumer interface {
	Name() string
	Consume(context.Context, fate.Fate, *Event) error
}

// resetter is an optional interface that a consumer can implement indicating
// that it is stateful and requires reset at the start of each Run.
type resetter interface {
	Reset() error
}

// ResetterCtx is an optional interface that a consumer can implement indicating
// that it is stateful and requires reset at the start of each Run.
type ResetterCtx interface {
	Reset(context.Context) error
}

// Stopper is an optional interface that a consumer can implement indicating
// that it has clean up work to do at the end of each Run.
type Stopper interface {
	Stop() error
}

// StreamClient is a stream interface providing subsequent events on calls to Recv.
type StreamClient interface {
	// Recv blocks until the next event is found. Either the event or error is non-nil.
	Recv() (*Event, error)

	// TODO(corver): Think about adding io.Closer explicitly
	// to this interface rather than current optional checks.
}

// StreamFunc is the main reflex stream interface that all implementations should provide.
// It returns a long lived StreamClient that will stream events from the source.
type StreamFunc func(ctx context.Context, after string, opts ...StreamOption) (StreamClient, error)

// ConsumeFunc is the main reflex consume interface. It blocks while events are
// streamed to consumer. It always returns a non-nil error. Cancel the context
// to return early.
// Deprecated: Please use Spec.
type ConsumeFunc func(context.Context, Consumer, ...StreamOption) error

// Consumable is an interface for an object that provides a ConsumeFunc with the name Run.
// Deprecated: Please use Spec.
type Consumable interface {
	// Run blocks while events are streamed to consumer. It always returns a non-nil error.
	// Cancel the context to return early.
	Consume(context.Context, Consumer, ...StreamOption) error
}

// CursorStore is an interface used to persist consumer offsets in a stream.
type CursorStore interface {
	// GetCursor returns the consumers cursor, it returns an empty string if no cursor exists.
	GetCursor(ctx context.Context, consumerName string) (string, error)

	// SetCursor stores the consumers cursor. Note some implementation may buffer writes.
	SetCursor(ctx context.Context, consumerName string, cursor string) error

	// Flush writes any buffered cursors to the underlying store.
	Flush(ctx context.Context) error
}
