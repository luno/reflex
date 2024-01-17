package rpatterns

import (
	"context"

	"github.com/luno/reflex"
)

// AckEvent wraps a reflex event and provides an Ack method to
// update underlying consumer cursor.
type AckEvent struct {
	reflex.Event
	cStore       reflex.CursorStore
	consumerName string
}

// Ack sets (and flushes) the event id to the underlying cursor store.
// Note that out-of-order acks is allowed but should be avoided.
func (e *AckEvent) Ack(ctx context.Context) error {
	err := e.cStore.SetCursor(ctx, e.consumerName, e.ID)
	if err != nil {
		return err
	}
	return e.cStore.Flush(ctx)
}

type AckConsumerFunc func(context.Context, *AckEvent) error

// AckConsumer mirrors the reflex consumer except that events need to be acked
// explicitly. Ex. if processing batches, only the last event in the batch
// should be acked.
type AckConsumer struct {
	name    string
	consume AckConsumerFunc
	cStore  reflex.CursorStore
	opts    []reflex.ConsumerOption
}

// Name returns the ack consumer name.
func (c *AckConsumer) Name() string {
	return c.name
}

// Consume executes the consumer business logic, converting the reflex event
// to an AckEvent.
func (c *AckConsumer) Consume(ctx context.Context, e *reflex.Event) error {
	return c.consume(ctx, &AckEvent{
		Event:        *e,
		cStore:       c.cStore,
		consumerName: c.name,
	})
}

// NewAckConsumer returns a new AckConsumer.
func NewAckConsumer(name string, cStore reflex.CursorStore,
	consume AckConsumerFunc,
	opts ...reflex.ConsumerOption,
) *AckConsumer {
	return &AckConsumer{
		name:    name,
		cStore:  cStore,
		consume: consume,
		opts:    opts,
	}
}

// NewAckSpec returns a reflex spec for the AckConsumer.
func NewAckSpec(stream reflex.StreamFunc, ac *AckConsumer,
	opts ...reflex.StreamOption,
) reflex.Spec {
	c := reflex.NewConsumer(ac.name, ac.Consume, ac.opts...)
	return reflex.NewSpec(stream, &noSetStore{ac.cStore}, c, opts...)
}

type noSetStore struct {
	cstore reflex.CursorStore
}

func (s *noSetStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	return s.cstore.GetCursor(ctx, consumerName)
}

func (s *noSetStore) SetCursor(_ context.Context, _ string, _ string) error {
	// noop
	return nil
}

func (s *noSetStore) Flush(ctx context.Context) error {
	return s.cstore.Flush(ctx)
}
