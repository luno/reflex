package rpatterns

import (
	"context"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
)

// AckEvent wraps a reflex event and provides an Ack method to
// update underlying consumer cursor.
type AckEvent struct {
	reflex.Event
	cstore       reflex.CursorStore
	consumerName reflex.ConsumerName
}

// Ack sets (and flushes) the event id to the underlying cursor store.
// Note that out-of-order acks is allowed but should be avoided.
func (e *AckEvent) Ack(ctx context.Context) error {
	err := e.cstore.SetCursor(ctx, e.consumerName.String(), e.ID)
	if err != nil {
		return err
	}
	return e.cstore.Flush(ctx)
}

// AckConsumer mirrors the reflex consumer except that events need to be acked
// explicitly. Ex. if processing batches, only the last event in the batch
// should be acked.
type AckConsumer interface {
	Name() reflex.ConsumerName
	Consume(context.Context, fate.Fate, *AckEvent) error
}

// AckConsumeFunc blocks and streams events to the AckConsumer, only updating the
// cursor when events are acked. It always returns a non-nil error.
type AckConsumeFunc func(context.Context, AckConsumer, ...reflex.StreamOption) error

// NewAckConsume returns a consume function for a AckConsumer which requires
// explicit acks to update the cursor store. This is useful if events are processed
// in batches.
func NewAckConsume(stream reflex.StreamFunc, cstore reflex.CursorStore,
	opts1 ...reflex.StreamOption) AckConsumeFunc {
	return func(ctx context.Context, ackConsumer AckConsumer, opts2 ...reflex.StreamOption) error {
		adapter := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
			ackEvent := &AckEvent{
				Event:        *e,
				cstore:       cstore,
				consumerName: ackConsumer.Name(),
			}
			return ackConsumer.Consume(ctx, f, ackEvent)
		}
		consumer := reflex.NewConsumer(ackConsumer.Name(), adapter)
		consumable := reflex.NewConsumable(stream, &noSetStore{cstore: cstore}, opts1...)
		return consumable.Consume(ctx, consumer, opts2...)
	}
}

type ackConsumer struct {
	name    reflex.ConsumerName
	consume func(context.Context, fate.Fate, *AckEvent) error
}

func (c *ackConsumer) Name() reflex.ConsumerName {
	return c.name
}

func (c *ackConsumer) Consume(ctx context.Context, f fate.Fate, e *AckEvent) error {
	return c.consume(ctx, f, e)
}

// NewAckConsumer returns a new AckConsumer.
func NewAckConsumer(name reflex.ConsumerName,
	consume func(context.Context, fate.Fate, *AckEvent) error) AckConsumer {
	return &ackConsumer{
		name:    name,
		consume: consume,
	}
}

type noSetStore struct {
	cstore reflex.CursorStore
}

func (s *noSetStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	return s.cstore.GetCursor(ctx, consumerName)
}

func (s *noSetStore) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	// noop
	return nil
}

func (s *noSetStore) Flush(ctx context.Context) error {
	return s.cstore.Flush(ctx)
}

// AckConsumeForever continuously runs the ack consume function, backing off
// and logging on unexpected errors.
func AckConsumeForever(getCtx func() context.Context, consume AckConsumeFunc,
	consumer AckConsumer, opts ...reflex.StreamOption) {
	for {
		ctx := getCtx()

		err := consume(ctx, consumer, opts...)
		if errors.Is(err, context.Canceled) || reflex.IsStoppedErr(err) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "ack consume forever error"),
			jettison.WithKeyValueString("consumer", consumer.Name().String()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}
