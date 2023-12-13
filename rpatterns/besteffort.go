package rpatterns

import (
	"context"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// NewBestEffortConsumer returns a reflex consumer that ignores errors
// after the provided number of retries and therefore eventually
// continues to the next event.
func NewBestEffortConsumer(name string, retries int, fn func(context.Context, *reflex.Event) error,
	opts ...reflex.ConsumerOption,
) reflex.Consumer {
	be := &bestEffort{
		inner:   fn,
		retries: retries,
	}

	return reflex.NewConsumer(name, be.consume, opts...)
}

type bestEffort struct {
	inner      func(context.Context, *reflex.Event) error
	retries    int
	name       string
	retryID    string
	retryCount int
}

func (b *bestEffort) consume(ctx context.Context, e *reflex.Event) error {
	err := b.inner(ctx, e)
	if err != nil {
		if b.retryID != e.ID {
			b.retryCount = 0
		}

		b.retryID = e.ID
		b.retryCount++

		if b.retryCount > b.retries {
			b.retryCount = 0
			b.retryID = ""
			if !reflex.IsExpected(err) {
				log.Error(ctx, errors.Wrap(err, "best effort consumer ignoring error"),
					j.KS("consumer", b.name))
			}
			return nil
		}

		return err
	}

	b.retryCount = 0
	b.retryID = ""
	return nil
}
