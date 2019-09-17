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

// ConsumeForever continuously runs the consume function, backing off
// and logging on unexpected errors.
func ConsumeForever(getCtx func() context.Context, consume reflex.ConsumeFunc,
	consumer reflex.Consumer, opts ...reflex.StreamOption) {
	for {
		ctx := getCtx()

		err := consume(ctx, consumer, opts...)
		if errors.IsAny(err, context.Canceled, reflex.ErrStopped, fate.ErrTempt) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "consume forever error"),
			jettison.WithKeyValueString("consumer", consumer.Name().String()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}
