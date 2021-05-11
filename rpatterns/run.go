package rpatterns

import (
	"context"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// RunForever continuously calls the run function, backing off
// and logging on unexpected errors.
func RunForever(getCtx func() context.Context, req reflex.Spec) {
	for {
		ctx := getCtx()

		err := reflex.Run(ctx, req)
		if reflex.IsExpected(err) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "run forever error"),
			j.KS("consumer", req.Name()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}

// ConsumeForever continuously runs the consume function, backing off
// and logging on unexpected errors.
// Deprecated: Please use RunForever.
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
			j.KS("consumer", consumer.Name()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}
