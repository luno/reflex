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
		ctx = log.ContextWith(ctx, j.KS("consumer", req.Name()))

		err := reflex.Run(ctx, req)
		if reflex.IsExpected(err) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			_ = req.Stop()
			continue
		}

		log.Error(ctx, errors.Wrap(err, "run forever error"))
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute): // 1 min backoff on errors
		}
		_ = req.Stop()
	}
}

// ConsumeForever continuously runs the consume function, backing off
// and logging on unexpected errors.
// Deprecated: Please use RunForever.
func ConsumeForever(getCtx func() context.Context, consume reflex.ConsumeFunc,
	consumer reflex.Consumer, opts ...reflex.StreamOption) {

	stop := func() error { return nil }
	if s, ok := consumer.(reflex.Stopper); ok {
		stop = s.Stop
	}

	for {
		ctx := getCtx()
		ctx = log.ContextWith(ctx, j.KS("consumer", consumer.Name()))

		err := consume(ctx, consumer, opts...)
		if errors.IsAny(err, context.Canceled, reflex.ErrStopped, fate.ErrTempt) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			_ = stop()
			continue
		}

		log.Error(ctx, errors.Wrap(err, "consume forever error"))
		select {
		case <-ctx.Done():
		case <-time.After(time.Minute): // 1 min backoff on errors
		}
		_ = stop()
	}
}
