package rpatterns

import (
	"context"
	"time"

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
