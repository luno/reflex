package reflex

import (
	"context"
	"io"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
)

// Run executes the spec by streaming events from the current cursor,
// feeding each into the consumer and updating the cursor on success.
// It always returns a non-nil error. Cancel the context to return early.
func Run(in context.Context, s Spec) error {
	ctx, cancel := context.WithCancel(in)
	defer cancel()
	defer s.cstore.Flush(context.Background()) // best effort flush with new context

	cursor, err := s.cstore.GetCursor(ctx, s.consumer.Name())
	if err != nil {
		return errors.Wrap(err, "get cursor error")
	}

	// Check if the consumer requires to be reset.
	switch r := s.consumer.(type) {
	case ResetterCtx:
		err = r.Reset(ctx)
		if err != nil {
			return errors.Wrap(err, "reset error")
		}
	case resetter:
		err = r.Reset()
		if err != nil {
			return errors.Wrap(err, "reset error")
		}
	}

	// Filter out stream lag option since we implement lag here not at server.
	var (
		lag  time.Duration
		opts []StreamOption
	)
	for _, opt := range s.opts {
		var temp StreamOptions
		opt(&temp)
		if temp.Lag > 0 {
			lag = temp.Lag
		} else {
			opts = append(opts, opt)
		}
	}

	// Start stream
	sc, err := s.stream(ctx, cursor, opts...)
	if err != nil {
		return err
	}

	// Check if the stream client is a closer.
	if closer, ok := sc.(io.Closer); ok {
		defer closer.Close()
	}

	f := fate.New()

	for {
		e, err := sc.Recv()
		if err != nil {
			return errors.Wrap(err, "recv error")
		}

		ctx := log.ContextWith(ctx, j.MKS{
			"event_id":  e.ID,
			"event_fid": e.ForeignID,
		})

		// Delay events if lag specified.
		if delay := lag - since(e.Timestamp); lag > 0 && delay > 0 {
			t := newTimer(delay)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}

		if err := s.consumer.Consume(ctx, f, e); err != nil {
			return errors.Wrap(err, "consume error", j.MKS{
				"event_id":  e.ID,
				"event_fid": e.ForeignID,
			})
		}

		if err := s.cstore.SetCursor(ctx, s.consumer.Name(), e.ID); err != nil {
			return errors.Wrap(err, "set cursor error", j.MKS{
				"event_id":  e.ID,
				"event_fid": e.ForeignID,
			})
		}
	}
}

// newTimer is aliased for testing.
var newTimer = time.NewTimer

// since is aliased for testing.
var since = time.Since
