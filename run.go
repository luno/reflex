package reflex

import (
	"context"
	"io"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
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

	// Check if the consumer requires reset.
	if resetter, ok := s.consumer.(resetter); ok {
		err := resetter.Reset()
		if err != nil {
			return errors.Wrap(err, "reset error")
		}
	}

	sc, err := s.stream(ctx, cursor, s.opts...)
	if err != nil {
		return err
	}

	// Check if the stream client is a closer.
	if closer, ok := sc.(io.Closer); ok {
		defer closer.Close()
	}

	for {
		e, err := sc.Recv()
		if err != nil {
			return errors.Wrap(err, "recv error")
		}

		if err := s.consumer.Consume(ctx, fate.New(), e); err != nil {
			return errors.Wrap(err, "consume error")
		}

		if err := s.cstore.SetCursor(ctx, s.consumer.Name(), e.ID); err != nil {
			return errors.Wrap(err, "set cursor error")
		}
	}
}
