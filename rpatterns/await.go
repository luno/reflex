package rpatterns

import (
	"context"
	"time"

	"github.com/luno/reflex"
)

// Await returns nil when a new event with foreignID and one of the eventTypes is
// received from the event streamer. It also returns nil if the pollFn
// returns true when it is periodically called. Cancel the input context to return
// early.
//
// This function can be used to wait for some state that is associated with an
// event. pollFn is used to periodically query the state, while this logic waits for
// a new event. It is done in parallel to mitigate race conditions.
func Await(
	in context.Context, stream reflex.StreamFunc, pollFn func() (bool, error),
	foreignID string, eventTypes ...reflex.EventType,
) error {
	// Ensure both go routines exit
	ctx, cancel := context.WithCancel(in)
	defer cancel()

	// Listen for new event
	listener := func() error {
		sc, err := stream(ctx, "", reflex.WithStreamFromHead())
		if err != nil {
			return err
		}

		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			e, err := sc.Recv()
			if err != nil {
				return err
			}
			if e.ForeignID != foreignID {
				continue
			}
			if reflex.IsAnyType(e.Type, eventTypes...) {
				return nil
			}
		}
	}

	// Poll external state periodically
	poller := func() error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			found, err := pollFn()
			if err != nil {
				return err
			} else if found {
				return nil
			}
			time.Sleep(time.Second)
		}
	}

	// Wait for either poller or listener
	var err error
	select {
	case err = <-goChan(func() error {
		return poller()
	}):
	case err = <-goChan(func() error {
		return listener()
	}):
	}

	return err
}

func goChan(f func() error) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- f() // will never block since buffered
		close(ch)
	}()
	return ch
}
