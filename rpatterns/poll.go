package rpatterns

import (
	"context"
	"testing"
	"time"

	"github.com/luno/reflex"
)

// NewPoller returns a new poller for the given poll function.
func NewPoller(pollFunc PollFunc, opts ...func(*Poller)) Poller {
	p := Poller{
		pollFunc: pollFunc,
		backoff:  time.Minute,
		sleep:    time.After,
	}

	for _, opt := range opts {
		opt(&p)
	}

	return p
}

// PollFunc returns subsequent events after the cursor or an error. It
// should return an empty list if no subsequent events are available. It may
// support stream options.
type PollFunc func(ctx context.Context, after string, opts ...reflex.StreamOption) ([]reflex.Event, error)

// Poller is an adapter that provides a reflex stream API on top of a polling
// (or pagination) API; so pull to push.
//
// The polling API needs to provide consistent and stable ordering of events
// and reliable event IDs that are used by reflex as the cursor.
//
// This is useful if one needs to continuously sync data from a polling API.
type Poller struct {
	pollFunc PollFunc
	backoff  time.Duration
	sleep    func(d time.Duration) <-chan time.Time
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from the underlying polling API after the provided cursor.
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
func (p Poller) Stream(ctx context.Context, after string,
	opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	return &pollStream{
		ctx:      ctx,
		cursor:   after,
		opts:     opts,
		pollFunc: p.pollFunc,
		backoff:  p.backoff,
		sleep:    p.sleep,
	}, nil
}

type pollStream struct {
	ctx      context.Context
	opts     []reflex.StreamOption
	pollFunc PollFunc
	backoff  time.Duration
	sleep    func(d time.Duration) <-chan time.Time

	cursor string
	buf    []reflex.Event
}

func (s *pollStream) Recv() (*reflex.Event, error) {
	if len(s.buf) == 0 {
		err := s.pollNext()
		if err != nil {
			return nil, err
		}
	}

	pop := s.buf[0]
	s.buf = s.buf[1:]
	s.cursor = pop.ID
	return &pop, nil
}

// pollNext blocks until a subsequent slice of events are available then
// loads the buffer.
func (s *pollStream) pollNext() error {
	for {
		el, err := s.pollFunc(s.ctx, s.cursor, s.opts...)
		if err != nil {
			return err
		}

		if len(el) > 0 {
			s.buf = el
			return nil
		}

		select {
		case <-s.sleep(s.backoff):
		case <-s.ctx.Done():
			return s.ctx.Err()
		}
	}
}

// WithPollBackoff returns a option to configure the
// polling backoff (period) if no new events are available.
func WithPollBackoff(d time.Duration) func(*Poller) {
	return func(p *Poller) {
		p.backoff = d
	}
}

// WithSleep returns a option to configure the
// sleep function for testing.
func WithSleep(_ *testing.T, fn func(d time.Duration) <-chan time.Time) func(*Poller) {
	return func(p *Poller) {
		p.sleep = fn
	}
}
