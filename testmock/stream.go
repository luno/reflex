package testmock

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/errors"

	"github.com/luno/reflex"
)

func NewTestStreamer(t *testing.T) TestStreamer {
	t.Helper()
	var log []reflex.Event

	ctx, cancel := context.WithCancel(context.Background())
	return &testStreamerImpl{
		ctx:    ctx,
		cancel: cancel,
		mu:     &sync.Mutex{},
		log:    &log,
	}
}

type TestStreamer interface {
	InsertEvent(r reflex.Event)
	StreamFunc() reflex.StreamFunc
	Stop()
}

type testStreamerImpl struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     *sync.Mutex
	log    *[]reflex.Event
}

func (ts *testStreamerImpl) Stop() {
	ts.ctx.Done()
}

func (ts *testStreamerImpl) InsertEvent(r reflex.Event) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	*ts.log = append(*ts.log, r)
}

func (ts *testStreamerImpl) StreamFunc() reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		ts.mu.Lock()
		defer ts.mu.Unlock()

		var options reflex.StreamOptions
		for _, opt := range opts {
			opt(&options)
		}

		var offset int
		if after != "" {
			i, err := strconv.ParseInt(after, 10, 64)
			if err != nil {
				return nil, err
			}

			offset = int(i)
		}

		// When after is not set, aka no cursor provided, along with the option to stream from head, set the offset
		// to a zero index based value that represents the position of the last value, n, plus 1 ( n + 1 ) in the
		// append only log so that the next consumed event is a new event.
		if after == "" && options.StreamFromHead && len(*ts.log) > 0 {
			offset = len(*ts.log)
		}

		return &streamClientImpl{
			// Use parent context rather for Stop method
			ctx:     ts.ctx,
			mu:      ts.mu,
			log:     ts.log,
			offset:  offset,
			options: options,
		}, nil
	}
}

type streamClientImpl struct {
	ctx     context.Context
	mu      *sync.Mutex
	log     *[]reflex.Event
	offset  int
	options reflex.StreamOptions
}

func (s *streamClientImpl) Recv() (*reflex.Event, error) {
	for s.ctx.Err() == nil {
		s.mu.Lock()
		log := *s.log
		offset := s.offset
		s.offset += 1
		s.mu.Unlock()

		if len(log)-1 < offset {
			if s.options.StreamToHead {
				return nil, errors.Wrap(reflex.ErrHeadReached, "")
			}

			time.Sleep(time.Millisecond * 100)
			continue
		}

		return &log[offset], nil
	}

	return nil, s.ctx.Err()
}
