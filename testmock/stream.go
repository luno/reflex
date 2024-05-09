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
	t.Cleanup(cancel)
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
	ts.cancel()
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

		var cursor cursorStore
		cursor.Set(offset)

		return &streamClientImpl{
			ctx:       ctx,
			parentCtx: ts.ctx,
			mu:        ts.mu,
			log:       ts.log,
			cursor:    &cursor,
			options:   options,
		}, nil
	}
}

type streamClientImpl struct {
	ctx       context.Context
	parentCtx context.Context
	mu        *sync.Mutex
	log       *[]reflex.Event
	cursor    *cursorStore
	options   reflex.StreamOptions
}

func (s *streamClientImpl) Recv() (*reflex.Event, error) {
	for s.ctx.Err() == nil && s.parentCtx.Err() == nil {
		s.mu.Lock()
		log := *s.log
		s.mu.Unlock()

		offset := s.cursor.Get()
		if len(log)-1 < offset {
			if s.options.StreamToHead {
				return nil, errors.Wrap(reflex.ErrHeadReached, "")
			}

			time.Sleep(time.Millisecond * 100)
			continue
		}

		s.cursor.Set(offset + 1)
		return &log[offset], nil
	}

	if s.parentCtx.Err() != nil {
		return nil, s.parentCtx.Err()
	}

	return nil, s.ctx.Err()
}

type cursorStore struct {
	mu     sync.Mutex
	cursor int
}

func (cs *cursorStore) Get() int {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	return cs.cursor
}

func (cs *cursorStore) Set(value int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	cs.cursor = value
}
