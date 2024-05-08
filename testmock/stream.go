package testmock

import (
	"context"
	"strconv"
	"sync"

	"github.com/luno/jettison/errors"

	"github.com/luno/reflex"
)

// NewStreamFunc provides an implementation of reflex.StreamFunc where the provided events will be streamed via the
// returned reflex.StreamClient implementation.
func NewStreamFunc(events ...reflex.Event) reflex.StreamFunc {
	return func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		var offset int
		if after != "" {
			i, err := strconv.ParseInt(after, 10, 64)
			if err != nil {
				return nil, err
			}

			offset = int(i)
		}

		return &streamClientImpl{
			ctx:    ctx,
			log:    events,
			offset: offset,
		}, nil
	}
}

// NewStreamClient provides an implementation of reflex.StreamClient where the provided events will be returned in order
// when Recv is called.
func NewStreamClient(ctx context.Context, events ...reflex.Event) reflex.StreamClient {
	return &streamClientImpl{
		ctx: ctx,
		log: events,
	}
}

type streamClientImpl struct {
	ctx    context.Context
	mu     sync.Mutex
	log    []reflex.Event
	offset int
}

func (s *streamClientImpl) Recv() (*reflex.Event, error) {
	for s.ctx.Err() == nil {
		s.mu.Lock()
		log := s.log
		offset := s.offset
		s.offset += 1
		s.mu.Unlock()

		if len(log)-1 < offset {
			return nil, errors.Wrap(reflex.ErrHeadReached, "")
		}

		return &log[offset], nil
	}

	return nil, s.ctx.Err()
}
