package reflex_test

import (
	"context"
	"time"

	"github.com/luno/reflex"
)

func newMockStreamer(events []*reflex.Event, endError error) *mockStreamer {
	return &mockStreamer{
		events:   events,
		endError: endError,
	}
}

type mockStreamer struct {
	events   []*reflex.Event
	endError error
}

func (m *mockStreamer) AddEvents(events ...*reflex.Event) {
	m.events = append(m.events, events...)
}

func (m *mockStreamer) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	index := -1
	for i, e := range m.events {
		if e.ID > after {
			break
		}
		index = i
	}
	index++

	return &sc{
		mockStreamer: m,
		ctx:          ctx,
		index:        index,
	}, nil
}

type sc struct {
	*mockStreamer
	ctx   context.Context
	index int
}

func (c *sc) Recv() (*reflex.Event, error) {
	for len(c.events) <= c.index {
		if c.endError != nil {
			return nil, c.endError
		}
		select {
		case <-c.ctx.Done():
			return nil, c.ctx.Err()
		case <-time.NewTimer(time.Millisecond * 10).C:
		}
	}
	e := c.events[c.index]
	c.index++
	return e, nil
}
