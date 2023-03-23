package reflex

import (
	"context"
	"strconv"
)

// NewMockStream stream options will not work with a mock stream, it will just return
// the list of events provided. Purely meant for testing.
func NewMockStream(events []*Event, endErr error) StreamFunc {
	return func(ctx context.Context, after string, opts ...StreamOption) (StreamClient, error) {
		return &mockstreamclient{events: events, endError: endErr, after: after}, nil
	}
}

type mockstreamclient struct {
	events   []*Event
	endError error
	after    string
}

func (m *mockstreamclient) Recv() (*Event, error) {
	prev := int64(-1)
	var err error

	if m.after != "" {
		prev, err = strconv.ParseInt(m.after, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	for {
		if len(m.events) == 0 {
			return nil, m.endError
		}

		e := m.events[0]
		m.events = m.events[1:]

		if e.IDInt() > prev {
			return e, nil
		} else {
			continue
		}
	}
}
