package rsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/luno/reflex"
)

type mockNotifier struct {
	mu      sync.Mutex
	count   int
	watched bool
	c       chan struct{}
}

func (m *mockNotifier) WaitForWatch() {
	for {
		m.mu.Lock()
		if m.watched {
			m.mu.Unlock()
			return
		}
		m.mu.Unlock()
		time.Sleep(time.Nanosecond) // Don't spin
	}
}

func (m *mockNotifier) Notify() {
	select {
	case m.c <- struct{}{}:
	default:
		// We don't want to deadlock any tests if the channel is blocked.
	}
}

func (m *mockNotifier) C() <-chan struct{} {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.c == nil {
		m.c = make(chan struct{}, 1)
		m.watched = true
	}

	m.count++
	return m.c
}

// mockTable provides a mock in-memory table implementing
// both the loader and inserter functions. It does not
// simulate gaps.
type mockTable struct {
	events []*reflex.Event
}

func (m *mockTable) Load(_ context.Context, _ *sql.DB, prevCursor int64,
	_ time.Duration,
) ([]*reflex.Event, error) {
	for i, e := range m.events {
		if e.IDInt() > prevCursor {
			return m.events[i:], nil
		}
	}
	return nil, nil
}

func (m *mockTable) Insert(_ context.Context, _ *sql.Tx,
	foreignID string, typ reflex.EventType, metadata []byte,
) error {
	m.events = append(m.events, &reflex.Event{
		ID:        fmt.Sprintf("%d", len(m.events)+1),
		ForeignID: foreignID,
		Timestamp: time.Now(),
		Type:      typ,
		MetaData:  metadata,
	})
	return nil
}

// mockErrorTable provides a mock in-memory table implementing
// errorInserter functions
type mockErrorTable struct{}

func (m *mockErrorTable) errorInserter(_ context.Context,
	_ *sql.DB, _ string, _ string, _ string, _ reflex.ErrorStatus,
) error {
	return nil
}
