package rsql_test

import (
	"sync"
	"time"
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
