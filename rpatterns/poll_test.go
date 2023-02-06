package rpatterns_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

func TestPoller(t *testing.T) {
	g := NewGate()
	api := new(pollapi)
	p := rpatterns.NewPoller(api.Poll, rpatterns.WithSleep(t, g.Sleep))

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	sc, err := p.Stream(ctx, "")
	require.NoError(t, err)

	_, err = sc.Recv() // This blocks and then we cancel the context.
	jtest.Require(t, context.DeadlineExceeded, err)
	cancel()

	// Try again, but with events
	sc, err = p.Stream(context.Background(), "")
	require.NoError(t, err)

	// Insert 10 events
	n0 := 10
	for i := 1; i <= n0; i++ {
		api.Add(*ItoE(i))
	}

	g.Unblock()

	// Assert we get all the events.
	for i := 1; i <= n0; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err)
		require.Equal(t, int64(i), e.IDInt())
	}

	g.Block()
	time.Sleep(100 * time.Millisecond)
	g.Unblock()

	// Poll again, wait for it to sleep, then add 20 more events.
	n1 := 20

	// Add n1 more
	for i := 1; i <= n1; i++ {
		api.Add(*ItoE(n0 + i))
	}

	for i := 1; i <= n1; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err)
		require.Equal(t, int64(n0+i), e.IDInt())
	}
}

type pollapi struct {
	events []reflex.Event
	mu     sync.Mutex
}

func (p *pollapi) Add(events ...reflex.Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, events...)
}

func (p *pollapi) Poll(_ context.Context, after string, _ ...reflex.StreamOption) ([]reflex.Event, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	var afterInt int64
	if after != "" {
		var err error
		afterInt, err = strconv.ParseInt(after, 10, 64)
		if err != nil {
			return nil, err
		}
	}

	var res []reflex.Event
	for _, event := range p.events {
		if event.IDInt() > afterInt {
			res = append(res, event)
		}
	}

	return res, nil
}

type Gate struct {
	mu sync.Mutex
	ch chan time.Time
}

func NewGate() *Gate {
	return &Gate{ch: make(chan time.Time)}
}

func (g *Gate) Block() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ch = make(chan time.Time)
}

func (g *Gate) Unblock() {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.ch)
}

func (g *Gate) Sleep(_ time.Duration) <-chan time.Time {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ch
}
