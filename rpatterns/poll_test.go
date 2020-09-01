package rpatterns_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/require"
)

func TestPoller(t *testing.T) {
	s := &sleeper{cond: sync.NewCond(new(sync.Mutex))}
	api := new(pollapi)
	p := rpatterns.NewPoller(api.Poll, rpatterns.WithSleep(t, s.Sleep))

	ctx, cancel := context.WithCancel(context.Background())
	sc, err := p.Stream(ctx, "")
	require.NoError(t, err)

	// No events in API, so first call to Recv will block/sleep.
	go func() {
		require.Eventually(t, func() bool {
			return s.Count() == 1
		}, time.Second, time.Millisecond)

		// Cancel context
		cancel()
	}()

	_, err = sc.Recv() // This blocks and then we cancel the context.
	jtest.Require(t, context.Canceled, err)
	s.Unblock()

	// Try again, but with events
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	sc, err = p.Stream(ctx, "")
	require.NoError(t, err)

	// Insert 10 events
	n0 := 10
	for i := 1; i <= n0; i++ {
		api.Add(*ItoE(i))
	}

	// Assert we get all the events.
	for i := 1; i <= n0; i++ {
		e, err := sc.Recv()
		jtest.RequireNil(t, err)
		require.Equal(t, int64(i), e.IDInt())
	}

	// It didn't sleep again, count still 1.
	require.Equal(t, 1, s.Count())

	// Poll again, wait for it to sleep, then add 20 more events.
	n1 := 20

	// No more events in API, so next call to Recv will block/sleep.
	go func() {
		require.Eventually(t, func() bool {
			return s.Count() == 2
		}, time.Second, time.Millisecond)

		// Add n1 more
		for i := 1; i <= n1; i++ {
			api.Add(*ItoE(n0 + i))
		}

		s.Unblock()
	}()

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

type sleeper struct {
	cond      *sync.Cond
	count     int
	unblocked bool
}

func (s *sleeper) Unblock() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	s.unblocked = true
	s.cond.Signal()
}
func (s *sleeper) Count() int {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	return s.count
}

func (s *sleeper) Sleep(time.Duration) <-chan time.Time {
	ch := make(chan time.Time, 0)

	go func() {
		s.cond.L.Lock()
		s.count++
		for !s.unblocked {
			s.cond.Wait()
		}

		s.unblocked = false

		s.cond.L.Unlock()

		close(ch)
	}()

	return ch
}
