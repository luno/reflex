package rsql_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/require"
)

func TestNewCursorsTable(t *testing.T) {
	type tuple struct {
		consumerID string
		cursor     int
	}
	cases := []struct {
		name   string
		input  []tuple
		output []tuple
	}{
		{
			name: "one",
			input: []tuple{
				{"one", 1},
				{"one", 2},
				{"one", 3},
				{"one", 5},
			},
			output: []tuple{
				{"one", 5},
			},
		},
		{
			name: "multi",
			input: []tuple{
				{"one", 1},
				{"two", 2},
				{"one", 2},
				{"two", 3},
			},
			output: []tuple{
				{"one", 2},
				{"two", 3},
			},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			name := test.name + "_cursors"
			dbc := ConnectTestDB(t, "", name)
			defer dbc.Close()

			var sets int

			table := rsql.NewCursorsTable(name,
				rsql.WithCursorAsyncDisabled(),
				rsql.WithCursorSetCounter(func() { sets++ }),
			)

			ids := make(map[string]bool)

			for _, i := range test.input {
				// ensure bootstrapped at 0 per unique consumer
				if !ids[i.consumerID] {
					c, err := table.GetCursor(context.Background(), dbc, i.consumerID)
					require.NoError(t, err)
					require.Equal(t, "", c)
				}
				ids[i.consumerID] = true

				err := table.SetCursor(context.Background(), dbc, i.consumerID, strconv.Itoa(i.cursor))
				require.NoError(t, err)
			}

			for _, o := range test.output {
				c, err := table.GetCursor(context.Background(), dbc, o.consumerID)
				require.NoError(t, err)
				require.Equal(t, strconv.Itoa(o.cursor), c)
			}

			require.Equal(t, len(test.input), sets)
		})
	}
}

func TestAsyncSetCursor(t *testing.T) {
	dbc := ConnectTestDB(t, "", "cursors")
	defer dbc.Close()

	s := newTestSleep()

	ct := rsql.NewCursorsTable(
		"cursors",
		rsql.WithTestCursorSleep(t, s.Block),
	)

	c, err := ct.GetCursor(context.Background(), dbc, "test")
	require.NoError(t, err)
	require.Equal(t, "", c)

	err = ct.SetCursor(context.Background(), dbc, "test", "5")
	require.NoError(t, err)

	c, err = ct.GetCursor(context.Background(), dbc, "test")
	require.NoError(t, err)
	require.Equal(t, "", c)

	s.UnblockOnce()
	waitForResult(t, 2, s.Count)

	getCursor := func() interface{} {
		c, err := ct.GetCursor(context.Background(), dbc, "test")
		require.NoError(t, err)
		return c
	}

	waitForResult(t, "5", getCursor)
}

func TestSyncSetCursor(t *testing.T) {
	dbc := ConnectTestDB(t, "", "cursors")
	defer dbc.Close()

	s := new(testSleep)

	//Clone table and disable async writes.
	ct := rsql.NewCursorsTable(
		"cursors",
		rsql.WithCursorAsyncDisabled(),
		rsql.WithTestCursorSleep(t, s.Block),
	)

	err := ct.SetCursor(context.Background(), dbc, "test", "10")
	require.NoError(t, err)

	c, err := ct.GetCursor(context.Background(), dbc, "test")
	require.NoError(t, err)
	require.Equal(t, "10", c)

	require.Equal(t, 0, s.Count())
}

func newTestSleep() *testSleep {
	return &testSleep{
		block: true,
	}
}

type testSleep struct {
	count int
	block bool
	mu    sync.Mutex
}

func (s *testSleep) UnblockOnce() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = false
}

func (s *testSleep) isBlocked() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.block
}

func (s *testSleep) Count() interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

func (s *testSleep) Block(_ time.Duration) {
	s.mu.Lock()
	s.count++
	s.mu.Unlock()

	for s.isBlocked() {
		time.Sleep(time.Nanosecond) // don't spin
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.block = true
}

func waitForResult(t *testing.T, expect interface{}, f func() interface{}) {
	t.Helper()
	t0 := time.Now()

	for {
		last := f()
		if last == expect {
			return
		}
		if time.Now().Sub(t0) > time.Second*2 {
			require.Fail(t, "Timeout waiting for result", "last=%v", last)
			return
		}
		time.Sleep(time.Millisecond) // don't spin
	}
}
