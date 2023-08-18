package rsql_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/luno/reflex/rsql"
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
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

			var sets int

			table := rsql.NewCursorsTable(cursorsTable,
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
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	s := newTestSleep()

	ct := rsql.NewCursorsTable(
		cursorsTable,
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
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	s := newTestSleep()

	// Clone table and disable async writes.
	ct := rsql.NewCursorsTable(
		cursorsTable,
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

func TestCloneAsyncCursor(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	s := newTestSleep()

	ct := rsql.NewCursorsTable(cursorsTable).Clone(rsql.WithTestCursorSleep(t, s.Block))

	err := ct.SetCursor(context.Background(), dbc, "test", "10")
	require.NoError(t, err)

	s.UnblockOnce()
	waitForResult(t, 2, s.Count)

	c, err := ct.GetCursor(context.Background(), dbc, "test")
	require.NoError(t, err)
	require.Equal(t, "10", c)
}

func newTestSleep() *testSleep {
	return &testSleep{
		block: true,
		cond:  sync.NewCond(new(sync.Mutex)),
	}
}

type testSleep struct {
	count int
	cond  *sync.Cond
	block bool
}

func (s *testSleep) UnblockOnce() {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	defer s.cond.Broadcast()
	s.block = false
}

func (s *testSleep) Count() any {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()
	return s.count
}

func (s *testSleep) Block(_ time.Duration) {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	s.count++
	for s.block {
		s.cond.Wait()
	}

	s.block = true
}

func waitForResult(t *testing.T, expect any, f func() any) {
	t.Helper()
	require.Eventually(t, func() bool {
		return f() == expect
	}, 2*time.Second, time.Millisecond)
}
