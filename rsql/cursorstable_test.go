package rsql_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/assert"
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
					assert.NoError(t, err)
					assert.Equal(t, "", c)
				}
				ids[i.consumerID] = true

				err := table.SetCursor(context.Background(), dbc, i.consumerID, strconv.Itoa(i.cursor))
				assert.NoError(t, err)
			}

			for _, o := range test.output {
				c, err := table.GetCursor(context.Background(), dbc, o.consumerID)
				assert.NoError(t, err)
				assert.Equal(t, strconv.Itoa(o.cursor), c)
			}

			assert.Equal(t, len(test.input), sets)
		})
	}
}

func TestSetCursor(t *testing.T) {
	dbc := ConnectTestDB(t, "", "cursors")
	defer dbc.Close()

	s := new(testSleep)

	ct := rsql.NewCursorsTable(
		"cursors",
		rsql.WithTestCursorSleep(t, s.IncCount),
	)

	c, err := ct.GetCursor(context.Background(), dbc, "test")
	assert.NoError(t, err)
	assert.Equal(t, "", c)

	err = ct.SetCursor(context.Background(), dbc, "test", "5")
	assert.NoError(t, err)

	c, err = ct.GetCursor(context.Background(), dbc, "test")
	assert.NoError(t, err)
	assert.Equal(t, "", c)

	waitForResult(t, "1", s.Count)

	s.block = false

	getCursor := func() interface{} {
		c, err := ct.GetCursor(context.Background(), dbc, "test")
		assert.NoError(t, err)
		return c
	}

	waitForResult(t, "5", getCursor)

	//Clone table and disable async writes.
	ct = ct.Clone(rsql.WithCursorAsyncDisabled())

	countBeforeSet := s.Count()

	err = ct.SetCursor(context.Background(), dbc, "test", "10")
	assert.NoError(t, err)

	c, err = ct.GetCursor(context.Background(), dbc, "test")
	assert.NoError(t, err)
	assert.Equal(t, "10", c)

	assert.Equal(t, countBeforeSet, s.Count())
}

type testSleep struct {
	count int
	block bool
}

func (s *testSleep) Count() interface{} {
	return strconv.Itoa(s.count)
}

func (s *testSleep) IncCount(d time.Duration) {
	s.count++
}

func (s *testSleep) Sleep(d time.Duration) {
	s.count++
	for s.block {
		time.Sleep(time.Nanosecond) // don't spin
	}
}

func waitForResult(t *testing.T, expect interface{}, f func() interface{}) {
	t0 := time.Now()

	for {
		if f() == expect {
			return
		}
		if time.Now().Sub(t0) > time.Second*2 {
			assert.Fail(t, "Timeout waiting for f")
			return
		}
		time.Sleep(time.Millisecond) // don't spin
	}
}
