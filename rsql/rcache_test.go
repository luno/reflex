package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/luno/reflex"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var rCacheLimit = 100

func TestGap(t *testing.T) {
	cases := []struct {
		name string
		add  []int64
		err  string
	}{
		{
			name: "Gap at 4", // Error only triggered for gap in loaded batches.
			add:  []int64{5, 6},
		}, {
			name: "Gap at 5",
			add:  []int64{4, 6},
			err:  ErrConsecEvent.Error(),
		}, {
			name: "No gap",
			add:  []int64{4, 5, 6},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			q := newQ()
			c := newRCache(q.Load, "test")
			c.limit = rCacheLimit

			q.addEvents(3)

			res, next, err := c.Load(nil, nil, 0, 0)
			assert.NoError(t, err)
			assert.Len(t, res, 3)
			assert.Equal(t, int(next), 3)
			q.assertTotal(t, 1)
			q.assertQuery(t, 0, 1)
			assert.Equal(t, 3, c.Len())

			for _, e := range test.add {
				q.events = append(q.events, &reflex.Event{ID: i2s(e)})
			}
			_, _, err = c.Load(nil, nil, 3, 0)
			if test.err == "" {
				require.NoError(t, err)
			} else {
				assert.EqualError(t, err, test.err)
			}
		})
	}

}

func TestRCache(t *testing.T) {
	tests := []struct {
		name    string
		add1    int
		q1      int64
		len1    int
		total1  int
		q1Count int
		cLen1   int

		add2    int
		q2      int64
		len2    int
		total2  int
		q2Count int
		cLen2   int
	}{
		{
			name:    "empty",
			total1:  1,
			q1Count: 1,

			total2:  2,
			q2Count: 2,
		},
		{
			name:    "miss-hit",
			add1:    2,
			len1:    2,
			total1:  1,
			q1Count: 1,
			cLen1:   2,

			len2:    2,
			total2:  1,
			q2Count: 1,
			cLen2:   2,
		},
		{
			name:    "miss-hit-offset",
			add1:    3,
			len1:    3,
			total1:  1,
			q1Count: 1,
			cLen1:   3,

			q2:      2,
			len2:    3 - 2,
			total2:  1,
			q2Count: 0,
			cLen2:   3,
		},
		{
			name:    "miss-miss",
			add1:    3,
			len1:    3,
			total1:  1,
			q1Count: 1,
			cLen1:   3,

			q2:      3,
			len2:    0,
			total2:  2,
			q2Count: 1,
			cLen2:   3,
		},
		{
			name:    "gap",
			add1:    5,
			len1:    5,
			total1:  1,
			q1Count: 1,
			cLen1:   5,

			add2:    5,
			q2:      7,
			len2:    10 - 7,
			total2:  2,
			q2Count: 1,
			cLen2:   10 - 7,
		},
		{
			name:    "trim",
			add1:    rCacheLimit * 2,
			len1:    rCacheLimit * 2,
			total1:  1,
			q1Count: 1,
			cLen1:   rCacheLimit,

			add2:    10,
			q2:      int64(rCacheLimit * 2),
			len2:    10,
			total2:  2,
			q2Count: 1,
			cLen2:   rCacheLimit,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := newQ()
			c := newRCache(q.Load, "test")
			c.limit = rCacheLimit

			q.addEvents(test.add1)

			res, next, err := c.Load(nil, nil, test.q1, 0)
			assert.NoError(t, err)
			assert.Len(t, res, test.len1)
			assert.Equal(t, int(next), test.add1)
			q.assertTotal(t, test.total1)
			q.assertQuery(t, test.q1, test.q1Count)
			assert.Equal(t, test.cLen1, c.Len())

			q.addEvents(test.add2)

			res, next, err = c.Load(nil, nil, test.q2, 0)
			assert.NoError(t, err)
			assert.Len(t, res, test.len2)
			assert.Equal(t, int(next), test.add1+test.add2)
			q.assertTotal(t, test.total2)
			q.assertQuery(t, test.q2, test.q2Count)
			assert.Equal(t, test.cLen2, c.Len())
		})
	}
}

type query struct {
	queried map[int64]int
	events  []*reflex.Event
}

func newQ() *query {
	return &query{
		queried: make(map[int64]int),
	}
}

func (q *query) addEvents(count int) {
	tail := len(q.events)
	for i := tail; i < tail+count; i++ {
		idStr := strconv.FormatInt(int64(i+1), 10) // EventIDs start from 1
		q.events = append(q.events, &reflex.Event{ID: idStr})
	}
}

func (q *query) assertTotal(t *testing.T, count int) {
	var total int
	for _, value := range q.queried {
		total += value
	}
	assert.Equal(t, count, total)
}

func (q *query) assertQuery(t *testing.T, lastID int64, count int) {
	assert.Equal(t, count, q.queried[lastID])
}

func (q *query) Load(ctx context.Context, dbc *sql.DB, prev int64,
	lag time.Duration) ([]*reflex.Event, int64, error) {

	q.queried[prev]++

	if len(q.events) <= int(prev) {
		return nil, 0, nil
	}
	el := q.events[prev:]
	return el, getNextCursor(el, prev), nil
}

func i2s(i int64) string {
	return strconv.FormatInt(i, 10)
}
