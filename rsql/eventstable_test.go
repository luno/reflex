package rsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

func TestWithIncludeNoopEvents(t *testing.T) {
	ctx := context.Background()

	table := rsql.NewEventsTable(
		eventsTable,
		rsql.WithEventsBackoff(time.Millisecond),
		rsql.WithIncludeNoopEvents(),
	)

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	rsql.FillGaps(dbc, table)

	// Insert 1
	err := insertTestEvent(dbc, table, "1", testEventType(1))
	require.NoError(t, err)

	tx, err := dbc.Begin()
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(ctx, tx, "2", testEventType(2))
	require.NoError(t, err)

	// Rollback insert, should cause gap filler to pick it up
	err = tx.Rollback()
	require.NoError(t, err)

	// Insert 3
	err = insertTestEvent(dbc, table, "3", testEventType(3))
	require.NoError(t, err)

	// Stop a bad test from hanging
	ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	sc, err := table.ToStream(dbc)(ctx, "")
	assert.NoError(t, err)

	// This should block until delay, then return 3 (noop(2) is filtered out).
	assertEvent(t, sc, 1, -2, 3)
}

func TestGapRollbackDetection(t *testing.T) {
	table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Millisecond))

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	rsql.FillGaps(dbc, table)

	// Insert 1
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	tx, err := dbc.Begin()
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(context.Background(), tx, "2", testEventType(2))
	require.NoError(t, err)

	// Insert 3
	err = insertTestEvent(dbc, table, i2s(3), testEventType(3))
	require.NoError(t, err)

	sc, err := table.ToStream(dbc)(context.Background(), "")
	assert.NoError(t, err)
	assertEvent(t, sc, 1) // 3 not available due to gap at 2

	// Rollback gap after delay.
	t0 := time.Now()
	delay := 100 * time.Millisecond
	go func() {
		time.Sleep(delay)
		err = tx.Rollback()
		require.NoError(t, err)
	}()

	sc, err = table.ToStream(dbc)(context.Background(), "1")
	assert.NoError(t, err)

	// This should block until delay, then return 3 (noop(2) is filtered out).
	assertEvent(t, sc, 3)
	assert.True(t, time.Since(t0) >= delay, "duration %v", time.Since(t0))
}

func TestGapCommitDetection(t *testing.T) {
	table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Millisecond))

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	// Insert 1
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	tx, err := dbc.Begin()
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(context.Background(), tx, "2", testEventType(2))
	require.NoError(t, err)

	// Insert 3
	err = insertTestEvent(dbc, table, i2s(3), testEventType(3))
	require.NoError(t, err)

	sc, err := table.ToStream(dbc)(context.Background(), "")
	assert.NoError(t, err)
	assertEvent(t, sc, 1) // 3 not available due to gap at 2

	// Commit gap after delay.
	t0 := time.Now()
	delay := 100 * time.Millisecond
	go func() {
		time.Sleep(delay)
		err = tx.Commit()
		require.NoError(t, err)
	}()

	sc, err = table.ToStream(dbc)(context.Background(), "1")
	assert.NoError(t, err)

	// This should block until delay, then return 2 and 3.
	assertEvent(t, sc, 2, 3)
	assert.True(t, time.Since(t0) >= delay, "duration %v", time.Since(t0))
}

func TestNoDeadlockGap(t *testing.T) {
	table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Millisecond))

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	rsql.FillGaps(dbc, table)

	// Insert 1
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	tx, err := dbc.Begin()
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(context.Background(), tx, "2", testEventType(2))
	require.NoError(t, err)

	// Insert 3
	err = insertTestEvent(dbc, table, i2s(3), testEventType(3))
	require.NoError(t, err)

	// Rollback gap after delay.
	t0 := time.Now()
	delay := 100 * time.Millisecond
	go func() {
		time.Sleep(delay)
		err = tx.Rollback()
		require.NoError(t, err)
	}()

	// Start two stream readers, they should not deadlock after rollback
	sc1, err := table.ToStream(dbc)(context.Background(), "1")
	assert.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		sc2, err := table.ToStream(dbc)(context.Background(), "1")
		assert.NoError(t, err)

		_, err = sc2.Recv()
		require.NoError(t, err)
		wg.Done()
	}()

	// This should block until delay, then return 3 (noop(2) is filtered out).
	assertEvent(t, sc1, 3)
	assert.True(t, time.Since(t0) >= delay, "duration %v", time.Since(t0))
	wg.Wait()
}

func assertEvent(t *testing.T, sc reflex.StreamClient, el ...int) {
	for _, e := range el {
		exID := int64(e)
		exFID := e
		exType := e
		exNoop := e < 0
		if exNoop {
			exID = -int64(e)
			exType = 0
			exFID = 0
		}
		result, err := sc.Recv()
		require.NoError(t, err)
		msg := fmt.Sprintf("event %d: %#v", exID, result)
		require.Equal(t, exID, result.IDInt(), msg)
		require.Equal(t, exType, result.Type.ReflexType(), msg)
		assertEqualI2S(t, exFID, result.ForeignID, msg)
	}
}

func TestEventsTable(t *testing.T) {
	cases := []struct {
		name   string
		events []int
	}{
		{
			name:   "basic",
			events: []int{1, 2, 3, 4},
		},
		{
			name:   "nothing",
			events: []int{},
		},
		{
			name:   "duplicate",
			events: []int{1, 1, 1},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

			table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Hour))

			for _, e := range test.events {
				err := insertTestEvent(dbc, table, i2s(e), testEventType(e))
				require.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())

			sc, err := table.ToStream(dbc)(ctx, "")
			assert.NoError(t, err)

			for i, e := range test.events {
				result, err := sc.Recv()
				assert.NoError(t, err)
				msg := fmt.Sprintf("event %d: %#v", i, result)
				assert.Equal(t, int64(i+1), result.IDInt(), msg)
				assert.Equal(t, e, result.Type.ReflexType(), msg)
				assertEqualI2S(t, e, result.ForeignID, msg)
			}

			// Next call to recv should block
			t0 := time.Now()
			go func() {
				time.Sleep(time.Millisecond * 100)
				cancel()
			}()
			_, err = sc.Recv()
			d := time.Now().Sub(t0)
			assert.EqualError(t, err, "context canceled")
			assert.True(t, d >= time.Millisecond*100)

			expected := 1 // The previous blocking call
			if len(test.events) > 1 {
				expected++ // The first call in the loop above
			}
		})
	}
}

func TestInsertNoop(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	table := rsql.NewEventsTable(eventsTable)
	err := insertTestEvent(dbc, table, i2s(0), testEventType(0))
	require.EqualError(t, err, "inserting invalid noop event")
}

func TestNoGapFill(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	notifier := new(mockNotifier)

	table := rsql.NewEventsTable(eventsTable,
		rsql.WithEventsNotifier(notifier))

	// Not registering any gap filler.

	// Insert 1
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	// Gap at 2
	tx, err := dbc.Begin()
	require.NoError(t, err)
	_, err = table.Insert(context.Background(), tx, i2s(2), testEventType(2))
	require.NoError(t, err)

	// Insert 3
	err = insertTestEvent(dbc, table, i2s(3), testEventType(3))
	require.NoError(t, err)

	// Permanent gap at 2.
	err = tx.Rollback()
	require.NoError(t, err)

	// Cancel context on notifier watch.
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		notifier.WaitForWatch()
		cancel()
	}()

	sc, err := table.ToStream(dbc)(ctx, "")
	assert.NoError(t, err)

	// Get 1.
	e, err := sc.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(1), e.ForeignIDInt())

	// Block on gap 2.
	_, err = sc.Recv()
	assert.EqualError(t, err, "context canceled")
}

func TestDoubleGap(t *testing.T) {
	table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Millisecond))

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	rsql.FillGaps(dbc, table)

	// Insert 1
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	tx, err := dbc.Begin()
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(context.Background(), tx, "2", testEventType(2))
	require.NoError(t, err)

	// Gap at 2
	_, err = table.Insert(context.Background(), tx, "3", testEventType(3))
	require.NoError(t, err)

	// Insert 4
	err = insertTestEvent(dbc, table, i2s(4), testEventType(4))
	require.NoError(t, err)

	sc, err := table.ToStream(dbc)(context.Background(), "")
	assert.NoError(t, err)
	assertEvent(t, sc, 1) // 4 not available due to gap at 2 and 3

	// Rollback gap after delay.
	t0 := time.Now()
	delay := 100 * time.Millisecond
	go func() {
		time.Sleep(delay)
		err = tx.Rollback()
		require.NoError(t, err)
	}()

	sc, err = table.ToStream(dbc)(context.Background(), "1")
	assert.NoError(t, err)

	// This should block until delay, then return 4.
	assertEvent(t, sc, 4)
	assert.True(t, time.Since(t0) >= delay, "duration %v", time.Since(t0))
}

func TestRandomGaps(t *testing.T) {
	table := rsql.NewEventsTable(eventsTable, rsql.WithEventsBackoff(time.Millisecond))

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	rsql.FillGaps(dbc, table)

	// Insert 1 (will stream after this)
	err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
	require.NoError(t, err)

	// N concurrent transactions that sleep and commit or rollback.
	const n = 8
	var inserted int64
	for i := 0; i < n; i++ {
		tx, err := dbc.Begin()
		require.NoError(t, err)

		_, err = table.Insert(context.Background(), tx, "99", testEventType(i))
		require.NoError(t, err)

		go func() {
			txx := tx

			d := rand.Intn(100)    // random sleep < 100ms
			commit := rand.Intn(2) // random commit/rollback
			time.Sleep(time.Duration(d) * time.Millisecond)

			if commit == 1 {
				_ = txx.Commit()
				atomic.AddInt64(&inserted, 1)
			} else {
				_ = txx.Rollback()
			}
		}()
	}

	// Insert n+2
	err = insertTestEvent(dbc, table, i2s(11), testEventType(11))
	require.NoError(t, err)
	atomic.AddInt64(&inserted, 1)

	// This should complete in less than a second.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sc, err := table.ToStream(dbc)(ctx, "1")
	assert.NoError(t, err)

	var actual int64
	for {
		e, err := sc.Recv()
		require.NoError(t, err)
		actual++
		if e.IDInt() == n+2 {
			// Got all events
			break
		}
	}
	assert.Equal(t, atomic.LoadInt64(&inserted), actual)
}

func TestNoMetadata(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	table := rsql.NewEventsTable(eventsTable)

	err := insertTestEventMeta(dbc, table, "0", testEventType(11), []byte{1, 2, 3})
	require.Error(t, err)
	require.Contains(t, err.Error(), "metadata not enabled")
}

func TestMetadata(t *testing.T) {
	ev := DefaultEventTable()
	ev.MetadataField = "metadata"
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	table := rsql.NewEventsTable(eventsTable, rsql.WithEventMetadataField(ev.MetadataField))

	md := []byte{1, 2, 3}

	err := insertTestEventMeta(dbc, table, "0", testEventType(11), md)
	require.NoError(t, err)

	el, err := rsql.GetNextEventsForTesting(context.Background(), t, dbc, table, 0, 0)
	require.NoError(t, err)
	require.Len(t, el, 1)
	require.Equal(t, md, el[0].MetaData)

	sc, err := table.ToStream(dbc)(context.Background(), "")
	assert.NoError(t, err)

	e, err := sc.Recv()
	require.NoError(t, err)
	require.Equal(t, md, e.MetaData)

	err = insertTestEventMeta(dbc, table, "0", testEventType(11), nil)
	require.NoError(t, err)

	e, err = sc.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte(nil), e.MetaData)
}

func TestInMemNotifier(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	table := rsql.NewEventsTable(eventsTable,
		rsql.WithEventsInMemNotifier(),
		rsql.WithEventsBackoff(time.Hour))

	t0 := time.Now()

	sc, err := table.ToStream(dbc)(context.Background(), "")
	assert.NoError(t, err)

	lag := time.Millisecond * 100
	go func() {
		// Insert 1 after lag in cloned table (shared in-memory client)
		// TODO(corver): adapt inmemNotifier so we can wait for listening to avoid sleep here.
		time.Sleep(lag)
		err := insertTestEvent(dbc, table, i2s(1), testEventType(1))
		require.NoError(t, err)
	}()

	// Get 1.
	e, err := sc.Recv()
	require.NoError(t, err)
	require.Equal(t, int64(1), e.ForeignIDInt())
	require.True(t, time.Since(t0) > lag, "want: %s\ngot: %s", lag, time.Since(t0))
	require.True(t, time.Since(t0) < 5*time.Second, time.Since(t0))
}

func TestCloneInserter(t *testing.T) {
	makeInserter := func(i *int) func(context.Context, *sql.Tx, string, reflex.EventType, []byte) error {
		return func(context.Context, *sql.Tx, string, reflex.EventType, []byte) error {
			*i++
			return nil
		}
	}

	var i1 int
	table1 := rsql.NewEventsTable("test", rsql.WithEventsInserter(makeInserter(&i1)))
	_, err := table1.Insert(nil, nil, "", nil)
	jtest.RequireNil(t, err)
	require.Equal(t, 1, i1)

	table2 := table1.Clone()
	_, err = table2.Insert(nil, nil, "", nil)
	jtest.RequireNil(t, err)
	require.Equal(t, 2, i1)

	var i2 int
	table3 := table1.Clone(rsql.WithEventsInserter(makeInserter(&i2)))
	_, err = table3.Insert(nil, nil, "", nil)
	jtest.RequireNil(t, err)
	require.Equal(t, 2, i1)
	require.Equal(t, 1, i2)
}

func TestInsertMany(t *testing.T) {
	ctx := context.Background()
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

	table1 := rsql.NewEventsTable(eventsTable)

	tx, _ := dbc.Begin()
	_, err := table1.InsertMany(ctx, tx, []rsql.EventToInsert{
		{"fid1", testEventType(1), nil},
		{"fid2", testEventType(2), nil},
		{"fid3", testEventType(3), nil},
	})
	jtest.RequireNil(t, err)
	err = tx.Commit()
	jtest.RequireNil(t, err)
	el, err := rsql.GetNextEventsForTesting(context.Background(), t, dbc, table1, 0, 0)
	jtest.RequireNil(t, err)
	assert.Equal(t, "fid1", el[0].ForeignID)
	assert.Equal(t, "fid2", el[1].ForeignID)
	assert.Equal(t, "fid3", el[2].ForeignID)
}
