package rsql_test

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/jtest"
	"github.com/luno/jettison/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/grpctest"
	"github.com/luno/reflex/rsql"
)

func TestStream(t *testing.T) {
	tests := []struct {
		name        string
		fillBuffers bool
		count       int
	}{
		{
			name:  "empty",
			count: 0,
		},
		{
			name:  "one",
			count: 1,
		},
		{
			name:  "hundred",
			count: 100,
		},
		{
			name:        "backpressure",
			count:       10000,
			fillBuffers: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Use mock table for speed
			mock := new(mockTable)
			s := setupState(t, nil, []rsql.EventsOption{
				rsql.WithEventsInserter(mock.Insert),
				rsql.WithEventsLoader(mock.Load),
			}, DefaultEventTable(), DefaultCursorTable())
			defer s.stop()

			// Skip 0 since that results in noop event.
			for i := 1; i <= test.count; i++ {
				err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
				assert.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			sc, err := s.client.StreamEvents(ctx, "")
			assert.NoError(t, err)

			if test.fillBuffers {
				// before reading anything, wait for full buffers
				var n int
				waitFor(t, time.Second, func() bool {
					n = int(s.server.SentCount())
					return n > 2000
				})
				assert.True(t, n < 5000, "Expect n < 5000: %s", n)
			}

			var results []*reflex.Event
			for i := 0; i < test.count; i++ {
				e, err := sc.Recv()
				assert.NoError(t, err)
				results = append(results, e)
			}
			assert.Len(t, results, test.count)
			cancel()
		})
	}
}

func TestConsumeStreamClient(t *testing.T) {
	cases := []struct {
		name   string
		events []int
	}{
		{
			name:   "basic",
			events: []int{1, 2, 3, 4, 5},
		},
		{
			name:   "duplicates",
			events: []int{1, 1, 2, 2, 1, 1, 2, 2},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			s := setupState(t, nil, nil, DefaultEventTable(), DefaultCursorTable())
			defer s.stop()

			for _, e := range test.events {
				err := insertTestEvent(s.dbc, s.etable, i2s(int(e)), testEventType(e))
				assert.NoError(t, err)
			}

			var results []*reflex.Event
			f := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
				results = append(results, e)
				return nil
			}

			ctx, cancel := context.WithCancel(context.Background())

			// consume should block
			t0 := time.Now()
			timeout := time.Millisecond * 100
			go func() {
				time.Sleep(timeout)
				cancel()
			}()

			consumer := reflex.NewConsumer(test.name, f)
			consumable := reflex.NewConsumable(s.client.StreamEvents, s.ctable.ToStore(s.dbc))
			err := consumable.Consume(ctx, consumer)
			d := time.Now().Sub(t0)
			assert.Contains(t, err.Error(), "context canceled")
			assert.True(t, d >= timeout)

			assert.Len(t, results, len(test.events))
			for i, e := range test.events {
				res := results[i]
				assert.Equal(t, int64(e), res.ForeignIDInt())
				assertEqualI2S(t, e, res.ForeignID)
				assert.Equal(t, e, res.Type.ReflexType())
				assert.Equal(t, int64(i+1), res.IDInt())
				assert.Len(t, res.MetaData, 0)
			}

			c, err := s.ctable.GetCursor(context.Background(), s.dbc, test.name)
			assert.NoError(t, err)
			assert.Equal(t, strconv.Itoa(len(results)), c)
		})
	}
}

func TestStreamClientErrors(t *testing.T) {
	// use mock table since temp DB tables are dropped on first context cancel.
	// Use mock table for speed
	mock := new(mockTable)
	s := setupState(t, nil, []rsql.EventsOption{
		rsql.WithEventsInserter(mock.Insert),
		rsql.WithEventsLoader(mock.Load),
	}, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	// Skip 0 since it is a noop.
	for i := 1; i <= 10; i++ {
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		assert.NoError(t, err)
	}

	errNOK := errors.New("nok", j.C("ERR_NOK"))
	mocks := []error{errNOK, nil, nil, errNOK}
	var calls []*reflex.Event
	f := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		calls = append(calls, e)
		err := mocks[0]
		mocks = mocks[1:]
		return err
	}
	// cache stream client
	var sc reflex.StreamClient
	streamFunc := func(ctx context.Context, after string, ol ...reflex.StreamOption) (reflex.StreamClient, error) {
		var err error
		sc, err = s.client.StreamEvents(ctx, after, ol...)
		return sc, err
	}

	// error consumer
	ctx := context.Background()
	consumer := reflex.NewConsumer("cid", f)
	consumable := reflex.NewConsumable(streamFunc, s.ctable.ToStore(s.dbc))
	err := consumable.Consume(ctx, consumer)

	jtest.Require(t, errNOK, err)
	assert.Len(t, calls, 1)
	assert.Nil(t, ctx.Err()) // parent context not cancelled
	assert.Equal(t, int64(1), calls[0].IDInt())
	assert.Equal(t, int64(1), calls[0].ForeignIDInt())

	// assert grpc stream also cancelled (it is async so wait)
	waitFor(t, time.Second, func() bool {
		_, err := sc.Recv()
		if err != nil {
			assert.Errorf(t, err, "context canceled")
			return true
		}
		return false
	})

	// try again
	calls = []*reflex.Event{}
	ctx = context.Background()
	err = consumable.Consume(ctx, consumer)

	jtest.Require(t, errNOK, err)
	require.Len(t, calls, 3)
	require.Nil(t, ctx.Err()) // parent context not cancelled

	for i := 0; i < 3; i++ {
		res := calls[i]
		require.Equal(t, i+1, res.Type.ReflexType())
		require.Equal(t, int64(i+1), res.ForeignIDInt())
		require.Equal(t, int64(i+1), res.IDInt())
	}
}

func TestConsumeStreamLag(t *testing.T) {
	s := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventsBackoff(0)},
		DefaultEventTable(),
		DefaultCursorTable(),
	)
	defer s.stop()

	total := 10
	for i := 1; i <= total; i++ { // Start at 1 since 0 is noop.
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}

	// delay last three events by 10 seconds
	for i := total - 2; i <= total; i++ {
		_, err := s.dbc.Exec("update "+eventsTable+" set timestamp=date_add(timestamp, interval 10 second) where id=?", i)
		assert.NoError(t, err)
	}

	errDone := errors.New("done", j.C("ERR_DONE"))
	feed := make(chan *reflex.Event)
	f := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		feed <- e
		if e.IDInt() == int64(total) {
			close(feed)
			return errDone
		}
		return nil
	}

	spec := reflex.NewSpec(
		s.client.StreamEvents,
		s.ctable.ToStore(s.dbc),
		reflex.NewConsumer("test", f),
		reflex.WithStreamLag(2*time.Second+100*time.Millisecond), // Add 100ms just to be sure.
	)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := reflex.Run(context.Background(), spec)
		jtest.Require(t, errDone, err)
	}()

	i := 1
	for ev := range feed {
		diff := time.Since(ev.Timestamp)
		assert.Greater(t, diff, 2*time.Second)
		assert.Less(t, diff, 3*time.Second)
		assert.Equal(t, int64(i), ev.IDInt())
		i++
	}
	wg.Wait()
}

func TestStreamFromHead(t *testing.T) {
	notifier := new(mockNotifier)
	s := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventsNotifier(notifier)}, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	prefill := 10
	expect := 5

	for i := 1; i <= prefill; i++ { // Start at 1 since 0 is noop.
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		notifier.WaitForWatch()

		for i := prefill + 1; i <= prefill+expect; i++ {
			err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
			assert.NoError(t, err)
		}

		notifier.Notify()

		// prevent broken test from hanging
		time.Sleep(time.Second)
		assert.Error(t, context.Canceled, ctx.Err()) // Should be done
		cancel()
	}()

	sc, err := s.client.StreamEvents(ctx, "", reflex.WithStreamFromHead())
	assert.NoError(t, err)

	var results []*reflex.Event
	for {
		e, err := sc.Recv()
		assert.NoError(t, err)

		results = append(results, e)
		if len(results) == expect {
			cancel()
			_, err := sc.Recv()
			assert.Error(t, err, context.Canceled)
			break
		}
	}

	assert.Len(t, results, expect)
	for i, e := range results {
		ii := i + 1 + prefill
		assertEqualI2S(t, ii, e.ForeignID)
		assert.Equal(t, ii, e.Type.ReflexType())
		assert.Equal(t, int64(ii), e.IDInt())
	}
}

func TestStreamToHead(t *testing.T) {
	notifier := new(mockNotifier)
	s := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventsNotifier(notifier)}, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	assertCount := func(t *testing.T, after string, n, offset int) {
		sc, err := s.client.StreamEvents(context.TODO(), after, reflex.WithStreamToHead())
		assert.NoError(t, err)

		var results []*reflex.Event
		for {
			e, err := sc.Recv()
			if reflex.IsHeadReachedErr(err) {
				break
			}
			if err != nil {
				log.Error(nil, err)
			}

			jtest.RequireNil(t, err)

			results = append(results, e)
		}

		require.Len(t, results, n)
		for i, e := range results {
			ii := i + offset + 1
			assertEqualI2S(t, ii, e.ForeignID)
			assert.Equal(t, ii, e.Type.ReflexType())
			assert.Equal(t, int64(ii), e.IDInt())
		}
	}

	// Table empty
	assertCount(t, "", 0, 0)

	round1 := 10
	for i := 1; i <= round1; i++ { // Start at 1 since 0 is noop.
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}
	assertCount(t, "", round1, 0)
	assertCount(t, "5", round1-5, 5)

	round2 := 20
	for i := round1 + 1; i <= round1+round2; i++ { // Offset by 1
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}
	assertCount(t, "", round2+round1, 0)
	assertCount(t, "10", round2, round1)
	assertCount(t, "30", 0, 0)
}

func TestStreamMetadata(t *testing.T) {
	ev := DefaultEventTable()
	ev.MetadataField = "metadata"
	s := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventMetadataField(ev.MetadataField)}, ev, DefaultCursorTable())
	defer s.stop()

	prefill := 10
	for i := 1; i <= prefill; i++ { // Start at 1 since 0 is noop.
		var meta []byte
		for l := 0; l < i-1; l++ {
			meta = append(meta, byte(l))
		}

		err := insertTestEventMeta(s.dbc, s.etable, i2s(i), testEventType(i), meta)
		require.NoError(t, err)
	}

	sc, err := s.client.StreamEvents(context.Background(), "")
	assert.NoError(t, err)

	var results []*reflex.Event
	for i := 0; i < prefill; i++ {
		e, err := sc.Recv()
		assert.NoError(t, err)
		results = append(results, e)
	}

	assert.Len(t, results, prefill)
	for i, e := range results {
		ii := i + 1
		assertEqualI2S(t, ii, e.ForeignID)
		assert.Equal(t, ii, e.Type.ReflexType())
		assert.Equal(t, int64(ii), e.IDInt())
		assert.Len(t, e.MetaData, i)

		var meta []byte
		for l := 0; l < i; l++ {
			meta = append(meta, byte(l))
		}
		assert.EqualValues(t, meta, e.MetaData)
	}
}

func TestStreamLag(t *testing.T) {
	// Wrap baseloader to count sql queries.
	loadCountCh := make(chan struct{}, 100)
	var table *rsql.EventsTable
	loader := func(ctx context.Context, dbc *sql.DB, prevCursor int64,
		lag time.Duration,
	) (events []*reflex.Event, err error) {
		loadCountCh <- struct{}{}
		return rsql.GetNextEventsForTesting(ctx, t, dbc, table, prevCursor, lag)
	}

	notifier := new(mockNotifier)
	s := setupState(t, nil,
		[]rsql.EventsOption{
			rsql.WithEventsLoader(loader),
			rsql.WithEventsNotifier(notifier),
			rsql.WithEventsBackoff(time.Hour), // Manual control on sleep.
		}, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	table = s.etable

	// Insert 10 events, staggered 1 min apart
	total := 10
	for i := 1; i <= total; i++ {
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)

		ts := (total - i) * 60
		_, err = s.dbc.Exec("update "+eventsTable+" set timestamp=date_sub(timestamp, interval ? second) where id=?", ts, i)
		assert.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First read all events into the cache.
	sc1 := s.etable.Stream(ctx, s.dbc, "")
	for i := 0; i < total; i++ {
		_, err := sc1.Recv()
		require.NoError(t, err)
	}

	// We only did only sql load.
	require.Len(t, loadCountCh, 1)

	// Then try again, but lag 5.5 min, so expect only 4 events.
	lag := reflex.WithStreamLag(time.Second * (60*5 + 30))
	sc2 := s.etable.Stream(ctx, s.dbc, "", lag)

	for i := 0; i < 4; i++ {
		_, err := sc2.Recv()
		require.NoError(t, err)
	}

	// We still only did only sql load, we are using the cache.
	require.Len(t, loadCountCh, 1)

	// Next call should not return an event, but backoff and then we cancel.
	go func() {
		notifier.WaitForWatch()
		// Trigger another read from the cache
		notifier.Notify()
		notifier.WaitForWatch()
		// And then cancel
		cancel()
	}()

	// We do not expect an event here.
	_, err := sc2.Recv()
	jtest.Assert(t, context.Canceled, err)

	// We still only did only sql load, we are using the cache.
	require.Len(t, loadCountCh, 1)
}

// TestStreamLagNoCache is a copy of TestStreamLagCache but with cache disabled.
func TestStreamLagNoCache(t *testing.T) {
	// Wrap baseloader to count sql queries.
	loadCountCh := make(chan struct{}, 100)
	var table *rsql.EventsTable
	loader := func(ctx context.Context, dbc *sql.DB, prevCursor int64,
		lag time.Duration,
	) (events []*reflex.Event, err error) {
		loadCountCh <- struct{}{}
		return rsql.GetNextEventsForTesting(ctx, t, dbc, table, prevCursor, lag)
	}

	notifier := new(mockNotifier)
	s := setupState(t, nil,
		[]rsql.EventsOption{
			rsql.WithEventsLoader(loader),
			rsql.WithoutEventsCache(),
			rsql.WithEventsNotifier(notifier),
			rsql.WithEventsBackoff(time.Hour), // Manual control on sleep.
		}, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	table = s.etable

	// Insert 10 events, staggered 1 min apart
	total := 10
	for i := 1; i <= total; i++ {
		err := insertTestEvent(s.dbc, s.etable, i2s(i), testEventType(i))
		require.NoError(t, err)

		ts := (total - i) * 60
		_, err = s.dbc.Exec("update "+eventsTable+" set timestamp=date_sub(timestamp, interval ? second) where id=?", ts, i)
		assert.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// First read all 10 events.
	sc1 := s.etable.Stream(ctx, s.dbc, "")
	for i := 0; i < total; i++ {
		_, err := sc1.Recv()
		require.NoError(t, err)
	}

	// We did one sql load.
	require.Len(t, loadCountCh, 1)

	// Then try again, but lag 5.5 min, so expect only 4 events.
	lag := reflex.WithStreamLag(time.Second * (60*5 + 30))
	sc2 := s.etable.Stream(ctx, s.dbc, "", lag)

	for i := 0; i < 4; i++ {
		_, err := sc2.Recv()
		require.NoError(t, err)
	}

	// We did another sql load, since cache disabled.
	require.Len(t, loadCountCh, 2)

	// Next call should not return an event, but backoff and then we cancel.
	go func() {
		notifier.WaitForWatch()
		// Trigger another read from the cache
		notifier.Notify()
		notifier.WaitForWatch()
		// And then cancel
		cancel()
	}()

	// We do not expect an event here.
	_, err := sc2.Recv()
	jtest.Assert(t, context.Canceled, err)

	// We did another sql load, since cache disabled, sometimes more due to races.
	require.True(t, len(loadCountCh) >= 3)
}

func TestCancelError(t *testing.T) {
	s := setupState(t, nil, nil, DefaultEventTable(), DefaultCursorTable())
	defer s.stop()

	ctx, cancel := context.WithCancel(context.Background())

	sc, err := s.client.StreamEvents(ctx, "")
	require.NoError(t, err)

	cancel()

	_, err = sc.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "context canceled") // Jettison doesn't support native grpc status errors properly.
}

type teststate struct {
	dbc    *sql.DB
	etable *rsql.EventsTable
	ctable rsql.CursorsTable
	client *grpctest.Client
	server *grpctest.Server
	stop   func()
}

func setupState(t *testing.T, streamOptions []reflex.StreamOption,
	eventOptions []rsql.EventsOption, ev EventTableSchema, crs CursorTableSchema,
) *teststate {
	dbc := ConnectTestDB(t, ev, crs)
	etable := rsql.NewEventsTable(eventsTable, eventOptions...)
	ctable := rsql.NewCursorsTable(cursorsTable, rsql.WithCursorAsyncPeriod(time.Minute)) // require flush
	srv, url := grpctest.NewServer(t, etable.ToStream(dbc, streamOptions...), ctable.ToStore(dbc))
	cl := grpctest.NewClient(t, url)
	stop := func() {
		assert.NoError(t, dbc.Close())
		srv.Stop()
		assert.NoError(t, cl.Close())
	}

	return &teststate{
		dbc:    dbc,
		etable: etable,
		ctable: ctable,
		client: cl,
		server: srv,
		stop:   stop,
	}
}
