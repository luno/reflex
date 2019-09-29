package rsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/grpctest"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	eventsTable = "events"
)

func TestStream(t *testing.T) {
	tests := []struct {
		name        string
		fillBuffers bool
		count       int
	}{
		{
			name:        "backpressure",
			count:       10000,
			fillBuffers: true,
		},
		{
			name:  "one",
			count: 1,
		},
		{
			name:  "empty",
			count: 0,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Use mocks to improve speed
			dbc, etable, _, client, stop := setupMockState(t)
			defer stop()

			for i := 0; i < test.count; i++ {
				err := insertTestEvent(dbc, etable, i2s(i), testEventType(i))
				assert.NoError(t, err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			sc, err := client.StreamEvents(ctx, "")
			assert.NoError(t, err)

			if test.fillBuffers {
				//before reading anything, wait for full buffers
				var n int
				waitFor(t, time.Second, func() bool {
					n := etable.(*mockETable).GetReads()
					return n > 2000
				})
				assert.True(t, n < 5000, "Expect n < 5000: %s", n)
			}

			var results []*reflex.Event
			for i := 0; i < test.count; i++ {
				r, err := sc.Recv()
				assert.NoError(t, err)
				results = append(results, r)
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
			dbc, eventsTable, cursorsTable, cl, stop := setupState(t, nil, nil)
			defer stop()

			for _, e := range test.events {
				err := insertTestEvent(dbc, eventsTable, i2s(int(e)), testEventType(e))
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

			consumer := reflex.NewConsumer(reflex.ConsumerName(test.name), f)
			consumable := reflex.NewConsumable(cl.StreamEvents, cursorsTable.ToStore(dbc))
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

			c, err := cursorsTable.GetCursor(context.Background(), dbc, test.name)
			assert.NoError(t, err)
			assert.Equal(t, strconv.Itoa(len(results)), c)
		})
	}
}

func TestStreamClientErrors(t *testing.T) {
	// use mock tables since temp DB tables are dropped on first context cancel.
	dbc, eventsTable, cursorsTable, cl, stop := setupMockState(t)
	defer stop()

	for i := 0; i < 10; i++ {
		err := insertTestEvent(dbc, eventsTable, i2s(i), testEventType(i))
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
		sc, err = cl.StreamEvents(ctx, after, ol...)
		return sc, err
	}

	// error consumer
	ctx := context.Background()
	consumer := reflex.NewConsumer(reflex.ConsumerName("cid"), f)
	consumable := reflex.NewConsumable(streamFunc, cursorsTable.ToStore(dbc))
	err := consumable.Consume(ctx, consumer)

	assert.True(t, errors.Is(err, errNOK))
	assert.Len(t, calls, 1)
	assert.Nil(t, ctx.Err()) // parent context not cancelled
	assert.Equal(t, int64(1), calls[0].IDInt())
	assertEqualI2S(t, 0, calls[0].ForeignID)
	assert.Equal(t, 1, cursorsTable.(*mockCTable).flushed)

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

	assert.True(t, errors.Is(err, errNOK))
	assert.Len(t, calls, 3)
	assert.Nil(t, ctx.Err()) // parent context not cancelled
	assert.Equal(t, 2, cursorsTable.(*mockCTable).flushed)

	for i := 0; i < 3; i++ {
		res := calls[i]
		assertEqualI2S(t, i, res.ForeignID)
		assert.Equal(t, i, res.Type.ReflexType())
		assert.Equal(t, int64(i+1), res.IDInt())
	}
}

func TestConsumeStreamLag(t *testing.T) {
	dbc, eventTable, cursorsTable, cl, stop := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventsBackoff(0)})
	defer stop()

	total := 10
	for i := 1; i <= total; i++ { // Start at 1 since 0 is noop.
		err := insertTestEvent(dbc, eventTable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}

	// push back first three event 2 mins
	firstBatch := 3
	for i := 0; i < firstBatch; i++ {
		_, err := dbc.Exec("update "+eventsTable+" set timestamp=date_sub(timestamp, interval 120 second) where id=?", i+1)
		assert.NoError(t, err)
	}

	errDone := errors.New("done", j.C("ERR_DONE"))
	var results []*reflex.Event
	f := func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
		results = append(results, e)
		if len(results) == total {
			return errDone
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		// wait for first batch
		waitForResult(t, firstBatch, func() interface{} { return len(results) })

		// push back rest of events 2 mins
		for i := firstBatch; i < total; i++ {
			_, err := dbc.Exec("update "+eventsTable+" set timestamp=date_sub(timestamp, interval 120 second) where id=?", i+1)
			assert.NoError(t, err)
		}

		// wait for rest
		waitForResult(t, total, func() interface{} { return len(results) })

		time.Sleep(time.Second) // sleep and cancel (should not affect test duration)
		cancel()
	}()

	consumer := reflex.NewConsumer(reflex.ConsumerName("test"), f)
	consumable := reflex.NewConsumable(cl.StreamEvents, cursorsTable.ToStore(dbc),
		reflex.WithStreamLag(time.Minute))
	err := consumable.Consume(ctx, consumer)
	assert.True(t, errors.Is(err, errDone))

	assert.Len(t, results, total)
	for i, e := range results {
		ii := i + 1
		assertEqualI2S(t, ii, e.ForeignID)
		assert.Equal(t, ii, e.Type.ReflexType())
		assert.Equal(t, int64(ii), e.IDInt())
	}
}

func TestStreamHead(t *testing.T) {
	notifier := new(mockNotifier)
	dbc, eventTable, _, cl, stop := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventsNotifier(notifier)})
	defer stop()

	prefill := 10
	expect := 5

	for i := 1; i <= prefill; i++ { // Start at 1 since 0 is noop.
		err := insertTestEvent(dbc, eventTable, i2s(i), testEventType(i))
		require.NoError(t, err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		notifier.WaitForWatch()

		for i := prefill + 1; i <= prefill+expect; i++ {
			err := insertTestEvent(dbc, eventTable, i2s(i), testEventType(i))
			assert.NoError(t, err)
		}

		notifier.Notify()

		// prevent broken test from hanging
		time.Sleep(time.Second)
		assert.Error(t, context.Canceled, ctx.Err()) // Should be done
		cancel()
	}()

	sc, err := cl.StreamEvents(ctx, "", reflex.WithStreamFromHead())
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

func TestStreamMetadata(t *testing.T) {
	cache := eventsMetadataField
	defer func() {
		eventsMetadataField = cache
	}()
	eventsMetadataField = "metadata"

	dbc, eventTable, _, cl, stop := setupState(t, nil,
		[]rsql.EventsOption{rsql.WithEventMetadataField(eventsMetadataField)})
	defer stop()

	prefill := 10
	for i := 1; i <= prefill; i++ { // Start at 1 since 0 is noop.
		var meta []byte
		for l := 0; l < i-1; l++ {
			meta = append(meta, byte(l))
		}

		err := insertTestEventMeta(dbc, eventTable, i2s(i), testEventType(i), meta)
		require.NoError(t, err)
	}

	sc, err := cl.StreamEvents(context.Background(), "")
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

func setupState(t *testing.T, streamOptions []reflex.StreamOption,
	eventOptions []rsql.EventsOption) (*sql.DB, rsql.EventsTable,
	rsql.CursorsTable, *grpctest.Client, func()) {

	dbc := ConnectTestDB(t, eventsTable, cursorsTable)
	etable := rsql.NewEventsTable(eventsTable, eventOptions...)
	ctable := rsql.NewCursorsTable(cursorsTable, rsql.WithCursorAsyncPeriod(time.Minute)) // require flush
	srv, url := grpctest.NewServer(t, etable.ToStream(dbc, streamOptions...), ctable.ToStore(dbc))
	cl := grpctest.NewClient(t, url)

	return dbc, etable, ctable, cl, func() {
		dbc.Close()
		srv.Stop()
		assert.NoError(t, cl.Close())
	}
}

func setupMockState(t *testing.T) (*sql.DB, rsql.EventsTable, rsql.CursorsTable, *grpctest.Client, func()) {
	dbc := ConnectTestDB(t, eventsTable, cursorsTable)
	eventsTable := newMockETable()
	cursorsTable := newMockCTable()
	srv, url := grpctest.NewServer(t, eventsTable.ToStream(dbc), cursorsTable.ToStore(dbc))
	cl := grpctest.NewClient(t, url)

	return dbc, eventsTable, cursorsTable, cl, func() {
		dbc.Close()
		srv.Stop()
		assert.NoError(t, cl.Close())
	}
}

func newMockCTable() rsql.CursorsTable {
	return &mockCTable{cursors: make(map[string]string)}
}

type mockCTable struct {
	cursors map[string]string
	flushed int
}

func (m *mockCTable) Flush(ctx context.Context) error {
	m.flushed++
	return nil
}

func (m *mockCTable) GetCursor(ctx context.Context, dbc *sql.DB, consumerID string) (string, error) {
	return m.cursors[consumerID], nil
}

func (m *mockCTable) SetCursor(ctx context.Context, dbc *sql.DB, consumerID string, cursor string) error {
	m.cursors[consumerID] = cursor
	return nil
}

func (m *mockCTable) SetCursorAsync(ctx context.Context, dbc *sql.DB, consumerID string, cursor string) {
	_ = m.SetCursor(ctx, dbc, consumerID, cursor)
}

func (m *mockCTable) Clone(ol ...rsql.CursorsOption) rsql.CursorsTable {
	return m
}
func (m *mockCTable) ToStore(dbc *sql.DB, opts ...rsql.CursorsOption) reflex.CursorStore {
	return &mockEtableCStore{m}
}

func newMockETable() rsql.EventsTable {
	return &mockETable{}
}

type mockEtableCStore struct {
	*mockCTable
}

func (m *mockEtableCStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	return m.mockCTable.GetCursor(ctx, nil, consumerName)
}

func (m *mockEtableCStore) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	return m.mockCTable.SetCursor(ctx, nil, consumerName, cursor)
}

type mockETable struct {
	mu     sync.Mutex
	events []*reflex.Event
	reads  int
}

func (m *mockETable) Insert(ctx context.Context, tx *sql.Tx, foreignID string,
	typ reflex.EventType) (rsql.NotifyFunc, error) {
	return m.InsertWithMetadata(ctx, tx, foreignID, typ, nil)
}

func (m *mockETable) InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID string,
	typ reflex.EventType, metadata []byte) (rsql.NotifyFunc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, &reflex.Event{
		ID:        strconv.Itoa(len(m.events) + 1),
		Timestamp: time.Now(),
		Type:      typ,
		ForeignID: fmt.Sprintf("%v", foreignID),
		MetaData:  metadata,
	})
	return func() {}, nil
}

func (m *mockETable) Stream(ctx context.Context, dbc *sql.DB, after string, options ...reflex.StreamOption) reflex.StreamClient {
	return &msc{
		ctx:        ctx,
		mockETable: m,
	}
}

func (m *mockETable) Clone(opts ...rsql.EventsOption) rsql.EventsTable {
	return m
}

func (m *mockETable) ToStream(dbc *sql.DB, opts1 ...reflex.StreamOption) reflex.StreamFunc {
	return func(ctx context.Context, after string,
		opts2 ...reflex.StreamOption) (reflex.StreamClient, error) {
		return m.Stream(ctx, dbc, after, append(opts1, opts2...)...), nil
	}
}

func (m *mockETable) GetReads() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reads
}

func (m *mockETable) Stop() {
}

type msc struct {
	*mockETable
	i   int
	ctx context.Context
}

func (m *msc) Recv() (*reflex.Event, error) {
	for {
		if m.ctx.Err() != nil {
			return nil, m.ctx.Err()
		}
		m.mu.Lock()
		if len(m.events) > m.i {
			m.i++
			m.reads++
			defer m.mu.Unlock()
			return m.events[m.i-1], nil
		}
		m.mu.Unlock()
		time.Sleep(time.Millisecond) // don't spin
	}
}
