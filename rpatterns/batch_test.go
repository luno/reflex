package rpatterns_test

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

func TestRunBatchConsumer(t *testing.T) {
	tests := []struct {
		name            string
		batchLen        int
		expectedBatches int
		partialBatch    bool
		inEvents        []int
	}{
		{
			name:            "5_batches_of_4",
			batchLen:        4,
			inEvents:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expectedBatches: 5,
		},
		{
			name:            "2_batches_of_10",
			batchLen:        10,
			inEvents:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expectedBatches: 2,
		},
		{
			name:            "1_batches_of_15",
			batchLen:        15,
			inEvents:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expectedBatches: 1,
			partialBatch:    true,
		},
		{
			name:            "1_batches_of_20",
			batchLen:        20,
			inEvents:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expectedBatches: 1,
		},
		{
			name:            "OutOfBoundsConsumer",
			batchLen:        40,
			expectedBatches: 0,
			inEvents:        []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			partialBatch:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				mu         sync.Mutex
				results    []rpatterns.Batch
				emptyDelay time.Duration
			)

			b := &bootstrapMock{
				events:     ItoEList(tt.inEvents...),
				emptyDelay: emptyDelay,
				gets:       []string{""},
			}
			if !tt.partialBatch {
				// Add one more event to trigger consumer.
				b.events = append(b.events, ItoE(0))
			}

			f := func(ctx context.Context, b rpatterns.Batch) error {
				mu.Lock()
				defer mu.Unlock()

				results = append(results, b)
				return nil
			}

			consumer := rpatterns.NewBatchConsumer(tt.name, b, f, 0, tt.batchLen)
			spec := rpatterns.NewBatchSpec(b.Stream, consumer)
			err := reflex.Run(context.Background(), spec)
			jtest.Assert(t, errEvents, err)

			mu.Lock()
			defer mu.Unlock()

			assert.Len(t, results, tt.expectedBatches)

			for _, batch := range results {
				assert.Len(t, batch, tt.batchLen)
			}

			require.Len(t, b.sets, tt.expectedBatches)
		})
	}
}

func TestReset(t *testing.T) {
	tests := []struct {
		name       string
		batchLen   int
		failEvents []int
		passEvents []int
	}{
		{
			name:       "NoDuplicates10",
			batchLen:   10,
			failEvents: []int{1, 2, 3, 4, 5},
			passEvents: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		},
		{
			name:       "NoDuplicates20",
			batchLen:   20,
			failEvents: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			passEvents: []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := new(bootstrapMock)
			b.gets = []string{"1", "2"}
			events := ItoEList(tt.failEvents...)
			b.events = events

			f := func(ctx context.Context, b rpatterns.Batch) error {
				for k, v := range b {
					assert.Equal(t, int64(k+1), v.IDInt())
				}

				return nil
			}

			consumer := rpatterns.NewBatchConsumer(tt.name, b, f, 0, tt.batchLen)

			spec := rpatterns.NewBatchSpec(b.Stream, consumer)
			ctx := context.Background()
			err := reflex.Run(ctx, spec)
			jtest.Assert(t, errEvents, err)

			events = ItoEList(tt.passEvents...)
			b.events = events

			err = reflex.Run(ctx, spec)
			jtest.Assert(t, errEvents, err)
		})
	}
}

func TestInvalidConfig(t *testing.T) {
	b := new(bootstrapMock)
	b.gets = []string{"1"}
	events := ItoEList([]int{1, 2, 3}...)
	b.events = events

	f := func(ctx context.Context, b rpatterns.Batch) error {
		return nil
	}

	consumer := rpatterns.NewBatchConsumer("", b, f, 0, 0)
	spec := rpatterns.NewBatchSpec(b.Stream, consumer)
	ctx := context.Background()
	err := reflex.Run(ctx, spec)
	jtest.Assert(t, rpatterns.ErrInvalidBatchConfig, err)
}

type EventList struct {
	Ctx    context.Context
	Idx    int
	Events []*reflex.Event
	Stop   chan struct{}
}

func (l *EventList) Recv() (*reflex.Event, error) {
	if l.Idx >= len(l.Events) {
		select {
		case <-l.Stop:
			return nil, reflex.ErrHeadReached
		case <-l.Ctx.Done():
			return nil, l.Ctx.Err()
		}
	}
	e := l.Events[l.Idx]
	l.Idx++
	return e, nil
}

func TestBatchError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	expErr := errors.New("whoops")

	el := EventList{Ctx: ctx, Events: ItoEList(1, 2, 3), Stop: make(chan struct{})}
	stream := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		return &(el), nil
	}

	batches := make(chan rpatterns.Batch)
	cs := rpatterns.MemCursorStore()
	consumer := rpatterns.NewBatchConsumer("test", cs,
		func(ctx context.Context, batch rpatterns.Batch) error {
			batches <- batch
			return expErr
		},
		time.Second, 3,
	)

	spec := rpatterns.NewBatchSpec(stream, consumer)

	go func() {
		defer close(batches)
		err := reflex.Run(ctx, spec)
		jtest.Assert(t, expErr, err)
	}()

	for bNo := 0; bNo < 3; bNo++ {
		b, ok := <-batches
		if !ok {
			break
		}
		assert.Len(t, b, 3)
	}
	close(el.Stop)

	<-batches
}

type EventsMax struct {
	Idx         int
	Max         int
	chDone      chan any
	chNextEvent chan any
	wait        time.Duration
}

func (l *EventsMax) Recv() (*reflex.Event, error) {
	if l.Idx >= l.Max {
		l.chDone <- struct{}{}
	}

	if l.chNextEvent != nil {
		// Wait until signal to continue
		<-l.chNextEvent
	}

	l.Idx++
	ev := reflex.Event{
		ID:        strconv.Itoa(l.Idx),
		Type:      testEventType(l.Idx),
		ForeignID: strconv.Itoa(l.Idx),
		Timestamp: time.Now(),
	}

	time.Sleep(l.wait)

	return &ev, nil
}

type ctxCancel struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func TestContextCancelled(t *testing.T) {
	chGetCtx := make(chan ctxCancel)
	chCancelCtx := make(chan any)

	fnGetCtx := func() context.Context {
		ctx := <-chGetCtx
		return ctx.ctx
	}

	processTracker := make(map[int64]bool)

	cancelCtxEvents := []int64{10, 25, 50}

	chDone := make(chan any)

	events := EventsMax{Max: 100, chDone: chDone}
	stream := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		idx, _ := strconv.Atoi(after)
		events.Idx = idx
		return &events, nil
	}

	cs := rpatterns.MemCursorStore()
	consumer := rpatterns.NewBatchConsumer("test", cs,
		func(ctx context.Context, batch rpatterns.Batch) error {
			if ctx.Err() != nil {
				return context.Canceled
			}
			for _, b := range batch {
				_, ok := processTracker[b.IDInt()]
				assert.False(t, ok, "event id already processed", b.ID)

				processTracker[b.IDInt()] = true
				for _, can := range cancelCtxEvents {
					if b.IDInt() == can {
						chCancelCtx <- struct{}{}
					}
				}
			}

			return nil
		},
		time.Second, 5,
	)

	spec := rpatterns.NewBatchSpec(stream, consumer)
	go func() {
		rpatterns.RunForever(fnGetCtx, spec)
	}()

	ctx, cancel := context.WithCancel(context.Background())

	for {
		if ctx.Err() != nil {
			ctx, cancel = context.WithCancel(context.Background())
		}

		var isDone bool

		select {
		case chGetCtx <- ctxCancel{
			ctx:    ctx,
			cancel: cancel,
		}:
		case <-chCancelCtx:
			cancel()
		case <-ctx.Done():
		case <-chDone:
			isDone = true
		case <-time.After(time.Second * 5):
			assert.Fail(t, "test timed out")
			isDone = true
		}

		if isDone {
			break
		}
	}

	for idx := 1; idx <= events.Max; idx++ {
		_, ok := processTracker[int64(idx)]
		if !ok {
			assert.Equal(t, true, ok, fmt.Sprintf("event id %d not processed", idx))
		}
	}
}

func TestBatchPeriod(t *testing.T) {
	processTracker := make(map[int64]bool)

	ctx := context.Background()

	chDone := make(chan any)

	events := EventsMax{Max: 50, wait: time.Millisecond}
	stream := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		idx, _ := strconv.Atoi(after)
		events.Idx = idx
		return &events, nil
	}

	cs := rpatterns.MemCursorStore()
	consumer := rpatterns.NewBatchConsumer("test", cs,
		func(ctx context.Context, batch rpatterns.Batch) error {
			for _, b := range batch {
				_, ok := processTracker[b.IDInt()]
				assert.False(t, ok, "event id already processed", b.ID)

				processTracker[b.IDInt()] = true
				if b.IDInt() == int64(events.Max) {
					chDone <- struct{}{}
				}
			}

			return nil
		},
		time.Millisecond*10, 0,
	)

	spec := rpatterns.NewBatchSpec(stream, consumer)
	go func() {
		rpatterns.RunForever(func() context.Context {
			return ctx
		}, spec)
	}()

	select {
	case <-chDone:
	case <-time.After(time.Second * 5):
		assert.Fail(t, "test timed out")
	}

	for idx := 1; idx <= events.Max; idx++ {
		_, ok := processTracker[int64(idx)]
		if !ok {
			assert.Equal(t, true, ok, fmt.Sprintf("event id %d not processed", idx))
		}
	}
}

func TestBatchErrorState(t *testing.T) {
	ctx := context.Background()

	var tMu sync.Mutex
	processTracker := make(map[int64]bool)

	// Ensure events are processed one-by-one for testing purposes
	chNextEvent := make(chan any, 1)
	chDone := make(chan any)

	events := EventsMax{Max: 100, chNextEvent: chNextEvent, chDone: chDone}
	stream := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		idx, _ := strconv.Atoi(after)
		events.Idx = idx
		return &events, nil
	}

	someError := errors.New("some error")

	chErr := make(chan error)

	returnError := true

	cs := rpatterns.MemCursorStore()
	consumer := rpatterns.NewBatchConsumer("test", cs,
		func(ctx context.Context, batch rpatterns.Batch) error {
			chNextEvent <- struct{}{}

			// Error on first attempt. Second attempt should be fine
			if returnError {
				returnError = false
				return someError
			} else {
				tMu.Lock()
				defer tMu.Unlock()
				for _, b := range batch {
					_, ok := processTracker[b.IDInt()]
					assert.False(t, ok, "event id already processed", b.ID)

					processTracker[b.IDInt()] = true
				}

				return nil
			}
		},
		time.Millisecond, 0,
	)

	spec := rpatterns.NewBatchSpec(stream, consumer)
	runProcessor := func() {
		chErr <- reflex.Run(ctx, spec)
	}

	// Run processor until second event which will trigger an ErrBatchState
	go runProcessor()

	// Signal first event
	chNextEvent <- struct{}{}

	select {
	case err := <-chErr:
		assert.True(t, errors.Is(err, rpatterns.ErrBatchState))
	case <-time.After(time.Second * 5):
		assert.Fail(t, "test timed out")
	}

	// Cursor would be on event 2 which is the one that triggers the ErrBatchState
	assert.Equal(t, 2, events.Idx)

	go runProcessor()

	// Signal first event
	chNextEvent <- struct{}{}

	select {
	case <-chErr:
		assert.Fail(t, "unexpected error return")
	case <-chDone:
		time.Sleep(time.Millisecond * 100) // Wait for last event to process since it's a background process
		// Batch recovered
	case <-time.After(time.Second * 5):
		assert.Fail(t, "test timed out")
	}

	for idx := 1; idx <= events.Max; idx++ {
		tMu.Lock()
		_, ok := processTracker[int64(idx)]
		tMu.Unlock()
		if !ok {
			assert.Equal(t, true, ok, fmt.Sprintf("event id %d not processed", idx))
		}
	}
}

func TestNewBatchConsumerCanStop(t *testing.T) {
	b := new(bootstrapMock)
	f := func(ctx context.Context, b rpatterns.Batch) error {
		return nil
	}

	consumer := rpatterns.NewBatchConsumer("", b, f, 0, 0)
	err := consumer.Stop()
	jtest.RequireNil(t, err)

	err = consumer.Stop()
	jtest.RequireNil(t, err)

	err = consumer.Reset(context.Background())
	jtest.RequireNil(t, err)

	err = consumer.Stop()
	jtest.RequireNil(t, err)
}
