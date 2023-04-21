package rpatterns_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luno/fate"
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

			f := func(ctx context.Context, f fate.Fate, b rpatterns.Batch) error {
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
	recvEndErr := errors.New("recv error")
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

			f := func(ctx context.Context, f fate.Fate, b rpatterns.Batch) error {
				for k, v := range b {
					assert.Equal(t, int64(k+1), v.IDInt())
				}

				return nil
			}

			consumer := rpatterns.NewBatchConsumer(tt.name, b, f, 0, tt.batchLen)

			spec := rpatterns.NewBatchSpec(b.Stream, consumer)
			ctx := context.Background()
			err := reflex.Run(ctx, spec)
			jtest.Assert(t, recvEndErr, err)

			events = ItoEList(tt.passEvents...)
			b.events = events

			err = reflex.Run(ctx, spec)
			jtest.Assert(t, recvEndErr, err)
		})
	}
}

func TestInvalidConfig(t *testing.T) {
	b := new(bootstrapMock)
	b.gets = []string{"1"}
	events := ItoEList([]int{1, 2, 3}...)
	b.events = events

	f := func(ctx context.Context, f fate.Fate, b rpatterns.Batch) error {
		return nil
	}

	consumer := rpatterns.NewBatchConsumer("", b, f, 0, 0)
	spec := rpatterns.NewBatchSpec(b.Stream, consumer)
	ctx := context.Background()
	err := reflex.Run(ctx, spec)
	jtest.Assert(t, errors.New("batchPeriod or batchLen must be non-zero"), err)
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

	el := EventList{Ctx: ctx, Events: ItoEList(1, 2, 3), Stop: make(chan struct{})}
	stream := func(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
		return &(el), nil
	}

	batches := make(chan rpatterns.Batch)
	cs := rpatterns.MemCursorStore()
	consumer := rpatterns.NewBatchConsumer("test", cs,
		func(ctx context.Context, f fate.Fate, batch rpatterns.Batch) error {
			batches <- batch
			return errors.New("whoops")
		},
		time.Second, 3,
	)

	spec := rpatterns.NewBatchSpec(stream, consumer)

	go func() {
		defer close(batches)
		err := reflex.Run(ctx, spec)
		jtest.Assert(t, reflex.ErrHeadReached, err)
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
