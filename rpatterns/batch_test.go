package rpatterns_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				mu          sync.Mutex
				results     []rpatterns.Batch
				doneCount   = tt.expectedBatches
				errConsumed = errors.New("consumer done")
				emptyDelay  time.Duration
			)

			if !tt.partialBatch {
				// Do not trigger stream empty error if consumer must error.
				emptyDelay = time.Hour
			}

			b := &bootstrapMock{
				events:     ItoEList(tt.inEvents...),
				emptyDelay: emptyDelay,
				gets:       []string{""},
			}
			if !tt.partialBatch {
				// Add one more event to trigger consumer errConsumed.
				b.events = append(b.events, ItoE(0))
			}

			f := func(ctx context.Context, f fate.Fate, b rpatterns.Batch) error {
				mu.Lock()
				defer mu.Unlock()

				results = append(results, b)

				if len(results) == doneCount && !tt.partialBatch {
					return errConsumed
				}

				return nil
			}

			consumer := rpatterns.NewBatchConsumer(tt.name, b, f, 0, tt.batchLen)
			spec := rpatterns.NewBatchSpec(b.Stream, consumer)
			err := reflex.Run(context.Background(), spec)
			if tt.partialBatch {
				jtest.Assert(t, errEvents, err)
			} else {
				jtest.Assert(t, errConsumed, err)
			}

			mu.Lock()
			defer mu.Unlock()

			assert.Len(t, results, tt.expectedBatches)

			for _, batch := range results {
				assert.Len(t, batch, tt.batchLen)
			}

			// Normally we error on last best set.
			expectedSets := tt.expectedBatches - 1
			if tt.partialBatch {
				// Partial batches don't error on last set.
				expectedSets += 1
			}
			require.Len(t, b.sets, expectedSets)
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
