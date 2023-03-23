package reflex

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestRunDelayLag(t *testing.T) {
	errDone := errors.New("no more events to mock")
	t0 := time.Now()

	// Define common set of events for each test.
	eventsFunc := func() []*Event {
		return []*Event{
			{ID: "0", Timestamp: t0.Add(time.Second * -2)},
			{ID: "1", Timestamp: t0.Add(time.Second * -1)},
			{ID: "2", Timestamp: t0.Add(time.Second * 0)},
			{ID: "3", Timestamp: t0.Add(time.Second * 1)},
			{ID: "4", Timestamp: t0.Add(time.Second * 2)},
		}
	}

	tests := []struct {
		name      string
		opt       StreamOption
		sleepSecs []int
		wantErr   bool
	}{
		{
			name:      "no lag",
			opt:       WithStreamToHead(),
			sleepSecs: []int{}, // No events are delayed.
		}, {
			name:      "1 sec lag",
			opt:       WithStreamLag(time.Second),
			sleepSecs: []int{1, 2, 3}, // First two events are not delayed.
		}, {
			name:      "5 sec lag",
			opt:       WithStreamLag(time.Second * 5),
			sleepSecs: []int{3, 4, 5, 6, 7}, // All events are delayed.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the sleep durations
			var sleeps []time.Duration
			newTimer = func(d time.Duration) *time.Timer {
				sleeps = append(sleeps, d)
				return time.NewTimer(0)
			}

			// Mock current time.
			since = func(t time.Time) time.Duration {
				return t0.Sub(t)
			}

			// Mock the spec
			var (
				actualOpts []StreamOption
				consumer   mockconsumer
			)
			spec := NewSpec(func(ctx context.Context, after string, opts ...StreamOption) (StreamClient, error) {
				actualOpts = opts
				return &mockstreamclient{eventsFunc(), errDone, ""}, nil
			}, &mockcursor{}, &consumer, tt.opt, WithStreamFromHead(), WithStreamToHead())

			// Run
			err := Run(context.Background(), spec)
			jtest.Require(t, errDone, err)

			// Assert lag options filtered out.
			var temp StreamOptions
			for _, opt := range actualOpts {
				opt(&temp)
			}
			require.Zero(t, temp.Lag)

			// Assert sleep durations
			var expectedSleeps []time.Duration
			for _, sec := range tt.sleepSecs {
				expectedSleeps = append(expectedSleeps, time.Second*time.Duration(sec))
			}
			require.Equal(t, expectedSleeps, sleeps)
		})
	}
}

func TestMockStream(t *testing.T) {
	tests := []struct {
		name           string
		events         []*Event
		startCursorAt  string
		expectedCursor string
		expectedEvents int64
	}{
		{
			name: "test mock events",
			events: []*Event{
				{
					ID: "1",
				},
				{
					ID: "2",
				},
				{
					ID: "3",
				},
			},
			expectedCursor: "3",
			expectedEvents: 3,
		},
		{
			name: "mock events with cursor",
			events: []*Event{
				{
					ID: "1",
				},
				{
					ID: "2",
				},
				{
					ID: "3",
				},
			},
			startCursorAt:  "2",
			expectedCursor: "3",
			expectedEvents: 1,
		},
	}

	ctx := context.Background()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			endErr := errors.New("end of mock")
			streamFunc := NewMockStream(tt.events, endErr)
			consumer := &mockconsumer{}

			memCursor := &mockcursor{
				cursor: map[string]string{
					consumer.Name(): tt.startCursorAt,
				},
			}

			spec := NewSpec(streamFunc, memCursor, consumer)

			err := Run(ctx, spec)
			jtest.Require(t, endErr, err)

			cursorResult, err := memCursor.GetCursor(ctx, consumer.Name())
			jtest.RequireNil(t, err)
			assert.Equal(t, tt.expectedCursor, cursorResult)

			assert.Equal(t, tt.expectedEvents, int64(len(consumer.Events)))
		})
	}
}

type mockconsumer struct {
	Events []*Event
}

func (m *mockconsumer) Name() string {
	return ""
}

func (m *mockconsumer) Consume(ctx context.Context, fate fate.Fate, event *Event) error {
	m.Events = append(m.Events, event)
	return nil
}

type mockcursor struct {
	cursor map[string]string
}

func (m *mockcursor) GetCursor(_ context.Context, consumerName string) (string, error) {
	val, exists := m.cursor[consumerName]
	if !exists {
		val = ""
	}

	return val, nil
}

func (m *mockcursor) SetCursor(_ context.Context, consumerName string, cursor string) error {
	if m.cursor == nil {
		m.cursor = make(map[string]string, 0)
	}

	m.cursor[consumerName] = cursor
	return nil
}

func (m *mockcursor) Flush(_ context.Context) error {
	return nil
}
