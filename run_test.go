package reflex

import (
	"context"
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
				return &mockstreamclient{eventsFunc(), errDone}, nil
			}, mockcursor{}, &consumer, tt.opt, WithStreamFromHead(), WithStreamToHead())

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

type mockstreamclient struct {
	Events   []*Event
	EndError error
}

func (m *mockstreamclient) Recv() (*Event, error) {
	if len(m.Events) == 0 {
		return nil, m.EndError
	}
	e := m.Events[0]
	m.Events = m.Events[1:]
	return e, nil
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

type mockcursor struct{}

func (m mockcursor) GetCursor(ctx context.Context, consumerName string) (string, error) {
	return "", nil
}

func (m mockcursor) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	return nil
}

func (m mockcursor) Flush(ctx context.Context) error {
	return nil
}
