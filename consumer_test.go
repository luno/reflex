package reflex

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/luno/reflex/internal/metrics"
	"github.com/luno/reflex/internal/tracing"
)

type (
	stream int64
	sType  int
)

func (i sType) ReflexType() int { return int(i) }

func (s *stream) Recv() (*Event, error) {
	val := int64(*s) + 1
	*s = stream(val)
	return &Event{
		ID:        strconv.FormatInt(val, 10),
		Type:      sType(1),
		Timestamp: time.Now(),
	}, nil
}

type cursor int64

func (c cursor) GetCursor(context.Context, string) (string, error) {
	return strconv.FormatInt(int64(c), 10), nil
}

func (c *cursor) SetCursor(_ context.Context, _ string, val string) error {
	v, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return err
	}
	*c = cursor(v)
	return nil
}

func (c cursor) Flush(context.Context) error { return nil }

func makeStream(_ context.Context, after string, _ ...StreamOption) (StreamClient, error) {
	var s stream
	if after != "" {
		i, err := strconv.ParseInt(after, 10, 64)
		if err != nil {
			return nil, err
		}
		s = stream(i)
	}
	return &s, nil
}

func TestReuseConsumer(t *testing.T) {
	idToFail := int64(2)

	metrics.ConsumerLag.Reset()

	con := NewConsumer("test", func(ctx context.Context, fate fate.Fate, event *Event) error {
		// Should have a metric when running inside a Consume function
		assert.Len(t, fetchMetrics(), 1)
		if event.IDInt() < idToFail {
			return nil
		}
		return errors.New("break")
	})

	var store cursor
	spec := NewSpec(makeStream, &store, con)

	ctx := context.Background()

	// Run the consumer, consuming a single event
	_ = Run(ctx, spec)
	v, err := store.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "1", v)

	assert.Len(t, fetchMetrics(), 1)

	idToFail++

	// Run again to make sure we can re-use the consumer
	_ = Run(ctx, spec)
	v, err = store.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "2", v)

	assert.Len(t, fetchMetrics(), 1)
}

func fetchMetrics() []prometheus.Metric {
	ch := make(chan prometheus.Metric, 100)
	metrics.ConsumerLag.Collect(ch)
	close(ch)
	ret := make([]prometheus.Metric, 0, len(ch))
	for m := range ch {
		ret = append(ret, m)
	}
	return ret
}

func TestConsumeFromFresh(t *testing.T) {
	c := NewConsumer("test",
		func(context.Context, fate.Fate, *Event) error { return nil },
	)
	err := c.Consume(context.Background(), fate.New(), &Event{
		ID:        "1",
		Type:      sType(1),
		Timestamp: time.Now(),
	})
	jtest.RequireNil(t, err)
}

func TestPassTraceIntoConsumerContext(t *testing.T) {
	t.Run("Ensure trace is loaded into context as parent and passed to consumer func", func(t *testing.T) {
		tp := tracesdk.NewTracerProvider()
		otel.SetTracerProvider(tp)

		traceID, err := trace.TraceIDFromHex("00000000000000000000000000000009")
		jtest.RequireNil(t, err)

		spanID, err := trace.SpanIDFromHex("0000000000000002")
		jtest.RequireNil(t, err)

		expectedSpanCtx := trace.NewSpanContext(
			trace.SpanContextConfig{
				TraceID: traceID,
				SpanID:  spanID,
			},
		)

		spanJson, err := tracing.Marshal(expectedSpanCtx)
		jtest.RequireNil(t, err)

		e := &Event{
			ID:        "1",
			Type:      sType(1),
			Timestamp: time.Now(),
			Trace:     spanJson,
		}

		c := NewConsumer("test",
			func(ctx context.Context, f fate.Fate, e *Event) error {
				span := trace.SpanFromContext(ctx)
				require.Equal(t, expectedSpanCtx.TraceID().String(), span.SpanContext().TraceID().String())
				require.Equal(t, expectedSpanCtx.TraceState().String(), span.SpanContext().TraceState().String())

				return nil
			},
		)

		err = c.Consume(context.Background(), fate.New(), e)
		jtest.RequireNil(t, err)
	})
}

func TestConsumeWithSkip(t *testing.T) {
	ctx := context.Background()
	consumerName := "test_consumer"
	f := fate.New()
	consumedCounter := new(int)
	cFn := func(context.Context, fate.Fate, *Event) error {
		*consumedCounter++
		return nil
	}
	evts := []*Event{
		{
			ID:        "1",
			Type:      eventType(1),
			Timestamp: time.Now(),
		},
		{
			ID:        "2",
			Type:      eventType(2),
			Timestamp: time.Now(),
		},
		{
			ID:        "3",
			Type:      eventType(3),
			Timestamp: time.Now(),
		},
	}

	tcs := []struct {
		name            string
		c               Consumer
		expConsumeCount int
		expMetricCount  int
		expSkipCount    float64
		errs            []error
	}{
		{
			name:            "no include filter provided",
			c:               NewConsumer(consumerName, cFn),
			expConsumeCount: 3,
			expMetricCount:  0,
			expSkipCount:    0,
		},
		{
			name:            "empty include filter provided",
			c:               NewConsumer(consumerName, cFn, WithFilterIncludeTypes()),
			expConsumeCount: 3,
			expMetricCount:  0,
			expSkipCount:    0,
		},
		{
			name:            "nill event filter provided",
			c:               NewConsumer(consumerName, cFn, WithEventFilter(nil)),
			expConsumeCount: 3,
			expMetricCount:  0,
			expSkipCount:    0,
		},
		{
			name:            "include filter provided",
			c:               NewConsumer(consumerName, cFn, WithFilterIncludeTypes(eventType(1), eventType(2))),
			expConsumeCount: 2,
			expMetricCount:  1,
			expSkipCount:    1,
		},
		{
			name: "event filter provided",
			c: NewConsumer(consumerName, cFn, WithEventFilter(func(e *Event) (bool, error) {
				return e.ID == "1" || e.ID == "2", nil
			})),
			expConsumeCount: 2,
			expMetricCount:  1,
			expSkipCount:    1,
		},
		{
			name: "include filter and event filter provided",
			c: NewConsumer(consumerName, cFn, WithFilterIncludeTypes(eventType(1), eventType(2)), WithEventFilter(func(e *Event) (bool, error) {
				return e.ID == "1", nil
			})),
			expConsumeCount: 1,
			expMetricCount:  1,
			expSkipCount:    2,
		},
		{
			name: "include filter and event filter errors",
			c: NewConsumer(consumerName, cFn, WithFilterIncludeTypes(eventType(1), eventType(2)), WithEventFilter(func(e *Event) (bool, error) {
				if e.ID == "1" {
					return false, errors.New("something failed")
				}
				return true, nil
			})),
			expConsumeCount: 1,
			expMetricCount:  1,
			expSkipCount:    1,
			errs:            []error{filterErr, nil, nil},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ConsumerSkippedEvents.Reset()
			*consumedCounter = 0

			for i, ev := range evts {
				err := tc.c.Consume(ctx, f, ev)
				if tc.errs == nil {
					jtest.RequireNil(t, err)
				} else {
					jtest.Require(t, tc.errs[i], err)
				}
			}

			require.Equal(t, tc.expConsumeCount, *consumedCounter)
			require.Equal(t, tc.expMetricCount, testutil.CollectAndCount(metrics.ConsumerSkippedEvents))
			require.Equal(t, tc.expSkipCount, testutil.ToFloat64(metrics.ConsumerSkippedEvents.WithLabelValues(consumerName)))
		})
	}
}

func TestConsumeWithErrorAndReporting(t *testing.T) {
	ctx := context.Background()
	consumerName := "test_consumer"
	f := fate.New()
	consumedCounter := new(int)
	recoveredCounter := new(int)
	eventId := new(int)
	cErr := errors.New("cfn errored")
	rErr := errors.New("rfn changed error")
	cFnErr := func(context.Context, fate.Fate, *Event) error {
		*consumedCounter++
		return errors.Wrap(cErr, "")
	}
	cFnCanc := func(context.Context, fate.Fate, *Event) error {
		*consumedCounter++
		return errors.Wrap(context.Canceled, "")
	}
	rFnNil := func(_ context.Context, _ fate.Fate, e *Event, c Consumer, err error) error {
		*recoveredCounter++
		*eventId, _ = strconv.Atoi(e.ID)
		return nil
	}
	rFnSame := func(_ context.Context, _ fate.Fate, e *Event, c Consumer, err error) error {
		*recoveredCounter++
		*eventId, _ = strconv.Atoi(e.ID)
		return err
	}
	rFnDiff := func(_ context.Context, _ fate.Fate, e *Event, c Consumer, err error) error {
		*recoveredCounter++
		*eventId, _ = strconv.Atoi(e.ID)
		return errors.Wrap(rErr, "")
	}
	evts := []*Event{
		{
			ID:        "1",
			Type:      eventType(1),
			Timestamp: time.Now(),
		},
		{
			ID:        "2",
			Type:      eventType(2),
			Timestamp: time.Now(),
		},
		{
			ID:        "3",
			Type:      eventType(3),
			Timestamp: time.Now(),
		},
	}

	allEventIds := []int{1, 2, 3}
	noEventIds := []int{0, 0, 0}

	tcs := []struct {
		name                string
		c                   Consumer
		expConsumeCount     int
		expRecoveredCount   int
		expMetricErrorCount int
		expEventIDs         []int
		expErr              error
	}{
		{
			name:                "Recover Function notes but doesn't change error",
			c:                   NewConsumer(consumerName, cFnErr, WithRecoverFunction(rFnSame)),
			expConsumeCount:     3,
			expRecoveredCount:   3,
			expMetricErrorCount: 3,
			expEventIDs:         allEventIds,
			expErr:              cErr,
		},
		{
			name:                "Recover Function not called when expected error",
			c:                   NewConsumer(consumerName, cFnCanc, WithRecoverFunction(rFnSame)),
			expConsumeCount:     3,
			expRecoveredCount:   0,
			expMetricErrorCount: 0,
			expEventIDs:         noEventIds,
			expErr:              context.Canceled,
		},
		{
			name:                "Recover Function handles error",
			c:                   NewConsumer(consumerName, cFnErr, WithRecoverFunction(rFnNil)),
			expConsumeCount:     3,
			expRecoveredCount:   3,
			expMetricErrorCount: 3,
			expEventIDs:         allEventIds,
			expErr:              nil,
		},
		{
			name:                "Recover Function throws different error",
			c:                   NewConsumer(consumerName, cFnErr, WithRecoverFunction(rFnDiff)),
			expConsumeCount:     3,
			expRecoveredCount:   3,
			expMetricErrorCount: 3,
			expEventIDs:         allEventIds,
			expErr:              rErr,
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			metrics.ConsumerErrors.Reset()
			metrics.ConsumerSkippedEvents.Reset()
			*consumedCounter = 0
			*recoveredCounter = 0
			*eventId = 0

			for i, ev := range evts {
				err := tc.c.Consume(ctx, f, ev)
				require.Equal(t, tc.expEventIDs[i], *eventId)
				jtest.Require(t, tc.expErr, err)
			}

			require.Equal(t, tc.expConsumeCount, *consumedCounter)
			require.Equal(t, tc.expRecoveredCount, *recoveredCounter)
		})
	}
}
