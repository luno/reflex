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
	"github.com/stretchr/testify/assert"
)

type stream int64
type sType int

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

	// Make sure there's no metrics at the end
	assert.Len(t, fetchMetrics(), 0)

	idToFail++

	// Run again to make sure we can re-use the consumer
	_ = Run(ctx, spec)
	v, err = store.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "2", v)

	assert.Len(t, fetchMetrics(), 0)
}

func fetchMetrics() []prometheus.Metric {
	ch := make(chan prometheus.Metric, 100)
	consumerLag.Collect(ch)
	close(ch)
	ret := make([]prometheus.Metric, 0, len(ch))
	for m := range ch {
		ret = append(ret, m)
	}
	return ret
}
