package testmock_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/testmock"
)

type reflexType int

func (rt reflexType) ReflexType() int { return int(rt) }

func TestNewTestStreamer(t *testing.T) {
	ctx := context.Background()
	streamer := testmock.NewTestStreamer(t)

	events := []reflex.Event{
		{
			ID:   "1",
			Type: reflexType(1),
		},
		{
			ID:   "2",
			Type: reflexType(5),
		},
		{
			ID:   "3",
			Type: reflexType(1),
		},
		{
			ID:   "4",
			Type: reflexType(4),
		},
	}
	for _, r := range events {
		streamer.InsertEvent(r)
	}

	streamFunc := streamer.StreamFunc()

	var consumedEvents []reflex.Event
	consumer := reflex.NewConsumer("test", func(ctx context.Context, event *reflex.Event) error {
		consumedEvents = append(consumedEvents, *event)
		return nil
	})
	var cStore cursor
	spec := reflex.NewSpec(
		streamFunc,
		&cStore,
		consumer,
		reflex.WithStreamToHead(),
	)
	err := reflex.Run(ctx, spec)
	jtest.Require(t, reflex.ErrHeadReached, err)

	for i, expectedEvent := range events {
		actualEvent := consumedEvents[i]
		require.Equal(t, expectedEvent, actualEvent)
	}
}

func TestNewTestStreamerOptions(t *testing.T) {
	t.Run("StreamToHead", func(t *testing.T) {
		ctx := context.Background()
		streamer := testmock.NewTestStreamer(t)
		streamer.InsertEvent(reflex.Event{
			ID:   "1",
			Type: reflexType(1),
		})
		streamer.InsertEvent(reflex.Event{
			ID:   "2",
			Type: reflexType(2),
		})

		streamFunc := streamer.StreamFunc()
		streamClient, err := streamFunc(ctx, "", reflex.WithStreamToHead())
		jtest.RequireNil(t, err)

		_, err = streamClient.Recv()
		jtest.RequireNil(t, err)

		_, err = streamClient.Recv()
		jtest.RequireNil(t, err)

		_, err = streamClient.Recv()
		jtest.Require(t, reflex.ErrHeadReached, err)
	})

	t.Run("StreamFromHead", func(t *testing.T) {
		ctx := context.Background()
		streamer := testmock.NewTestStreamer(t)
		streamer.InsertEvent(reflex.Event{
			ID:   "1",
			Type: reflexType(1),
		})
		streamer.InsertEvent(reflex.Event{
			ID:   "2",
			Type: reflexType(2),
		})

		streamFunc := streamer.StreamFunc()
		streamClient, err := streamFunc(ctx, "", reflex.WithStreamFromHead())
		jtest.RequireNil(t, err)

		streamer.InsertEvent(reflex.Event{
			ID:   "3",
			Type: reflexType(3),
		})

		actual, err := streamClient.Recv()
		jtest.RequireNil(t, err)
		expected := reflex.Event{
			ID:   "3",
			Type: reflexType(3),
		}
		require.Equal(t, expected, *actual)
	})
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
