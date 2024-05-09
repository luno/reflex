package testmock_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/testmock"
)

type reflexType int

func (rt reflexType) ReflexType() int { return int(rt) }

func TestNewTestStreamer(t *testing.T) {
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

	ctx := context.Background()
	streamer := testmock.NewTestStreamer(t)

	for _, r := range events {
		streamer.InsertEvent(r)
	}

	streamFunc := streamer.StreamFunc()
	for i, expectedEvent := range events {
		after := fmt.Sprintf("%d", i)
		streamClient, err := streamFunc(ctx, after)
		jtest.RequireNil(t, err)

		actualEvent, err := streamClient.Recv()
		jtest.RequireNil(t, err)

		require.Equal(t, expectedEvent, *actualEvent)
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
