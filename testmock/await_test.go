package testmock_test

import (
	"context"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/luno/reflex/testmock"
)

func TestAwait(t *testing.T) {
	streamer := testmock.NewTestStreamer(t)
	defer streamer.Stop()

	streamer.InsertEvent(reflex.Event{
		ID:        "1",
		Type:      reflexType(1),
		ForeignID: "9",
		Timestamp: time.Now(),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	cStore := rpatterns.MemCursorStore()

	s := ""
	result := &s
	consumer := reflex.NewConsumer("my-consumer", func(ctx context.Context, event *reflex.Event) error {
		*result = "Hello World"
		return nil
	})

	spec := reflex.NewSpec(streamer.StreamFunc(), cStore, consumer, reflex.WithStreamToHead())
	go func() {
		err := reflex.Run(ctx, spec)
		jtest.Require(t, reflex.ErrHeadReached, err)
	}()

	testmock.AwaitConsumer(t, cStore, "my-consumer", 1)

	require.Equal(t, "Hello World", *result)
}
