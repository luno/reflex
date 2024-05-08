package testmock_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
	"github.com/luno/reflex/testmock"
)

type reflexType int

func (rt reflexType) ReflexType() int { return int(rt) }

func TestNewStreamClient(t *testing.T) {
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
	stream := testmock.NewStreamClient(ctx, events...)

	for _, expectedEvent := range events {
		actualEvent, err := stream.Recv()
		jtest.RequireNil(t, err)

		require.Equal(t, expectedEvent, *actualEvent)
	}
}

func TestNewStreamFunc(t *testing.T) {
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
	streamFunc := testmock.NewStreamFunc(events...)

	for i, expectedEvent := range events {
		after := fmt.Sprintf("%d", i)
		streamer, err := streamFunc(ctx, after)
		jtest.RequireNil(t, err)

		actualEvent, err := streamer.Recv()
		jtest.RequireNil(t, err)

		require.Equal(t, expectedEvent, *actualEvent)
	}
}

func ExampleNewStreamClient() {
	eventDate := time.Date(2024, time.May, 8, 0, 0, 0, 0, time.UTC)
	events := []reflex.Event{
		{
			ID:        "1",
			Type:      reflexType(1),
			ForeignID: "1223894672394",
			Timestamp: eventDate,
		},
		{
			ID:        "2",
			Type:      reflexType(5),
			ForeignID: "1223894672394",
			Timestamp: eventDate.Add(time.Hour),
		},
	}

	ctx := context.Background()
	client := testmock.NewStreamClient(ctx, events...)

	event, err := client.Recv()
	if err != nil {
		panic(err)
	}

	fmt.Println(event)

	event, err = client.Recv()
	if err != nil {
		panic(err)
	}

	fmt.Println(event)

	// Output:
	// &{1 1 1223894672394 2024-05-08 00:00:00 +0000 UTC [] []}
	// &{2 5 1223894672394 2024-05-08 01:00:00 +0000 UTC [] []}
}

func ExampleNewStreamFunc() {
	eventDate := time.Date(2024, time.May, 8, 0, 0, 0, 0, time.UTC)
	events := []reflex.Event{
		{
			ID:        "1",
			Type:      reflexType(1),
			ForeignID: "1223894672394",
			Timestamp: eventDate,
		},
		{
			ID:        "2",
			Type:      reflexType(5),
			ForeignID: "1223894672394",
			Timestamp: eventDate.Add(time.Hour),
		},
	}
	streamClient := testmock.NewStreamFunc(events...)

	ctx := context.Background()
	client, err := streamClient(ctx, "")
	if err != nil {
		panic(err)
	}

	event, err := client.Recv()
	if err != nil {
		panic(err)
	}

	fmt.Println(event)

	event, err = client.Recv()
	if err != nil {
		panic(err)
	}

	fmt.Println(event)

	// Output:
	// &{1 1 1223894672394 2024-05-08 00:00:00 +0000 UTC [] []}
	// &{2 5 1223894672394 2024-05-08 01:00:00 +0000 UTC [] []}
}
