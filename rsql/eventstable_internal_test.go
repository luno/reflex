package rsql

import (
	"context"
	"database/sql"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"

	"github.com/luno/reflex"
)

type EventTable struct {
	mu     sync.Mutex
	Events []*reflex.Event
}

func InitTable(inEvents []*reflex.Event) *EventTable {
	start := inEvents[0].IDInt()
	end := inEvents[len(inEvents)-1].IDInt()
	ev := make([]*reflex.Event, end-start+1)
	var inIdx int
	for i := range ev {
		if inEvents[inIdx].IDInt() != start+int64(i) {
			continue
		}
		ev[i] = inEvents[inIdx]
		inIdx++
	}
	return &EventTable{Events: ev}
}

func (t *EventTable) FillGap(g Gap) {
	t.mu.Lock()
	defer t.mu.Unlock()
	from := slices.IndexFunc(t.Events, func(event *reflex.Event) bool {
		return event != nil && event.ID == strconv.FormatInt(g.Prev, 10)
	})
	to := slices.IndexFunc(t.Events, func(event *reflex.Event) bool {
		return event != nil && event.ID == strconv.FormatInt(g.Next, 10)
	})
	if from == -1 || to == -1 {
		panic("cant identify gap to fill")
	}
	id := g.Prev + 1
	for i := from + 1; i < to; i++ {
		t.Events[i] = &reflex.Event{
			ID:        strconv.FormatInt(id, 10),
			Type:      eventType(1),
			ForeignID: "123",
			Timestamp: time.Unix(int64(i), 0),
		}
		id++
	}
}

func (t *EventTable) GetEvents(after int64) []*reflex.Event {
	t.mu.Lock()
	defer t.mu.Unlock()
	cpy := make([]*reflex.Event, len(t.Events))
	copy(cpy, t.Events)
	return slices.DeleteFunc(cpy, func(event *reflex.Event) bool {
		return event == nil || event.IDInt() <= after
	})
}

func (t *EventTable) Len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.Events)
}

func TestGapFiller(t *testing.T) {
	evTable := InitTable([]*reflex.Event{
		{
			ID: "100", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(1, 0),
		},
		{
			ID: "101", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(2, 0),
		},
		{
			ID: "103", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(3, 0),
		},
		{
			ID: "104", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(5, 0),
		},
	})

	loader := func(ctx context.Context, dbc *sql.DB, prevCursor int64, lag time.Duration) ([]*reflex.Event, error) {
		return evTable.GetEvents(prevCursor), nil
	}

	table := NewEventsTable("test", WithEventsLoader(loader), WithEventsBackoff(time.Millisecond))
	table.ListenGaps(func(gap Gap) {
		evTable.FillGap(gap)
	})

	toStream := table.ToStream(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	cli, err := toStream(ctx, "")
	jtest.RequireNil(t, err)

	for i := 0; i < evTable.Len(); i++ {
		_, err := cli.Recv()
		jtest.RequireNil(t, err, "event", i)
	}

	_, err = cli.Recv()
	jtest.Assert(t, context.DeadlineExceeded, err)

	table.StopGapListener(context.Background())
}

func TestGapFillerStopped(t *testing.T) {
	evTable := InitTable([]*reflex.Event{
		{
			ID: "100", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(1, 0),
		},
		{
			ID: "101", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(2, 0),
		},
		{
			ID: "103", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(3, 0),
		},
		{
			ID: "104", Type: eventType(1),
			ForeignID: "123", Timestamp: time.Unix(5, 0),
		},
	})

	loader := func(ctx context.Context, dbc *sql.DB, prevCursor int64, lag time.Duration) ([]*reflex.Event, error) {
		return evTable.GetEvents(prevCursor), nil
	}

	table := NewEventsTable("test", WithEventsLoader(loader), WithEventsBackoff(time.Millisecond))
	table.ListenGaps(func(gap Gap) {
		evTable.FillGap(gap)
	})

	toStream := table.ToStream(nil)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	cli, err := toStream(ctx, "")
	jtest.RequireNil(t, err)

	table.StopGapListener(context.Background())

	_, err = cli.Recv()
	jtest.RequireNil(t, err)
	_, err = cli.Recv()
	jtest.RequireNil(t, err)

	// Gap never filled, time out
	_, err = cli.Recv()
	jtest.Assert(t, context.DeadlineExceeded, err)
}

func TestStopGapFillerCanBeCancelled(t *testing.T) {
	table := NewEventsTable("test")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)

	// This will deadlock without some other await
	table.StopGapListener(ctx)
}
