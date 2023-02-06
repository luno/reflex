package rsql

import (
	"context"
	"database/sql"
	"time"

	"github.com/luno/reflex"
)

// loader defines a function type for loading events from a sql db.
// It either returns the next available events after prev cursor (exclusive)
// or an error.
// TODO(corver): Remove support for lag since we now do this at destination.
type loader func(ctx context.Context, dbc *sql.DB, prevCursor int64,
	lag time.Duration) (events []*reflex.Event, err error)

// filterLoader defines a function type for loading events from a sql db but
// also supports filtering of whole ranges of events. It either returns the
// next available events after prev cursor (exclusive), or a cursor override
// to use as cursor for the subsequent call, or an error. Event ids are used
// as cursors, so supporting cursor override when no events are returned
// allows skipping ranges of noops events.
//
// Loaders are layered as follows in streamclient.Recv (from outer to inner):
//
//	noopFilter         (filterLoader)
//	rCache (if enable) (loader)
//	gapDetector        (loader)
//	baseLoader         (loader)
//
// TODO(corver): Remove lag since we now do this at destination.
type filterLoader func(ctx context.Context, dbc *sql.DB, prevCursor int64,
	lag time.Duration) (events []*reflex.Event, cursorOverride int64, err error)

// makeBaseLoader returns the default base loader that queries the sql for next events.
// This loader can be replaced with the WithBaseLoader option.
func makeBaseLoader(schema etableSchema) loader {
	return func(ctx context.Context, dbc *sql.DB,
		prevCursor int64, lag time.Duration) ([]*reflex.Event, error) {

		return getNextEvents(ctx, dbc, schema, prevCursor, lag)
	}
}

// wrapFilter default filter that wraps a loader and returns a filterLoader
// does not have logic of its own.
func wrapFilter(loader loader) filterLoader {
	return func(ctx context.Context, dbc *sql.DB,
		prev int64, lag time.Duration) ([]*reflex.Event, int64, error) {

		el, err := loader(ctx, dbc, prev, lag)
		if err != nil {
			return nil, 0, err
		}
		if len(el) == 0 {
			// No new events
			return nil, prev, nil
		}
		return el, 0, nil
	}
}

// wrapNoopFilter returns a filterloader that filters out all noop events returned
// by the provided loader. Noops are required to ensure at-least-once event consistency for
// event streams in the face of long running transactions. Consumers however
// should not have to handle the special noop case. If all events returned
// by loader are noops, it returns the last event id as the cursor override.
func wrapNoopFilter(loader loader) filterLoader {
	return func(ctx context.Context, dbc *sql.DB,
		prev int64, lag time.Duration) ([]*reflex.Event, int64, error) {

		el, err := loader(ctx, dbc, prev, lag)
		if err != nil {
			return nil, 0, err
		}
		if len(el) == 0 {
			// No new events
			return nil, prev, nil
		}
		var res []*reflex.Event
		for _, e := range el {
			if isNoopEvent(e) {
				continue
			}
			res = append(res, e)
		}
		if len(res) == 0 {
			// All events are noops, override cursor.
			return nil, el[len(el)-1].IDInt(), nil
		}
		return res, 0, nil
	}
}

// wrapGapDetector returns a loader that loads monotonically incremental
// events (backed by auto increment int column). All events after `prev` cursor and before any
// gap is returned. Gaps may be permanent, due to rollbacks, or temporary due to uncommitted
// transactions. Detected gaps are sent on the channel.
func wrapGapDetector(loader loader, ch chan<- Gap, name string) loader {
	return func(ctx context.Context, dbc *sql.DB, prev int64,
		lag time.Duration) ([]*reflex.Event, error) {

		el, err := loader(ctx, dbc, prev, lag)
		if err != nil {
			return nil, err
		} else if len(el) == 0 {
			return nil, nil
		}

		for i, e := range el {
			if !e.IsIDInt() {
				return nil, ErrInvalidIntID
			}

			next := e.IDInt()
			if prev != 0 && next != prev+1 {
				eventsBlockingGapGauge.WithLabelValues(name).Set(1)
				// Gap detected, return everything before it.
				eventsGapDetectCounter.WithLabelValues(name).Inc()
				select {
				case ch <- Gap{Prev: prev, Next: next}:
				default:
				}
				return el[:i], nil
			}
			eventsBlockingGapGauge.WithLabelValues(name).Set(0)

			prev = next
		}

		return el, nil
	}
}
