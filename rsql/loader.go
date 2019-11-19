package rsql

import (
	"context"
	"database/sql"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
)

// Loader defines a function type for loading events from a sql db.
// It returns the next available events and the associated next cursor
// after the previous cursor or an error.
// Loaders are layered as follows (from outer to inner):
//   noopFilter
//   rCache (if enable)
//   gapDetector
//   baseLoader
type Loader func(ctx context.Context, dbc *sql.DB, prevCursor int64,
	lag time.Duration) ([]*reflex.Event, int64, error)

// makeBaseLoader returns the default base loader that queries the sql for next events.
// This loader can be replaced with the WithBaseLoader option.
func makeBaseLoader(schema etableSchema) Loader {
	return func(ctx context.Context, dbc *sql.DB,
		prevCursor int64, lag time.Duration) ([]*reflex.Event, int64, error) {

		el, err := getNextEvents(ctx, dbc, schema, prevCursor, lag)
		if err != nil {
			return nil, 0, err
		}
		if len(el) == 0 {
			return nil, prevCursor, nil
		}

		last := el[len(el)-1]
		if !last.IsIDInt() {
			return nil, 0, ErrInvalidIntID
		}

		return el, last.IDInt(), nil
	}
}

// wrapNoopFilter returns a loader that filters out all noop events returned
// by the provided loader. Noops are required to ensure at-least-once event consistency for
// event streams in the face of long running transactions. Consumers however
// should not have to handle the special noop case.
func wrapNoopFilter(loader Loader) Loader {
	return func(ctx context.Context, dbc *sql.DB,
		prev int64, lag time.Duration) ([]*reflex.Event, int64, error) {

		el, next, err := loader(ctx, dbc, prev, lag)
		if err != nil {
			return nil, 0, err
		}
		if len(el) == 0 {
			// No new events
			return nil, next, nil
		}
		var res []*reflex.Event
		for _, e := range el {
			if isNoopEvent(e) {
				continue
			}
			res = append(res, e)
		}
		return res, next, nil
	}
}

// wrapGapDetector returns a loader that loads monotonically incremental
// events (backed by auto increment int column). All events after `prev` cursor and before any
// gap is returned. Gaps may be permanent, due to rollbacks, or temporary due to uncommitted
// transactions. Detected gaps are sent on the channel.
func wrapGapDetector(loader Loader, ch chan<- Gap, name string) Loader {
	return func(ctx context.Context, dbc *sql.DB, prev int64,
		lag time.Duration) ([]*reflex.Event, int64, error) {

		el, next, err := loader(ctx, dbc, prev, lag)
		if err != nil {
			return nil, 0, err
		} else if len(el) == 0 {
			return nil, prev, nil
		}

		// Sanity check: Last event ID should match next cursor since it will be ignored.
		last := el[len(el)-1]
		if !last.IsIDInt() {
			return nil, 0, ErrInvalidIntID
		} else if last.IDInt() != next {
			return nil, 0, errors.Wrap(ErrNextCursorMismatch, "",
				j.MKV{"next": next, "last": last.IDInt()})
		}

		id0 := prev
		for i, e := range el {
			if !e.IsIDInt() {
				return nil, 0, ErrInvalidIntID
			}

			id1 := e.IDInt()
			if id0 != 0 && id1 != id0+1 {
				// Gap detected, return everything before it.
				eventsGapDetectCounter.WithLabelValues(name)
				select {
				case ch <- Gap{Prev: id0, Next: id1}:
				default:
				}
				return el[:i], id0, nil
			}

			id0 = id1
		}

		return el, id0, nil
	}
}
