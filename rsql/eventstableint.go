package rsql

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/luno/reflex"
)

// EventsTableInt wraps reflex EventsTable and provides typed int64 foreign id inserts.
type EventsTableInt interface {
	Insert(ctx context.Context, tx *sql.Tx, foreignID int64,
		typ reflex.EventType) (NotifyFunc, error)
	ToStream(dbc *sql.DB, opts ...reflex.StreamOption) reflex.StreamFunc
	Clone(opts ...EventsOption) EventsTableInt
}

func NewEventsTableInt(name string, options ...EventsOption) EventsTableInt {
	return &etableint{NewEventsTable(name, options...)}
}

type etableint struct {
	EventsTable
}

func (e *etableint) Insert(ctx context.Context, tx *sql.Tx, foreignID int64,
	typ reflex.EventType) (NotifyFunc, error) {

	return e.EventsTable.Insert(ctx, tx, strconv.FormatInt(foreignID, 10), typ)
}

func (e *etableint) Clone(opts ...EventsOption) EventsTableInt {
	return &etableint{e.EventsTable.Clone(opts...)}
}

// Compile time interface implementation checks
var _ EventsTableInt = new(etableint)
