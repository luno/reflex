package rsql

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/luno/reflex"
)

// EventsTableInt wraps reflex EventsTable and provides typed int64 foreign id inserts.
type EventsTableInt interface {
	// Insert works as EventsTable.Insert except that foreign id is an int64.
	Insert(ctx context.Context, tx *sql.Tx, foreignID int64,
		typ reflex.EventType) (NotifyFunc, error)

	// InsertWithMetadata works as EventsTable.InsertWithMetadata except
	// that foreign id is an int64.
	InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID int64,
		typ reflex.EventType, metadata []byte) (NotifyFunc, error)

	// Insert works as EventsTable.ToStream.
	ToStream(dbc *sql.DB, opts ...reflex.StreamOption) reflex.StreamFunc

	// Clone works as EventsTable.Clone.
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

func (e *etableint) InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID int64,
	typ reflex.EventType, metadata []byte) (NotifyFunc, error) {

	return e.EventsTable.InsertWithMetadata(ctx, tx,
		strconv.FormatInt(foreignID, 10), typ, metadata)
}

func (e *etableint) Clone(opts ...EventsOption) EventsTableInt {
	return &etableint{e.EventsTable.Clone(opts...)}
}

// Compile time interface implementation checks
var _ EventsTableInt = new(etableint)
