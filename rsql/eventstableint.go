package rsql

import (
	"context"
	"database/sql"
	"strconv"

	"github.com/luno/reflex"
)

func NewEventsTableInt(name string, options ...EventsOption) *EventsTableInt {
	return &EventsTableInt{NewEventsTable(name, options...)}
}

// EventsTableInt wraps reflex EventsTable and provides typed int64 foreign id inserts.
type EventsTableInt struct {
	*EventsTable
}

// Insert works as EventsTable.Insert except that foreign id is an int64.
func (e *EventsTableInt) Insert(ctx context.Context, tx *sql.Tx, foreignID int64,
	typ reflex.EventType) (NotifyFunc, error) {

	return e.EventsTable.Insert(ctx, tx, strconv.FormatInt(foreignID, 10), typ)
}

// InsertWithMetadata works as EventsTable.InsertWithMetadata except
// that foreign id is an int64.
func (e *EventsTableInt) InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID int64,
	typ reflex.EventType, metadata []byte) (NotifyFunc, error) {

	return e.EventsTable.InsertWithMetadata(ctx, tx,
		strconv.FormatInt(foreignID, 10), typ, metadata)
}

// Clone works as EventsTable.Clone.
func (e *EventsTableInt) Clone(opts ...EventsOption) *EventsTableInt {
	return &EventsTableInt{e.EventsTable.Clone(opts...)}
}
