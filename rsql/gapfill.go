package rsql

import (
	"context"
	"database/sql"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
)

// Gap represents a gap in monotonically incrementing events IDs.
// The gap is after previous before next, so if Prev+1==Next,
// then there is no gap.
type Gap struct {
	// Prev(ious) event ID.
	Prev int64

	// Next event ID.
	Next int64
}

// NewGapFiller returns a new gap filler for the events table.
//   Usage:
//   var events = rsql.NewEventsTable()
//   ...
//   events.ListenGaps(req.NewGapFiller(dbc, events).Fill)
func NewGapFiller(dbc *sql.DB, table *EventsTable) *GapFiller {
	return &GapFiller{
		schema: table.schema,
		dbc:    dbc,
	}
}

// GapFiller provides a default implementation for filling gaps
// in an event table. It should be used with EventsTable.ListenGaps.
type GapFiller struct {
	schema etableSchema
	dbc    *sql.DB
}

// Fill ensures that rows exist with the ids indicated by the Gap.
// It does so by either detecting existing rows or by inserting
// noop events. It is idempotent.
func (g GapFiller) Fill(gap Gap) {
	ctx := context.Background()
	for i := gap.Prev + 1; i < gap.Next; i++ {
		err := fillGap(ctx, g.dbc, g.schema, i)
		if err != nil {
			log.Error(ctx, errors.Wrap(err, "errors filling gap", j.MKV{"table": g.schema.name}))
			return
		}
	}
}

// fillGap blocks until an event with id exists (committed) in the table or if it
// could insert a noop event with that id.
func fillGap(ctx context.Context, dbc *sql.DB, schema etableSchema, id int64) error {
	// Wait until the event is committed.
	committed, err := waitCommitted(ctx, dbc, schema, id)
	if err != nil {
		return err
	}
	if committed {
		return nil // Gap filled
	}

	// It does not exists at all, so insert noop.
	_, err = dbc.ExecContext(ctx, "insert into "+schema.name+
		" set id=?, "+schema.foreignIDField+"=0, "+schema.timeField+"=now(), "+
		schema.typeField+"=0", id)
	if isMySQLErrDupEntry(err) {
		// Someone got there first, but that's ok.
		return nil
	}
	return err
}

func exists(ctx context.Context, dbc *sql.DB, schema etableSchema, id int64,
	level sql.IsolationLevel) (bool, error) {

	tx, err := dbc.BeginTx(ctx, &sql.TxOptions{Isolation: level})
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	var exists int
	err = tx.QueryRow("select exists(select 1 from "+schema.name+
		" where id=?)", id).Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists == 1, tx.Commit()
}

// waitCommitted blocks while an uncommitted event with id exists and returns true once
// it is committed or false if it is rolled back or there is no uncommitted event at all.
func waitCommitted(ctx context.Context, dbc *sql.DB, schema etableSchema, id int64) (bool, error) {
	for {
		uncommitted, err := exists(ctx, dbc, schema, id, sql.LevelReadUncommitted)
		if err != nil {
			return false, err
		}

		if !uncommitted {
			return false, nil
		}

		committed, err := exists(ctx, dbc, schema, id, sql.LevelDefault)
		if err != nil {
			return false, err
		}

		if committed {
			return true, nil
		}

		time.Sleep(time.Millisecond * 100) // Don't spin
	}
}
