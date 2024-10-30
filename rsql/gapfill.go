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

// FillGaps registers the default gap filler with the events table. It
// inserts noops into the events table when gaps are detected. Both
// EventsTable and EventsTableInt satisfy the gapTable internal interface.
//
//	Usage:
//	var events = rsql.NewEventsTable()
//	...
//	rsql.FillGaps(dbc, events)
func FillGaps(dbc *sql.DB, gapTable gapTable) {
	gapTable.ListenGaps(makeFill(dbc, gapTable.getSchema()))
}

// StopFillingGaps stops any goroutine started from FillGaps
// If FillGaps has not been called, then this will block until ctx is cancelled or deadline is reached
func StopFillingGaps(ctx context.Context, gapTable gapTable) {
	gapTable.StopGapListener(ctx)
}

// gapTable is a common interface between EventsTable and EventsTableInt
// defining the subset of methods required for gap filling.
type gapTable interface {
	ListenGaps(listenFunc GapListenFunc)
	StopGapListener(ctx context.Context)
	getSchema() eTableSchema
}

// makeFill returns a fill function that ensures that rows exist
// with the ids indicated by the Gap. It does so by either detecting
// existing rows or by inserting noop events. It is idempotent.
func makeFill(dbc *sql.DB, schema eTableSchema) func(Gap) {
	return func(gap Gap) {
		ctx := context.Background()
		for i := gap.Prev + 1; i < gap.Next; i++ {
			err := fillGap(ctx, dbc, schema, i)
			if err != nil {
				log.Error(ctx, errors.Wrap(err, "errors filling gap",
					j.MKV{"table": schema.name, "id": i}))
				return
			}
		}
	}
}

// fillGap blocks until an event with id exists (committed) in the table or if it
// could insert a noop event with that id.
func fillGap(ctx context.Context, dbc *sql.DB, schema eTableSchema, id int64) error {
	// Wait until the event is committed.
	committed, err := waitCommitted(ctx, dbc, schema, id)
	if err != nil {
		return err
	}
	if committed {
		return nil // Gap already filled
	}

	// It does not exists at all, so insert noop.
	_, err = dbc.ExecContext(ctx, "insert into "+schema.name+
		" set id=?, "+schema.foreignIDField+"=0, "+schema.timeField+"=now(), "+
		schema.typeField+"=0", id)
	if isMySQLErrDupEntry(err) {
		// Someone got there first, but that's ok.
		return nil
	} else if err != nil {
		return err
	}

	eventsGapFilledCounter.WithLabelValues(schema.name).Inc()

	return nil
}

func exists(ctx context.Context, dbc *sql.DB, schema eTableSchema, id int64,
	level sql.IsolationLevel,
) (bool, error) {
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
func waitCommitted(ctx context.Context, dbc *sql.DB, schema eTableSchema, id int64) (bool, error) {
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
