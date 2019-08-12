package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
)

const (
	defaultEventTimeField      = "timestamp"
	defaultEventTypeField      = "type"
	defaultEventForeignIDField = "foreign_id"
	defaultMetadataField       = "" // disabled
)

// eventType is the rsql internal implementation of EventType interface.
type eventType int

func (t eventType) ReflexType() int {
	return int(t)
}

func insertEvent(ctx context.Context, tx *sql.Tx, schema etableSchema,
	foreignID string, typ reflex.EventType, metadata []byte) error {

	q := "insert into " + schema.name +
		" set " + schema.foreignIDField + "=?, " + schema.timeField + "=now(), " + schema.typeField + "=?"
	args := []interface{}{foreignID, typ.ReflexType()}

	if schema.metadataField != "" {
		q += ", " + schema.metadataField + "=?"
		args = append(args, metadata)
	} else if metadata != nil {
		return errors.New("metadata not enabled")
	}

	_, err := tx.ExecContext(ctx, q, args...)
	return errors.Wrap(err, "insert error")
}

type row interface {
	Scan(dest ...interface{}) error
}

func scan(row row) (*reflex.Event, error) {
	var (
		e  reflex.Event
		id int64
		t  eventType
	)
	err := row.Scan(&id, &e.ForeignID, &e.Timestamp, &t, &e.MetaData)
	if err != nil {
		return nil, err
	}
	e.ID = strconv.FormatInt(id, 10)
	e.Type = t
	return &e, err
}

func getLatestID(ctx context.Context, dbc *sql.DB, schema etableSchema) (int64, error) {
	var id sql.NullInt64
	err := dbc.QueryRowContext(ctx, "select max(id) from "+schema.name).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id.Int64, nil
}

func getNextEvents(ctx context.Context, dbc *sql.DB, schema etableSchema,
	after int64, lag time.Duration) ([]*reflex.Event, error) {

	var (
		q    string
		args []interface{}
	)

	q += "select id, " + schema.foreignIDField + ", " + schema.timeField + ", " + schema.typeField
	if schema.metadataField != "" {
		q += " , " + schema.metadataField
	} else {
		q += ", null"
	}

	q += " from " + schema.name + " where id>?"
	args = append(args, after)

	if lag > 0 {
		q += " and " + schema.timeField + "<timestamp(now()-interval ? second) "
		args = append(args, lag.Seconds())
	}

	q += " order by id asc limit 1000"

	rows, err := dbc.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var el []*reflex.Event
	for rows.Next() {
		batch, err := scan(rows)
		if err != nil {
			return nil, err
		}

		el = append(el, batch)
	}

	return el, rows.Err()
}

func GetNextEventsForTesting(t *testing.T, ctx context.Context, dbc *sql.DB,
	table EventsTable, after int64, lag time.Duration) ([]*reflex.Event, error) {
	et, ok := table.(*etable)
	if !ok {
		t.Fatalf("invalid EventsTable type=%T", table)
	}
	return getNextEvents(ctx, dbc, et.schema, after, lag)
}

func GetLatestIDForTesting(t *testing.T, ctx context.Context, dbc *sql.DB, eventTable string) (int64, error) {
	return getLatestID(ctx, dbc, etableSchema{name: eventTable})
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

// isMySQLErrCantWrite returns true if the error is due to not being able to write
// in this DB instace.
func isMySQLErrCantWrite(err error) bool {
	return isMySQLErrReadOnly(err) || isMySQLErrNoAccess(err)
}

func isMySQLErrDupEntry(err error) bool {
	return isMySQLErr(err, 1062)
}

// isMySQLErrReadOnly returns true if the error is due the DB running in read only mode.
//  - 1290: ER_OPTION_PREVENTS_STATEMENT
func isMySQLErrReadOnly(err error) bool {
	return isMySQLErr(err, 1290)
}

// isMySQLErrNoAccess returns true if the error is due lack of permissions.
//  - 1142: ER_TABLEACCESS_DENIED_ERROR
//  - 1143: ER_COLUMNACCESS_DENIED_ERROR
//  - 1370: ER_PROCACCESS_DENIED_ERROR
func isMySQLErrNoAccess(err error) bool {
	return isMySQLErr(err, 1142, 1143, 1370)
}

// See https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html#error_er_dup_entry
func isMySQLErr(err error, nums ...uint16) bool {
	if err == nil {
		return false
	}
	me, ok := err.(*mysql.MySQLError)
	if !ok || me == nil {
		return false
	}

	for _, num := range nums {
		if me.Number == num {
			return true
		}
	}
	return false
}

func getCursor(ctx context.Context, dbc *sql.DB, schema ctableSchema, id string) (string, error) {
	var cursor string
	err := dbc.QueryRowContext(ctx, "select "+schema.cursorField+
		" from "+schema.name+" where "+schema.idField+"=?", id).Scan(&cursor)
	if err == sql.ErrNoRows {
		return "", nil
	} else if err != nil {
		return "", errors.Wrap(err, "query last id error")
	}
	return cursor, nil
}

// setCursor sets the processor's last successfully processed event ID to
// `id`.
func setCursor(ctx context.Context, dbc *sql.DB, schema ctableSchema,
	id string, cursor string) error {
	opts := []jettison.Option{j.KS("consumer", id), j.KS("cursor", cursor)}

	// 😱: mysql uses "numerical" comparison if you compare a db string to an int.
	c, err := schema.cursorType.Cast(cursor)
	if err != nil {
		return err
	}

	res, err := dbc.ExecContext(ctx, "update "+schema.name+
		" set "+schema.cursorField+"=?, updated_at=now() where "+schema.idField+"=?"+
		" and "+schema.cursorField+"<?",
		c, id, c)
	if err != nil {
		return errors.Wrap(err, "set cursor error", opts...)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return errors.Wrap(err, "rows affected error", opts...)
	} else if rows > 1 {
		return errors.New("invalid rows affected error", opts...)
	} else if rows == 1 {
		// done
		return nil
	}

	// Insert since rows == 0
	_, err = dbc.ExecContext(ctx, "insert into "+schema.name+" set "+schema.idField+"=?, "+
		schema.cursorField+"=?, updated_at=now()", id, c)
	if isMySQLErrDupEntry(err) {
		return errors.Wrap(err, "attempted to set cursor <= existing cursor", opts...)
	} else if err != nil {
		return errors.Wrap(err, "insert cursor error", opts...)
	}

	return nil
}
