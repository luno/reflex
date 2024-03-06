package rsql

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"

	"github.com/luno/reflex"
	"github.com/luno/reflex/internal/tracing"
)

const (
	defaultEventIDField        = "id"
	defaultEventTimeField      = "timestamp"
	defaultEventTypeField      = "type"
	defaultEventForeignIDField = "foreign_id"
	defaultMetadataField       = "" // disabled
	defaultTraceField          = "" // default is empty to support backwards compatibility

	defaultErrorTable              = "consumer_errors"
	defaultErrorEventsSuffix       = "_events"
	defaultErrorEventMetadataField = "metadata"
	defaultErrorIDField            = "id"
	defaultErrorEventConsumerField = "consumer"
	defaultErrorEventIDField       = "event_id"
	defaultErrorMsgField           = "error_msg"
	defaultErrorCreatedAtField     = "created_at"
	defaultErrorUpdatedAtField     = "updated_at"
	defaultErrorStatusField        = "error_status"
)

// eventType is the rsql internal implementation of EventType interface.
type eventType int

func (t eventType) ReflexType() int {
	return int(t)
}

// makeDefaultInserter returns the default sql inserter configured via WithEventsXField options.
func makeDefaultInserter(schema eTableSchema) inserter {
	return func(ctx context.Context, tx *sql.Tx,
		foreignID string, typ reflex.EventType, metadata []byte,
	) error {
		q := "insert into " + schema.name +
			" set " + schema.foreignIDField + "=?, " + schema.timeField + "=now(6), " + schema.typeField + "=?"
		args := []interface{}{foreignID, typ.ReflexType()}

		if schema.metadataField != "" {
			q += ", " + schema.metadataField + "=?"
			args = append(args, metadata)
		} else if metadata != nil {
			return errors.New("metadata not enabled")
		}

		spanCtx, hasTrace := tracing.Extract(ctx)
		if schema.traceField != "" && hasTrace {
			traceData, err := tracing.Marshal(spanCtx)
			if err != nil {
				return err
			}

			q += ", " + schema.traceField + "=?"
			args = append(args, traceData)
		}

		_, err := tx.ExecContext(ctx, q, args...)
		return errors.Wrap(err, "insert error")
	}
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
	err := row.Scan(&id, &e.ForeignID, &e.Timestamp, &t, &e.MetaData, &e.Trace)
	if err != nil {
		return nil, err
	}
	e.ID = strconv.FormatInt(id, 10)
	e.Type = t
	return &e, err
}

func getLatestID(ctx context.Context, dbc *sql.DB, schema eTableSchema) (int64, error) {
	var id sql.NullInt64
	q := fmt.Sprintf("select max(%s) from %s", schema.idField, schema.name)
	err := dbc.QueryRowContext(ctx, q).Scan(&id)
	if err != nil {
		return 0, err
	}
	return id.Int64, nil
}

func getNextEvents(ctx context.Context, dbc *sql.DB, schema eTableSchema,
	after int64, lag time.Duration,
) ([]*reflex.Event, error) {
	var (
		q    string
		args []interface{}
	)

	q += "select " + schema.idField + ", " + schema.foreignIDField + ", " + schema.timeField + ", " + schema.typeField
	if schema.metadataField != "" {
		q += " , " + schema.metadataField
	} else {
		q += ", null"
	}

	if schema.traceField != "" {
		q += ", " + schema.traceField
	} else {
		q += ", null"
	}

	q += " from " + schema.name + " where " + schema.idField + ">?"
	args = append(args, after)

	// TODO(corver): Remove support for lag since we now do this at destination.
	if lag > 0 {
		q += " and " + schema.timeField + "<timestamp(now()-interval ? second) "
		args = append(args, lag.Seconds())
	}

	q += " order by " + schema.idField + " asc limit 1000"

	rows, err := dbc.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

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

// GetNextEventsForTesting fetches a bunch of events from the event table
func GetNextEventsForTesting(ctx context.Context, _ *testing.T, dbc *sql.DB, table *EventsTable, after int64, lag time.Duration) ([]*reflex.Event, error) {
	return getNextEvents(ctx, dbc, table.schema, after, lag)
}

// GetLatestIDForTesting fetches the latest event id from the event table
func GetLatestIDForTesting(ctx context.Context, _ *testing.T, dbc *sql.DB, eventTable, idField string) (int64, error) {
	return getLatestID(ctx, dbc, eTableSchema{name: eventTable, idField: idField})
}

// isMySQLErrCantWrite returns true if the error is due to not being able to write
// in this DB instance.
func isMySQLErrCantWrite(err error) bool {
	return isMySQLErrReadOnly(err) || isMySQLErrNoAccess(err)
}

func isMySQLErrDupEntry(err error) bool {
	return isMySQLErr(err, 1062)
}

// isMySQLErrReadOnly returns true if the error is due the DB running in read only mode.
//   - 1290: ER_OPTION_PREVENTS_STATEMENT
func isMySQLErrReadOnly(err error) bool {
	return isMySQLErr(err, 1290)
}

// isMySQLErrNoAccess returns true if the error is due lack of permissions.
//   - 1142: ER_TABLEACCESS_DENIED_ERROR
//   - 1143: ER_COLUMNACCESS_DENIED_ERROR
//   - 1370: ER_PROCACCESS_DENIED_ERROR
func isMySQLErrNoAccess(err error) bool {
	return isMySQLErr(err, 1142, 1143, 1370)
}

// See https://dev.mysql.com/doc/refman/5.6/en/error-messages-server.html#error_er_dup_entry
func isMySQLErr(err error, nums ...uint16) bool {
	if err == nil {
		return false
	}

	me := new(mysql.MySQLError)
	if !errors.As(err, &me) {
		return false
	}

	for _, num := range nums {
		if me.Number == num {
			return true
		}
	}
	return false
}

func getCursor(ctx context.Context, dbc *sql.DB, schema ctableSchema, id string) (string, time.Time, error) {
	var cursor string
	var ts time.Time
	err := dbc.QueryRowContext(ctx, "select "+schema.cursorField+","+schema.timefield+
		" from "+schema.name+" where "+schema.idField+"=?", id).Scan(&cursor, &ts)
	if errors.Is(err, sql.ErrNoRows) {
		return "", time.Time{}, nil
	} else if err != nil {
		return "", time.Time{}, errors.Wrap(err, "query last id error")
	}
	return cursor, ts, nil
}

// setCursor sets the processor's last successfully processed event ID to
// `id`.
func setCursor(ctx context.Context, dbc *sql.DB, schema ctableSchema,
	id string, cursor string,
) error {
	opts := []errors.Option{j.KS("consumer", id), j.KS("cursor", cursor)}

	// 😱: mysql uses "numerical" comparison if you compare a db string to an int.
	c, err := schema.cursorType.Cast(cursor)
	if err != nil {
		return err
	}

	res, err := dbc.ExecContext(ctx, "update "+schema.name+
		" set "+schema.cursorField+"=?, "+schema.timefield+"=now() where "+schema.idField+"=?"+
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
		schema.cursorField+"=?, "+schema.timefield+"=now()", id, c)
	if isMySQLErrDupEntry(err) {
		// Best effort lookup for improved debugging.
		existing, updatedAt, getErr := getCursor(ctx, dbc, schema, id)
		if getErr == nil {
			opts = append(opts, j.MKV{"existing": existing, "updated_at": updatedAt})
		}
		return errors.Wrap(err, "attempted to set cursor <= existing cursor", opts...)
	} else if err != nil {
		return errors.Wrap(err, "insert cursor error", opts...)
	}

	return nil
}

// makeDefaultErrorInserter returns the default sql ErrorInsertFunc configured via WithErrorsXField options.
func makeDefaultErrorInserter(schema errTableSchema) ErrorInserter {
	msg := "insert consumer error failed"
	// TODO(jkilloran): Should we also reset the status to be 1 i.e. EventErrorRecorded status even if it has previously
	//                  been handled/updated to another state. Or should we return any duplicate error in a way so we
	//                  don't write another event off of the same error. Or indeed is it safer as currently written when
	//                  encountering a duplicate error to still write a new event off of it but not to revert the status
	//                  back to recorded.

	// NOTE: This insert statement will return the generated autoincrement "id" column value if no (secondary) key is
	//       already found in the table (i.e. something like consumer + event_id) otherwise it will do a non-op update
	//       but due to the use of last_insert_id(id) it will still pass the existing row's "id" column back as if it
	//       was just inserted ensuring that it always returns a reasonable value.
	// NB: See the documentation is the following link on the behaviour of "on duplicate key update" https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html#:~:text=KEY%20UPDATE%20Statement-,13.2.5.2,-INSERT%20...%20ON%20DUPLICATE
	// NB: See the documentation is the following link on the behaviour of "on last_insert_id(<expr>)" https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
	q := fmt.Sprintf(
		"insert into %s set %s=?, %s=?, %s=?, %s=now(6), %s=now(6), %s=? on duplicate key update %s=last_insert_id(%s)",
		schema.name, schema.eventConsumerField, schema.eventIDField, schema.errorMsgField, schema.errorCreatedAtField, schema.errorUpdatedAtField, schema.errorStatusField, schema.idField, schema.idField)
	return func(ctx context.Context, tx *sql.Tx, consumer string, eventID string, errMsg string, errStatus reflex.ErrorStatus) (string, error) {
		r, err := tx.ExecContext(ctx, q, consumer, eventID, errMsg, errStatus)
		// If the error has already been written then we can ignore the error
		if err != nil && !IsDuplicateErrorInsertion(err) {
			return "", errors.Wrap(err, msg)
		}
		// This will still work with a duplicate due the "on duplicate key update id" part of the insert statement above
		id, idErr := r.LastInsertId()
		if idErr != nil {
			return "", errors.Wrap(idErr, msg)
		}
		return strconv.FormatInt(id, 10), nil
	}
}

func IsDuplicateErrorInsertion(err error) bool {
	return isMySQLErrDupEntry(err)
}

func quoted(name string) string {
	return fmt.Sprintf("`%s`", name)
}
