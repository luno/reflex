package rsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/jtest"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

var (
	duplicateErr = errors.New("insert consumer error failed", j.C("ERR_78adcaf0b507391b"))
	cases        = []struct {
		name     string
		consumer []string
		event    []string
		errMsg   []string
		errors   []error
	}{
		{
			name:     "basic - single consumer - single message",
			consumer: []string{"consumer", "consumer", "consumer", "consumer"},
			event:    []string{"1", "2", "3", "4"},
			errMsg:   []string{"message", "message", "message", "message"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name:     "basic - multiple consumer - single message",
			consumer: []string{"consumer A", "consumer B", "consumer C", "consumer D"},
			event:    []string{"1", "2", "3", "4"},
			errMsg:   []string{"message", "message", "message", "message"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name:     "basic - single consumer - multiple message",
			consumer: []string{"consumer", "consumer", "consumer", "consumer"},
			event:    []string{"1", "2", "3", "4"},
			errMsg:   []string{"message 1", "message 2", "message 3", "message 4"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name:     "basic - multiple consumer - multiple message",
			consumer: []string{"consumer A", "consumer B", "consumer C", "consumer D"},
			event:    []string{"1", "2", "3", "4"},
			errMsg:   []string{"message 1", "message 2", "message 3", "message 4"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name: "nothing",
		},
		{
			name:     "duplicate - consumer - event",
			consumer: []string{"consumer", "consumer", "consumer", "consumer"},
			event:    []string{"1", "1", "1", "1"},
			errMsg:   []string{"message 1", "message 2", "message 3", "message 4"},
			errors:   []error{nil, duplicateErr, duplicateErr, duplicateErr},
		},
		{
			name:     "duplicate - consumer - errMsg",
			consumer: []string{"consumer", "consumer", "consumer", "consumer"},
			event:    []string{"1", "2", "3", "4"},
			errMsg:   []string{"message", "message", "message", "message"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name:     "duplicate - event - errMsg",
			consumer: []string{"consumer 1", "consumer 2", "consumer 3", "consumer 4"},
			event:    []string{"1", "1", "1", "1"},
			errMsg:   []string{"message", "message", "message", "message"},
			errors:   []error{nil, nil, nil, nil},
		},
		{
			name:     "duplicate - consumer - event - errMsg",
			consumer: []string{"consumer", "consumer", "consumer", "consumer"},
			event:    []string{"1", "1", "1", "1"},
			errMsg:   []string{"message", "message", "message", "message"},
			errors:   []error{nil, duplicateErr, duplicateErr, duplicateErr},
		},
	}
)

func TestDefaultErrorsTable(t *testing.T) {
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())
			table := rsql.NewErrorsTable()
			dFn := table.ToDeadLetterFunc(dbc)

			for i := 0; i < len(test.event); i++ {
				err := dFn(context.Background(), test.consumer[i], test.event[i], test.errMsg[i])
				// Duplicates are ignored when writing errors
				jtest.RequireNil(t, err)
			}
		})
	}
}

func TestErrorsTable(t *testing.T) {
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable(), DefaultErrorEventTable())

			table := rsql.NewErrorsTable(
				rsql.WithErrorTableName(errorsTable),
				rsql.WithErrorEventConsumerField("consumer"),
				rsql.WithErrorIDField("id"),
				rsql.WithErrorEventIDField("event_id"),
				rsql.WithErrorMsgField("error_msg"),
				rsql.WithErrorTimeField("timestamp"),
				rsql.WithErrorStatusField("status"),
				rsql.WithErrorInserter(
					func(ctx context.Context, tx *sql.Tx, consumer string, eventID string, errMsg string, errStatus reflex.ErrorStatus) (string, error) {
						msg := "insert consumer error failed"
						// TODO(jkilloran): Should we also reset the status to be 1 i.e. EventErrorRecorded status even if it has previously
						//                  been handled/updated to another state. Or should we return any duplicate error in a way so we
						//                  don't write another event off of the same error. Or indeed is it safer as currently written when
						//                  encountering a duplicate error to still write a new event off of it but not to revert the status
						//                  back to recorded.
						//                  [Repeated here since this code is a copy of the private function rsql.makeDefaultErrorInserter for testing purposes]

						// NOTE: This insert statement will return the generated autoincrement "id" column value if no (secondary) key is
						//       already found in the table (i.e. something like consumer + event_id) otherwise it will do a non-op update
						//       but due to the use of last_insert_id(id) it will still pass the existing row's "id" column back as if it
						//       was just inserted ensuring that it always returns a reasonable value.
						// NB: See the documentation is the following link on the behaviour of "on duplicate key update" https://dev.mysql.com/doc/refman/5.7/en/insert-on-duplicate.html#:~:text=KEY%20UPDATE%20Statement-,13.2.5.2,-INSERT%20...%20ON%20DUPLICATE
						// NB: See the documentation is the following link on the behaviour of "on last_insert_id(<expr>)" https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id
						q := fmt.Sprintf(
							"insert into %s set %s=?, %s=?, %s=?, %s=now(6), %s=? on duplicate key update id=last_insert_id(id)",
							errorsTable, "consumer", "event_id", "error_msg", "timestamp", "status")
						r, err := tx.ExecContext(ctx, q, consumer, eventID, errMsg, errStatus)
						// If the error has already been written then we can ignore the error
						if err != nil && !rsql.IsDuplicateErrorInsertion(err) {
							return "", errors.Wrap(err, msg)
						}
						// This will still work with a duplicate due the "on duplicate key update id" part of the insert statement above
						id, idErr := r.LastInsertId()
						if idErr != nil {
							return "", errors.Wrap(idErr, msg)
						}
						return strconv.FormatInt(id, 10), errors.Wrap(err, msg)
					}),
				rsql.WithErrorCounter(func(_ string) {}),
				rsql.WithErrorEventInserter(rsql.NewEventsTable(errorsTable+"_events", rsql.WithEventMetadataField("metadata")).InsertWithMetadata),
			)
			dFn := table.ToDeadLetterFunc(dbc)

			for i := 0; i < len(test.event); i++ {
				err := dFn(context.Background(), test.consumer[i], test.event[i], test.errMsg[i])
				jtest.RequireNil(t, err)
			}
		})
	}
}
