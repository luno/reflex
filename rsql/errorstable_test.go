package rsql_test

import (
	"context"
	"database/sql"
	"fmt"
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
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable())

			table := rsql.NewErrorsTable()

			for i := 0; i < len(test.event); i++ {
				err := table.Insert(context.Background(), dbc, test.consumer[i], test.event[i], test.errMsg[i])
				// Duplicates are ignored when writing errors
				jtest.RequireNil(t, err)
			}
		})
	}
}

func TestErrorsTable(t *testing.T) {
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable())

			table := rsql.NewErrorsTable(
				rsql.WithErrorTableName(errorsTable),
				rsql.WithErrorEventConsumerField("consumer"),
				rsql.WithErrorIDField("id"),
				rsql.WithErrorEventIDField("event_id"),
				rsql.WithErrorMsgField("error_msg"),
				rsql.WithErrorTimeField("timestamp"),
				rsql.WithErrorStatusField("status"),
				rsql.WithErrorInserter(
					func(ctx context.Context, dbc *sql.DB, consumer string, eventID string, errMsg string, errStatus reflex.ErrorStatus) error {
						q := fmt.Sprintf("insert into %s set %s=?, %s=?, %s=?, %s=now(6), %s=?",
							errorsTable, "consumer", "event_id", "error_msg", "timestamp", "status")
						_, err := dbc.ExecContext(ctx, q, consumer, eventID, errMsg, errStatus)
						if rsql.IsDuplicateErrorInsertion(err) {
							return nil
						}
						return errors.Wrap(err, "insert consumer error failed")
					},
				),
				rsql.WithErrorCounter(func(_ string) {}),
			)

			for i := 0; i < len(test.event); i++ {
				err := table.Insert(context.Background(), dbc, test.consumer[i], test.event[i], test.errMsg[i])
				jtest.RequireNil(t, err)
			}
		})
	}
}

func TestErrorInserter(t *testing.T) {
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable())

			inserter := rsql.NewErrorsTable().ErrorInserter(dbc)

			for i := 0; i < len(test.event); i++ {
				err := inserter(context.Background(), test.consumer[i], test.event[i], test.errMsg[i])
				// Duplicates are ignored when writing errors
				jtest.RequireNil(t, err)
			}
		})
	}
}
