package rsql

import (
	"context"
	"database/sql"

	"github.com/luno/jettison/errors"

	"github.com/luno/reflex"
	"github.com/luno/reflex/internal/metrics"
)

// ErrorInserter abstracts the insertion of an error into a sql table.
type ErrorInserter func(ctx context.Context, dbc DBC, consumer, eventID, errMsg string, errStatus reflex.ErrorStatus) (string, error)

// ErrorEventInserter abstracts the insertion of an event into a sql table including providing a notification capability.
type ErrorEventInserter func(ctx context.Context, dbc DBC, foreignID string, typ reflex.EventType, metadata []byte) (NotifyFunc, error)

func doNothing() {}

func nullEventInserter(_ context.Context, _ DBC, _ string, _ reflex.EventType, _ []byte) (NotifyFunc, error) {
	return doNothing, nil
}

// NewErrorsTable returns a new event consumer errors table.
func NewErrorsTable(opts ...ErrorsOption) *ErrorsTable {
	table := &ErrorsTable{
		schema: errTableSchema{
			name:                quoted(defaultErrorTable),
			idField:             quoted(defaultErrorIDField),
			eventConsumerField:  quoted(defaultErrorEventConsumerField),
			eventIDField:        quoted(defaultErrorEventIDField),
			errorMsgField:       quoted(defaultErrorMsgField),
			errorCreatedAtField: quoted(defaultErrorCreatedAtField),
			errorUpdatedAtField: quoted(defaultErrorUpdatedAtField),
			errorStatusField:    quoted(defaultErrorStatusField),
		},
		config: opts,
	}

	for _, o := range table.config {
		o(table)
	}

	if table.eventInserter == nil {
		name := defaultErrorTable + defaultErrorEventsSuffix
		opt := WithEventMetadataField(defaultErrorEventMetadataField)
		table.eventInserter = NewEventsTable(name, opt).InsertWithMetadata
	}

	if table.errorInserter == nil {
		table.errorInserter = makeDefaultErrorInserter(table.schema)
	}

	if table.counter == nil {
		table.counter = func(consumer string) {
			metrics.ConsumerDeadLetteredEvents.With(metrics.Labels(consumer)).Inc()
		}
	}

	return table
}

// ErrorsOption defines a functional option to configure new error tables.
type ErrorsOption func(*ErrorsTable)

// WithErrorTableName provides an option to set the name of the consumer error table.
// It defaults to 'consumer_errors'.
func WithErrorTableName(name string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.name = quoted(name)
	}
}

// WithErrorIDField provides an option to set the event DB ID field. This is
// useful for tables which implement custom error loaders. It defaults to 'id'.
func WithErrorIDField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.idField = quoted(field)
	}
}

// WithErrorCreatedAtField provides an option to set the error DB created at timestamp field.
// It defaults to 'created_at'.
func WithErrorCreatedAtField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.errorCreatedAtField = quoted(field)
	}
}

// WithErrorUpdatedAtField provides an option to set the error DB updated at timestamp field.
// It defaults to 'updated_at'.
func WithErrorUpdatedAtField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.errorUpdatedAtField = quoted(field)
	}
}

// WithErrorEventIDField provides an option to set the event DB eventID field.
// It defaults to 'event_id'.
func WithErrorEventIDField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.eventIDField = quoted(field)
	}
}

// WithErrorEventConsumerField provides an option to set the event consumer DB msg field.
// It defaults to 'consumer'.
func WithErrorEventConsumerField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.eventConsumerField = quoted(field)
	}
}

// WithErrorMsgField provides an option to set the error DB msg field.
// It defaults to 'msg'.
func WithErrorMsgField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.errorMsgField = quoted(field)
	}
}

// WithErrorStatusField provides an option to set the error status field.
// It defaults to 'status'.
func WithErrorStatusField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.errorStatusField = quoted(field)
	}
}

// WithErrorInserter provides an option to set the error inserter which
// inserts error into a sql table. The default inserter would be generated
// from the rsql.makeDefaultErrorInserter function parameterised with the
// errTableSchema of the given ErrorsTable.
func WithErrorInserter(inserter ErrorInserter) ErrorsOption {
	return func(table *ErrorsTable) {
		table.errorInserter = inserter
	}
}

// WithErrorEventInserter provides an option to set the error event inserter
// which inserts error into a sql table. The default inserter would be the
// EventsTable.InsertWithMetadata function of a given EventsTable instance.
func WithErrorEventInserter(inserter ErrorEventInserter) ErrorsOption {
	return func(table *ErrorsTable) {
		table.eventInserter = inserter
	}
}

// WithErrorRecordOnly provides an option to set that we only record errors
// and not enable streaming of those errors.
func WithErrorRecordOnly() ErrorsOption {
	return func(table *ErrorsTable) {
		table.eventInserter = nullEventInserter
	}
}

// WithErrorCounter provides an option to set the error counter which
// counts the errors being successfully record to the error table.
func WithErrorCounter(counter func(consumer string)) ErrorsOption {
	return func(table *ErrorsTable) {
		table.counter = counter
	}
}

// ErrorsTable provides reflex consumer event errors insertion and streaming
// for a sql db table.
type ErrorsTable struct {
	config        []ErrorsOption
	schema        errTableSchema
	errorInserter ErrorInserter
	eventInserter ErrorEventInserter
	counter       func(consumer string)
}

func (t *ErrorsTable) ToErrorInsertFunc(dbc *sql.DB) reflex.ErrorInsertFunc {
	msg := "dead lettering error failed"
	errStatus := reflex.EventErrorRecorded
	return func(ctx context.Context, consumer string, eventID string, errMsg string) error {
		tx, err := dbc.BeginTx(ctx, nil)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		// Rollback if we don't commit (if we commit then this becomes a no-op)
		defer func() { _ = tx.Rollback() }()

		// Write the error
		id, err := t.errorInserter(ctx, tx, consumer, eventID, errMsg, errStatus)
		if err != nil {
			return errors.Wrap(err, msg)
		}

		// Write the error event
		nFn, err := t.eventInserter(ctx, tx, id, errStatus, []byte(consumer))
		if err == nil {
			err = tx.Commit()
		}

		if err == nil {
			defer nFn()
			defer func() { t.counter(consumer) }()
		}

		// Note if err == nil this will just return nil as well
		return errors.Wrap(err, msg)
	}
}

// errTableSchema defines the mysql schema of a consumer event errors table.
type errTableSchema struct {
	name                string
	idField             string
	eventConsumerField  string
	eventIDField        string
	errorMsgField       string
	errorCreatedAtField string
	errorUpdatedAtField string
	errorStatusField    string
}
