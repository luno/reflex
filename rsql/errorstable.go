package rsql

import (
	"context"
	"database/sql"

	"github.com/luno/reflex"
	"github.com/luno/reflex/internal/metrics"
)

// NewErrorsTable returns a new event consumer errors table.
func NewErrorsTable(opts ...ErrorsOption) *ErrorsTable {
	table := &ErrorsTable{
		schema: errTableSchema{
			name:               quoted(defaultErrorTable),
			idField:            quoted(defaultErrorIDField),
			eventConsumerField: quoted(defaultErrorEventConsumerField),
			eventIDField:       quoted(defaultErrorEventIDField),
			errorMsgField:      quoted(defaultErrorMsgField),
			errorTimeField:     quoted(defaultErrorTimeField),
			errorStatusField:   quoted(defaultErrorStatusField),
		},
		config: opts,
	}

	for _, o := range table.config {
		o(table)
	}

	if table.inserter == nil {
		table.inserter = makeDefaultErrorInserter(table.schema)
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

// WithErrorTimeField provides an option to set the error DB timestamp field.
// It defaults to 'timestamp'.
func WithErrorTimeField(field string) ErrorsOption {
	return func(table *ErrorsTable) {
		table.schema.errorTimeField = quoted(field)
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

// WithErrorInserter provides an option to set the error inserter
// which inserts error into a sql table. The default inserter is
// configured with the WithErrorsXField options.
func WithErrorInserter(inserter errorInserter) ErrorsOption {
	return func(table *ErrorsTable) {
		table.inserter = inserter
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
	config   []ErrorsOption
	schema   errTableSchema
	inserter errorInserter
	counter  func(consumer string)
}

// Insert inserts a consumer error into the ErrorsTable.
func (t *ErrorsTable) Insert(ctx context.Context, dbc *sql.DB, consumer string, eventID string, errMsg string) error {
	err := t.inserter(ctx, dbc, consumer, eventID, errMsg, reflex.SavedEventError)
	if err == nil {
		t.counter(consumer)
	}
	return err
}

func (t *ErrorsTable) ErrorInserter(dbc *sql.DB) reflex.ErrorInsertFunc {
	return func(ctx context.Context, consumer string, eventID string, errMsg string) error {
		return t.Insert(ctx, dbc, consumer, eventID, errMsg)
	}
}

// errTableSchema defines the mysql schema of a consumer event errors table.
type errTableSchema struct {
	name               string
	idField            string
	eventConsumerField string
	eventIDField       string
	errorMsgField      string
	errorTimeField     string
	errorStatusField   string
}

// errorInserter abstracts the insertion of an error into a sql table.
type errorInserter func(ctx context.Context, dbc *sql.DB, consumer string, eventID string, errMsg string, errStatus reflex.ErrorStatus) error
