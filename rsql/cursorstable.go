package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// CursorType is either int or string
type CursorType int

// Cast returns cursor casted to type.
func (t CursorType) Cast(cursor string) (interface{}, error) {
	if t == cursorTypeString {
		return cursor, nil
	}
	if t != cursorTypeInt {
		return nil, errors.New("unsupported cursor type")
	}
	i, err := strconv.ParseInt(cursor, 10, 64)
	if err != nil {
		return nil, errors.New("invalid int cursor")
	}
	return i, nil
}

const (
	cursorTypeUnknown CursorType = 0
	cursorTypeInt     CursorType = 1
	cursorTypeString  CursorType = 2

	defaultCursorCursorField = "last_event_id"
	defaultCursorIDField     = "id"
	defaultCursorTimeField   = "updated_at"
	defaultAsyncPeriod       = time.Second * 5
)

// CursorsTable provides an interface to an event consumer cursors db table.
type CursorsTable interface {
	GetCursor(ctx context.Context, dbc *sql.DB, consumerID string) (string, error)
	SetCursor(ctx context.Context, dbc *sql.DB, consumerID string, cursor string) error
	Flush(ctx context.Context) error
	Clone(ol ...CursorsOption) CursorsTable
	ToStore(dbc *sql.DB, ol ...CursorsOption) reflex.CursorStore
}

// NewCursorsTable returns a new CursorsTable implementation.
func NewCursorsTable(name string, options ...CursorsOption) CursorsTable {
	table := &ctable{
		schema: ctableSchema{
			name:        name,
			cursorField: defaultCursorCursorField,
			idField:     defaultCursorIDField,
			timefield:   defaultCursorTimeField,
			cursorType:  cursorTypeInt,
		},
		sleep:       time.Sleep,
		setCounter:  makeCursorSetCounter(name),
		asyncPeriod: defaultAsyncPeriod,
	}
	for _, o := range options {
		o(table)
	}

	return table
}

// CursorsOption are the configurations for the cursor table
type CursorsOption func(*ctable)

// WithCursorCursorField provides an option to configure the cursor field.
// It defaults to 'last_event_id'.
func WithCursorCursorField(field string) CursorsOption {
	return func(table *ctable) {
		table.schema.cursorField = field
	}
}

// WithCursorIDField provides an option to configure the cursor ID field.
// It defaults to 'id'.
func WithCursorIDField(field string) CursorsOption {
	return func(table *ctable) {
		table.schema.idField = field
	}
}

// WithCursorTimeField provides an option to configure the cursor time field.
// It defaults to 'updated_at'.
func WithCursorTimeField(field string) CursorsOption {
	return func(table *ctable) {
		table.schema.timefield = field
	}
}

// WithCursorAsyncPeriod provides an option to configure the async write period.
// It defaults to 5 seconds.
func WithCursorAsyncPeriod(d time.Duration) CursorsOption {
	return func(table *ctable) {
		table.asyncPeriod = d
	}
}

// WithCursorAsyncDisabled provides an option to disable async writes.
func WithCursorAsyncDisabled() CursorsOption {
	return WithCursorAsyncPeriod(0)
}

// WithCursorSetCounter provides an option to set the cursor DB set cursor metric.
// It defaults to prometheus metrics.
func WithCursorSetCounter(f func()) CursorsOption {
	return func(table *ctable) {
		table.setCounter = f
	}
}

// WithCursorStrings provides an option to configure the cursor type to string.
// It defaults to int.
func WithCursorStrings() CursorsOption {
	return func(table *ctable) {
		table.schema.cursorType = cursorTypeString
	}
}

// WithTestCursorSleep replaces the sleep function for testing.
func WithTestCursorSleep(_ testing.TB, f func(time.Duration)) CursorsOption {
	return func(table *ctable) {
		table.sleep = f
	}
}

var _ CursorsTable = (*ctable)(nil)

type ctable struct {
	schema     ctableSchema
	sleep      func(d time.Duration) // Abstracted for testing
	setCounter func()

	// Async goodies
	flushMu      sync.Mutex // Required for flushing to DB
	cursorMu     sync.Mutex // Required for asyncCursors
	cursorOnce   sync.Once
	asyncCursors map[string]string
	asyncDBC     *sql.DB
	asyncPeriod  time.Duration
}

// ctableSchema defines the mysql schema of a cursors table.
type ctableSchema struct {
	name        string
	cursorField string
	idField     string
	timefield   string
	cursorType  CursorType
}

func (t *ctable) GetCursor(ctx context.Context, dbc *sql.DB, consumerID string) (string, error) {
	value, _, err := getCursor(ctx, dbc, t.schema, consumerID)
	return value, err
}

func (t *ctable) SetCursor(ctx context.Context, dbc *sql.DB, consumerID string, cursor string) error {
	_, err := t.schema.cursorType.Cast(cursor)
	if err != nil {
		return err
	}
	if !t.isAsyncEnabled() {
		t.setCounter()
		return setCursor(ctx, dbc, t.schema, consumerID, cursor)
	}

	t.cursorOnce.Do(func() {
		go t.flushForever()
	})

	t.cursorMu.Lock()
	defer t.cursorMu.Unlock()

	if t.asyncCursors == nil {
		t.asyncCursors = make(map[string]string)
		t.asyncDBC = dbc
	}

	t.asyncCursors[consumerID] = cursor
	return nil
}

func (t *ctable) isAsyncEnabled() bool {
	return t.asyncPeriod > 0
}

func (t *ctable) Flush(ctx context.Context) error {
	if !t.isAsyncEnabled() {
		return nil
	}

	t.cursorMu.Lock()
	dbc := t.asyncDBC
	m := t.asyncCursors
	t.asyncCursors = nil

	if len(m) == 0 {
		// Nothing to flush
		t.cursorMu.Unlock()
		return nil
	}

	// Grab the flush mutex before releasing the cursor mutex.
	t.flushMu.Lock()
	t.cursorMu.Unlock()
	defer t.flushMu.Unlock()

	// TODO(corver): Write all at once.
	for id, cursor := range m {
		t.setCounter()
		err := setCursor(ctx, dbc, t.schema, id, cursor)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *ctable) Clone(ol ...CursorsOption) CursorsTable {
	t.cursorMu.Lock()
	defer t.cursorMu.Unlock()
	table := &ctable{
		schema: ctableSchema{
			name:        t.schema.name,
			cursorField: t.schema.cursorField,
			idField:     t.schema.idField,
			timefield:   t.schema.timefield,
			cursorType:  t.schema.cursorType,
		},
		sleep:       t.sleep,
		asyncDBC:    t.asyncDBC,
		asyncPeriod: t.asyncPeriod,
		setCounter:  t.setCounter,
	}

	for _, o := range ol {
		o(table)
	}

	return table
}

func (t *ctable) ToStore(dbc *sql.DB, ol ...CursorsOption) reflex.CursorStore {
	cs := &cursorStore{
		t:   t,
		dbc: dbc,
	}
	if len(ol) > 0 {
		cs.t = t.Clone(ol...).(*ctable)
	}
	return cs
}

func (t *ctable) flushForever() {
	for {
		t.sleep(t.asyncPeriod)

		ctx := context.Background()
		if err := t.Flush(ctx); err != nil {
			log.Error(ctx, errors.Wrap(err, "reflex: error flushing cursor"))
		}
	}
}

type cursorStore struct {
	t   *ctable
	dbc *sql.DB
}

func (cs *cursorStore) GetCursor(ctx context.Context, consumerName string) (string, error) {
	return cs.t.GetCursor(ctx, cs.dbc, consumerName)
}

func (cs *cursorStore) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	return cs.t.SetCursor(ctx, cs.dbc, consumerName, cursor)
}

func (cs *cursorStore) Flush(ctx context.Context) error {
	return cs.t.Flush(ctx)
}
