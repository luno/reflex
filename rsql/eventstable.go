package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
)

const (
	defaultStreamBackoff = time.Second * 10
)

var ErrInvalidIntID = errors.New("invalid id, only int supported", j.C("ERR_82d0368b5478d378"))

// EventsTable provides an interface to an events db table.
type EventsTable interface {
	// Insert inserts an event into the EventsTable and returns a function that
	// can be optionally called to notify the table's EventNotifier of the change.
	// The intended pattern for this function is:
	//
	//       notify, err := etable.Insert(ctx, tx, ...)
	//       if err != nil {
	//         return err
	//       }
	//	     defer notify()
	//       return doWorkAndCommit(tx)
	Insert(ctx context.Context, tx *sql.Tx, foreignID string, typ reflex.EventType) (
		NotifyFunc, error)

	// InsertWithMetadata inserts an event with metadata into the EventsTable.
	// Note metadata is disabled by default, enable with WithEventMetadataField option.
	InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID string, typ reflex.EventType,
		metadata []byte) (NotifyFunc, error)

	// ToStream returns a reflex StreamFunc interface of this EventsTable.
	ToStream(dbc *sql.DB, opts ...reflex.StreamOption) reflex.StreamFunc

	// Clone returns a copy of the EventsTable with the given options applied.
	Clone(opts ...EventsOption) EventsTable
}

func NewEventsTable(name string, opts ...EventsOption) EventsTable {
	table := &etable{
		schema: etableSchema{
			name:           name,
			timeField:      defaultEventTimeField,
			typeField:      defaultEventTypeField,
			foreignIDField: defaultEventForeignIDField,
			metadataField:  defaultMetadataField,
		},
		options: options{
			notifier: &mockNotifier{},
			backoff:  defaultStreamBackoff,
		},
	}
	for _, o := range opts {
		o(table)
	}
	table.loader = buildLoader(table.enableCache, table.noGapFill)
	return table
}

type EventsOption func(*etable)

// WithEventTimeField provides an option to set the event DB timestamp field.
// It defaults to 'timestamp'.
func WithEventTimeField(field string) EventsOption {
	return func(table *etable) {
		table.schema.timeField = field
	}
}

// WithEventTypeField provides an option to set the event DB type field.
// It defaults to 'type'.
func WithEventTypeField(field string) EventsOption {
	return func(table *etable) {
		table.schema.typeField = field
	}
}

// WithEventForeignIDField provides an option to set the event DB foreignID field.
// It defaults to 'foreign_id'.
func WithEventForeignIDField(field string) EventsOption {
	return func(table *etable) {
		table.schema.foreignIDField = field
	}
}

// WithEventsNotifier provides an option to receive event notifications
// and trigger StreamClients when new events are available.
func WithEventsNotifier(notifier EventsNotifier) EventsOption {
	return func(table *etable) {
		table.notifier = notifier
	}
}

// WithEventsCacheEnabled provides an option to enable the read-through
// cache on the events tables.
func WithEventsCacheEnabled() EventsOption {
	return func(table *etable) {
		table.enableCache = true
	}
}

// WithEventsNoGapFill provides an option to disable gap filling (write operations)
// during event streaming. This is useful if streaming from replicas to avoid db errors.
//
// Note that each events table needs at least one stream with gap filling enabled
// since streams block on gaps.
//
// Note that since one gap filling stream is required and since caching is supported and reads are
// efficient (range selects on primary keys) streaming from replicas is only recommended for
// in case of data consistency issues due to replica lag.
func WithEventsNoGapFill() EventsOption {
	return func(table *etable) {
		table.noGapFill = true
	}
}

// WithEventMetadataField provides an option to set the event DB metadata field.
// It is disabled by default; ie. ''.
func WithEventMetadataField(field string) EventsOption {
	return func(table *etable) {
		table.schema.metadataField = field
	}
}

// WithEventsBackoff provides an option to set the backoff period between polling
// the DB for new events.
func WithEventsBackoff(d time.Duration) EventsOption {
	return func(table *etable) {
		table.backoff = d
	}
}

type etable struct {
	options
	schema      etableSchema
	enableCache bool
	noGapFill   bool

	loader eventsLoader // Not cloned
}

func (t *etable) Insert(ctx context.Context, tx *sql.Tx, foreignID string,
	typ reflex.EventType) (NotifyFunc, error) {
	return t.InsertWithMetadata(ctx, tx, foreignID, typ, nil)
}

func (t *etable) InsertWithMetadata(ctx context.Context, tx *sql.Tx, foreignID string,
	typ reflex.EventType, metadata []byte) (NotifyFunc, error) {
	if isNoop(foreignID, typ) {
		return nil, errors.New("inserting invalid noop event")
	}
	err := insertEvent(ctx, tx, t.schema, foreignID, typ, metadata)
	if err != nil {
		return noopFunc, err
	}

	return t.notifier.Notify, nil
}

// Clone returns a copy of etable with the new options applied.
// Note that the internal event loader is rebuilt, so the cache is not shared.
func (t *etable) Clone(opts ...EventsOption) EventsTable {
	res := *t
	for _, opt := range opts {
		opt(&res)
	}

	res.loader = buildLoader(res.enableCache, res.noGapFill)

	return &res
}

// Stream returns a StreamClient that streams events from the db.
// It is only safe for a single goroutine to use.
func (t *etable) Stream(ctx context.Context, dbc *sql.DB, after string,
	opts ...reflex.StreamOption) reflex.StreamClient {

	sc := &streamclient{
		schema:  t.schema,
		after:   after,
		dbc:     dbc,
		ctx:     ctx,
		options: t.options,
		loader:  t.loader,
	}

	for _, o := range opts {
		o(&sc.StreamOptions)
	}

	return sc
}

func (t *etable) ToStream(dbc *sql.DB, opts1 ...reflex.StreamOption) reflex.StreamFunc {
	return func(ctx context.Context, after string,
		opts2 ...reflex.StreamOption) (client reflex.StreamClient, e error) {
		return t.Stream(ctx, dbc, after, append(opts1, opts2...)...), nil
	}
}

// buildLoader returns a new layered event loader.
func buildLoader(enableCache, noGapFill bool) eventsLoader {
	loader := makeIncrementLoader(noGapFill)
	if enableCache {
		loader = wrapRCache(loader)
	}
	return wrapNoopFilter(loader)
}

// options define config/state defined in etable used by the streamclients.
type options struct {
	reflex.StreamOptions

	notifier EventsNotifier
	backoff  time.Duration
}

// etableSchema defines the mysql schema of an events table.
type etableSchema struct {
	name           string
	timeField      string
	typeField      string
	foreignIDField string
	metadataField  string
}

type streamclient struct {
	options

	schema etableSchema
	after  string
	lastID int64
	buf    []*reflex.Event
	dbc    *sql.DB
	ctx    context.Context

	// loader queries next events from the DB.
	loader eventsLoader
}

// Recv blocks and returns the next event in the stream. It queries the db
// in batches caching the results. If the cache is not empty is pops one
// event and returns it. When querying if no new events are found it backs off
// before retrying. It blocks until it can return a non-nil event or an error.
// It is only safe for a single goroutine to call Recv.
func (s *streamclient) Recv() (*reflex.Event, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	// Initialise cursor s.LastID once.
	var err error
	if s.StreamFromHead {
		s.lastID, err = getLatestID(s.ctx, s.dbc, s.schema)
		if err != nil {
			return nil, err
		}
		s.StreamFromHead = false
	} else if s.after != "" {
		s.lastID, err = strconv.ParseInt(s.after, 10, 64)
		if err != nil {
			return nil, ErrInvalidIntID
		}
		s.after = ""
	}

	for len(s.buf) == 0 {
		eventsPollCounter.WithLabelValues(s.schema.name).Inc()
		el, err := s.loader(s.ctx, s.dbc, s.schema, s.lastID, s.Lag)
		if err != nil {
			return nil, err
		} else if len(el) == 0 {
			if err := s.wait(s.backoff); err != nil {
				return nil, err
			}
			continue
		}

		s.buf = el
	}
	e := s.buf[0]
	s.lastID = e.IDInt()
	s.buf = s.buf[1:]
	return e, nil
}

func (s *streamclient) wait(d time.Duration) error {
	if d == 0 {
		return nil
	}
	t := time.NewTimer(d)
	select {
	case <-s.notifier.C():
		return nil
	case <-t.C:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

type eventsLoader func(ctx context.Context, dbc *sql.DB, schema etableSchema, after int64, lag time.Duration) ([]*reflex.Event, error)

// wrapNoopFilter returns a loader that filters out all noop events returned
// by the provided loader. Noops are required to ensure at-least-once event consistency for
// event streams in the face of long running transactions. Consumers however
// should not have to handle the special noop case.
func wrapNoopFilter(loader eventsLoader) eventsLoader {
	return func(ctx context.Context, dbc *sql.DB, schema etableSchema,
		after int64, lag time.Duration) (events []*reflex.Event, e error) {
		for {
			el, err := loader(ctx, dbc, schema, after, lag)
			if err != nil {
				return nil, err
			}
			if len(el) == 0 {
				// No new events
				return nil, nil
			}
			var res []*reflex.Event
			for _, e := range el {
				if isNoopEvent(e) {
					continue
				}
				res = append(res, e)
			}
			if len(res) > 0 {
				return res, nil
			}
			// Only noops found, skip them and try again.
			after += int64(len(el))
		}
	}
}

// makeIncrementLoader returns an incrementLoader that loads monotonically incremental
// events (backed by auto increment int column). All events after `after` and before any
// gap is returned. Gaps may be permanent, due to rollbacks, or temporary due to uncommitted
// transactions. Noop events are inserted into permanent gaps, unless noGapFill which will result
// in the stream blocking until the gap is filled by another. Temporary gaps return empty results
// after a timeout. This implies that streams block on uncommitted transactions.
func makeIncrementLoader(noGapFill bool) eventsLoader {
	return func(ctx context.Context, dbc *sql.DB, schema etableSchema, after int64,
		lag time.Duration) ([]*reflex.Event, error) {

		el, err := getNextEvents(ctx, dbc, schema, after, lag)
		if err != nil {
			return nil, err
		} else if len(el) == 0 {
			return nil, nil
		}

		first := el[0]
		if !first.IsIDInt() {
			return nil, ErrInvalidIntID
		}
		if after != 0 && first.IDInt() != after+1 {
			// Gap between after and first detected, so try to fill it.

			if noGapFill {
				// May not attempt to insert noops, only option is to wait for someone else to do it.
				return nil, nil
			}

			err := fillGap(ctx, dbc, schema, after+1)
			if isMySQLErrCantWrite(err) {
				// Cannot insert noops, only option is to wait for someone else to do it.
				// Disable write attempts with option WithEventsReadOnlyStream.
				log.Info(ctx, "Reflex stream waiting, "+
					"since cannot insert noop", j.KS("table", schema.name))
				return nil, nil
			} else if err != nil {
				return nil, err
			}

			// Gap filled, retry now.
			return makeIncrementLoader(noGapFill)(ctx, dbc, schema, after, lag)
		}

		prev := after
		for i, e := range el {
			if !e.IsIDInt() {
				return nil, ErrInvalidIntID
			}
			if prev != 0 && e.IDInt() != prev+1 {
				// Gap detected, return everything before it (maybe the gap is gone on next load).
				return el[:i], nil
			}

			prev = e.IDInt()
		}
		return el, nil
	}
}

// isNoopEvent returns true if an event has "0" foreignID and 0 type.
func isNoopEvent(e *reflex.Event) bool {
	return isNoop(e.ForeignID, e.Type)
}

// isNoop returns true if the foreignID is "0" and the type 0.
func isNoop(foreignID string, typ reflex.EventType) bool {
	return foreignID == "0" && typ.ReflexType() == 0
}

// NotifyFunc notifies an events table's underlying EventsNotifier.
type NotifyFunc func()

var noopFunc NotifyFunc = func() {}

// mockNotifier is an implementation of EventsNotifier that does nothing.
type mockNotifier struct {
	c chan struct{}
}

func (m *mockNotifier) Notify() {
}

func (m *mockNotifier) C() <-chan struct{} {
	return m.c
}

// EventsNotifier provides a way to receive notifications when an event is
// inserted in an EventsTable, and a way to trigger an EventsTable's
// StreamClients when there are new events available.
type EventsNotifier interface {
	// StreamWatcher is passed as the default StreamWatcher every time Stream()
	// is called on the EventsTable.
	StreamWatcher

	// Notify is called by reflex every time an event is inserted into the
	// EventsTable.
	Notify()
}

// StreamWatcher provides the ability to trigger the streamer when new events are available.
type StreamWatcher interface {
	// C returns a channel that blocks until the next event is available in the
	// StreamWatcher's EventsTable. C will be called every time a StreamClient
	// needs to wait for events.
	C() <-chan struct{}
}
