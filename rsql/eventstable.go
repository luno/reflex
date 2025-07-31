package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"sync"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"

	"github.com/luno/reflex"
)

const (
	defaultStreamBackoff = time.Second * 10
)

// NewEventsTable returns a new events table.
func NewEventsTable(name string, opts ...EventsOption) *EventsTable {
	table := &EventsTable{
		schema: eTableSchema{
			name:           name,
			idField:        defaultEventIDField,
			timeField:      defaultEventTimeField,
			typeField:      defaultEventTypeField,
			foreignIDField: defaultEventForeignIDField,
			metadataField:  defaultMetadataField,
			traceField:     defaultTraceField,
		},
		options: options{
			notifier: &stubNotifier{},
			backoff:  defaultStreamBackoff,
		},
		config: opts,
	}

	for _, o := range table.config {
		o(table)
	}

	if table.inserter == nil {
		table.inserter = makeDefaultInserter(table.schema)
	}

	table.gapCh = make(chan Gap)
	table.gapListeners = make(chan GapListenFunc)
	table.gapListenDone = make(chan struct{})
	
	// Initialize gap fill cancellation context
	table.gapFillCtx, table.gapFillCancel = context.WithCancel(context.Background())
	table.currentLoader = buildLoader(
		table.baseLoader,
		table.gapCh,
		table.disableCache,
		table.schema,
		table.includeNoopEvents,
	)

	return table
}

// EventsOption defines a functional option to configure new event tables.
type EventsOption func(*EventsTable)

// WithEventIDField provides an option to set the event DB ID field. This is
// useful for tables which implement custom event loaders. It defaults to 'id'.
func WithEventIDField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.idField = field
	}
}

// WithEventTimeField provides an option to set the event DB timestamp field.
// It defaults to 'timestamp'.
func WithEventTimeField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.timeField = field
	}
}

// WithEventTypeField provides an option to set the event DB type field.
// It defaults to 'type'.
func WithEventTypeField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.typeField = field
	}
}

// WithEventForeignIDField provides an option to set the event DB foreignID field.
// It defaults to 'foreign_id'.
func WithEventForeignIDField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.foreignIDField = field
	}
}

// WithEventMetadataField provides an option to set the event DB metadata field.
// It is disabled by default; ie. ”.
func WithEventMetadataField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.metadataField = field
	}
}

// WithEventTraceField provides an option to persist an opentelemetry trace through the events stream
func WithEventTraceField(field string) EventsOption {
	return func(table *EventsTable) {
		table.schema.traceField = field
	}
}

// WithEventsNotifier provides an option to receive event notifications
// and trigger StreamClients when new events are available.
func WithEventsNotifier(notifier EventsNotifier) EventsOption {
	return func(table *EventsTable) {
		table.notifier = notifier
	}
}

// WithEventsInMemNotifier provides an option that enables an in-memory
// notifier.
//
// Note: This can have a significant impact on database load
// if the cache is disabled since all consumers might query
// the database on every event.
func WithEventsInMemNotifier() EventsOption {
	return func(table *EventsTable) {
		table.notifier = &inmemNotifier{}
	}
}

// WithEventsCacheEnabled provides an option to enable the read-through
// cache on the events table.
//
// Deprecated: Cache enabled by default.
func WithEventsCacheEnabled() EventsOption {
	return func(table *EventsTable) {
		table.disableCache = false
	}
}

// WithIncludeNoopEvents noop events are not streamed by default. Use this option to enable
// the streaming of noop events
func WithIncludeNoopEvents() EventsOption {
	return func(table *EventsTable) {
		table.includeNoopEvents = true
	}
}

// WithoutEventsCache provides an option to disable the read-through
// cache on the events table.
func WithoutEventsCache() EventsOption {
	return func(table *EventsTable) {
		table.disableCache = true
	}
}

// WithEventsBackoff provides an option to set the backoff period between polling
// the DB for new events. It defaults to 10s.
func WithEventsBackoff(d time.Duration) EventsOption {
	return func(table *EventsTable) {
		table.backoff = d
	}
}

// WithEventsLoader provides an option to set the base event loader function.
// The base event loader loads events returns the next available events and
// the associated next cursor after the previous cursor or an error.
// The default loader is configured with the WithEventsXField options.
func WithEventsLoader(loader loader) EventsOption {
	return func(table *EventsTable) {
		table.baseLoader = loader
	}
}

// WithEventsInserter provides an option to set the event inserter
// which inserts event into a sql table. The default inserter is
// configured with the WithEventsXField options.
func WithEventsInserter(inserter inserter) EventsOption {
	return func(table *EventsTable) {
		table.inserter = inserter
	}
}

// inserter abstracts the insertion of an event into a sql table.
type inserter func(ctx context.Context, dbc DBC,
	foreignID string, typ reflex.EventType, metadata []byte) error

// EventsTable provides reflex event insertion and streaming
// for a sql db table.
type EventsTable struct {
	options
	config            []EventsOption
	schema            eTableSchema
	disableCache      bool
	includeNoopEvents bool
	baseLoader        loader
	inserter          inserter

	// Stateful fields not cloned
	currentLoader filterLoader
	gapCh         chan Gap
	gapListeners  chan GapListenFunc
	gapListenDone chan struct{}
	gapListening  sync.Once

	// Gap filling cancellation support
	gapFillCtx    context.Context
	gapFillCancel context.CancelFunc
	gapFillMu     sync.Mutex
}

// Insert inserts an event into the EventsTable and returns a function that
// can be optionally called to notify the table's EventNotifier of the change.
// The intended pattern for this function is:
//
//	notify, err := eTable.Insert(ctx, tx, ...)
//	if err != nil {
//	  return err
//	}
//	defer notify()
//	return doWorkAndCommit(tx)
func (t *EventsTable) Insert(ctx context.Context, dbc DBC, foreignID string,
	typ reflex.EventType,
) (NotifyFunc, error) {
	return t.InsertWithMetadata(ctx, dbc, foreignID, typ, nil)
}

// InsertWithMetadata inserts an event with metadata into the EventsTable.
// Note metadata is disabled by default, enable with WithEventMetadataField option.
func (t *EventsTable) InsertWithMetadata(ctx context.Context, dbc DBC, foreignID string,
	typ reflex.EventType, metadata []byte,
) (NotifyFunc, error) {
	if isNoop(foreignID, typ) {
		return nil, errors.New("inserting invalid noop event")
	}
	err := t.inserter(ctx, dbc, foreignID, typ, metadata)
	if err != nil {
		return noopFunc, err
	}

	return t.notifier.Notify, nil
}

// Clone returns a new events table generated from the config of t with the new options applied.
// Note that non-config fields are not copied, so things like the cache and inmemnotifier
// are not shared.
func (t *EventsTable) Clone(opts ...EventsOption) *EventsTable {
	clone := append([]EventsOption(nil), t.config...)
	clone = append(clone, opts...)
	return NewEventsTable(t.schema.name, clone...)
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from the db. It is only safe for a single goroutine to use.
func (t *EventsTable) Stream(ctx context.Context, dbc *sql.DB, after string,
	opts ...reflex.StreamOption,
) reflex.StreamClient {
	sc := &streamClient{
		schema:  t.schema,
		after:   after,
		dbc:     dbc,
		ctx:     ctx,
		options: t.options,
		loader:  t.currentLoader,
	}

	for _, o := range opts {
		o(&sc.StreamOptions)
	}

	eventsGapListenGauge.WithLabelValues(t.schema.name) // Init zero gap filling gauge.

	return sc
}

// ToStream returns a reflex StreamFunc interface of this EventsTable.
func (t *EventsTable) ToStream(dbc *sql.DB, opts1 ...reflex.StreamOption) reflex.StreamFunc {
	return func(ctx context.Context, after string,
		opts2 ...reflex.StreamOption,
	) (client reflex.StreamClient, e error) {
		return t.Stream(ctx, dbc, after, append(opts1, opts2...)...), nil
	}
}

// ListenGaps adds f to a slice of functions that are called when a gap is detected.
// On first call, it starts a goroutine that serves these functions.
func (t *EventsTable) ListenGaps(f GapListenFunc) {
	t.gapListening.Do(func() { go t.serveGaps() })
	t.gapListeners <- f
}

func (t *EventsTable) StopGapListener(ctx context.Context) {
	// Cancel any ongoing gap filling operations
	t.gapFillMu.Lock()
	if t.gapFillCancel != nil {
		t.gapFillCancel()
	}
	t.gapFillMu.Unlock()

	close(t.gapListeners)
	select {
	case <-ctx.Done():
	case <-t.gapListenDone:
	}
}

type GapListenFunc func(Gap)

func (t *EventsTable) serveGaps() {
	defer close(t.gapListenDone)
	var fns []GapListenFunc
	for {
		select {
		case gapFn, more := <-t.gapListeners:
			if !more {
				return
			}
			fns = append(fns, gapFn)
		case gap := <-t.gapCh:
			for _, fn := range fns {
				fn(gap)
			}
		}
	}
}

// getSchema returns the table schema and implements the gapTable interface for FillGaps.
func (t *EventsTable) getSchema() eTableSchema {
	return t.schema
}

// getGapFillContext returns the context used for gap filling operations.
func (t *EventsTable) getGapFillContext() context.Context {
	t.gapFillMu.Lock()
	defer t.gapFillMu.Unlock()
	return t.gapFillCtx
}

// buildLoader returns a new layered event loader.
func buildLoader(
	baseLoader loader,
	ch chan<- Gap,
	disableCache bool,
	schema eTableSchema,
	withNoopEvents bool,
) filterLoader {
	if baseLoader == nil {
		baseLoader = makeBaseLoader(schema)
	}
	loader := wrapGapDetector(baseLoader, ch, schema.name)
	if !disableCache /* ie. enableCache */ {
		loader = newRCache(loader, schema.name).Load
	}

	if !withNoopEvents {
		return wrapNoopFilter(loader)
	}
	return wrapFilter(loader)
}

// options define config/state defined in EventsTable used by a streamClient.
type options struct {
	reflex.StreamOptions

	notifier EventsNotifier
	backoff  time.Duration
}

// eTableSchema defines the mysql schema of an events table.
type eTableSchema struct {
	name           string
	idField        string
	timeField      string
	typeField      string
	foreignIDField string
	metadataField  string
	traceField     string
}

type streamClient struct {
	options

	schema     eTableSchema
	after      string
	prev       int64 // Previous (current) cursor.
	buf        []*reflex.Event
	dbc        *sql.DB
	ctx        context.Context
	notifyChan <-chan struct{}

	// loader queries next events from the DB.
	loader filterLoader
}

// Recv blocks and returns the next event in the stream. It queries the db
// in batches buffering the results. If the buffer is not empty is pops one
// event and returns it. When querying and no new events are found it backs off
// before retrying. It blocks until it can return a non-nil event or an error.
// It is only safe for a single goroutine to call Recv.
func (s *streamClient) Recv() (*reflex.Event, error) {
	if err := s.ctx.Err(); err != nil {
		return nil, err
	}

	// Initialise cursor s.prev once.
	var err error
	if s.StreamFromHead {
		s.prev, err = getLatestID(s.ctx, s.dbc, s.schema)
		if err != nil {
			return nil, err
		}
		s.StreamFromHead = false
		s.after = "" // StreamFromHead overrides after.
	} else if s.after != "" {
		s.prev, err = strconv.ParseInt(s.after, 10, 64)
		if err != nil {
			return nil, ErrInvalidIntID
		}
		s.after = ""
	}

	for len(s.buf) == 0 {
		eventsPollCounter.WithLabelValues(s.schema.name).Inc()
		el, override, err := s.loader(s.ctx, s.dbc, s.prev, s.Lag)
		if err != nil {
			return nil, err
		}

		// Sanity check: override cursor if no events.
		if override != 0 && len(el) > 0 {
			return nil, errors.New("cursor override with events",
				j.MKV{"prev": s.prev, "override": override})
		} else if len(el) == 0 && s.prev != 0 && override == 0 {
			return nil, errors.New("no cursor override and no events",
				j.MKV{"prev": s.prev, "override": override})
		}

		s.buf = el

		if len(el) > 0 {
			break
		}

		if s.prev != override {
			// Whole range of events skipped, try again with new override cursor.
			s.prev = override
			continue
		}

		// No cursor override or events, so current head reached.

		if s.StreamToHead {
			return nil, reflex.ErrHeadReached
		}

		if err := s.wait(s.backoff); err != nil {
			return nil, err
		}
	}

	// Pop next event from buffer.
	e := s.buf[0]
	s.buf = s.buf[1:]
	next := e.IDInt()

	// Sanity check: next cursor must be greater than prev
	if s.prev >= next {
		return nil, errors.Wrap(ErrConsecEvent, "pop error",
			j.MKV{"prev": s.prev, "next": next})
	}

	s.prev = next

	return e, nil
}

func (s *streamClient) wait(d time.Duration) error {
	if d == 0 {
		return nil
	} else if s.notifyChan == nil {
		s.notifyChan = s.notifier.C()
	}

	t := time.NewTimer(d)
	select {
	case <-s.notifyChan:
		s.notifyChan = nil
		return nil
	case <-t.C:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
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

// stubNotifier is an implementation of EventsNotifier that does nothing.
type stubNotifier struct {
	c chan struct{}
}

func (m *stubNotifier) Notify() {
}

func (m *stubNotifier) C() <-chan struct{} {
	return m.c
}

// inmemNotifier is an in-memory implementation of EventsNotifier.
type inmemNotifier struct {
	mu        sync.Mutex
	listeners []chan struct{}
}

func (n *inmemNotifier) Notify() {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, l := range n.listeners {
		select {
		case l <- struct{}{}:
		default:
		}
	}
	n.listeners = nil
}

func (n *inmemNotifier) C() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	ch := make(chan struct{}, 1)
	n.listeners = append(n.listeners, ch)
	return ch
}

// EventsNotifier provides a way to receive notifications when an event is
// inserted in an EventsTable, and a way to trigger an EventsTable's
// StreamClients when there are new events available.
type EventsNotifier interface {
	// StreamWatcher is passed as the default StreamWatcher every time stream()
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
	// reaches the head of an events table.
	C() <-chan struct{}
}
