package rpatterns

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"

	"github.com/luno/reflex"
)

// HashOption the different hashing option to spread work over consumers.
type HashOption int

const (
	// HashOptionEventID results in the most even distribution, but doesn't
	// provide any ordering guarantees. If no hash option is provided then
	// this option is used by default.
	HashOptionEventID HashOption = 0

	// HashOptionEventType will probably result in a very uneven distribution
	// (depending on the total number of event types), but it does guarantee
	// process ordering by type.
	HashOptionEventType HashOption = 1

	// HashOptionEventForeignID should result in a good distribution and
	// guarantees process ordering by foreign id.
	HashOptionEventForeignID HashOption = 2

	// HashOptionCustomHashFn allows the caller to provide a custom hash function
	// allowing them to tailor distribution and ordering for specific needs.
	// Deprecated: Only need to use WithHashFn
	HashOptionCustomHashFn HashOption = 3
)

type parallelConfig struct {
	streamOpts []reflex.StreamOption

	fmtName               func(string, int, int) string
	hashFn                func(event *reflex.Event) ([]byte, error)
	shardConsumerOptsFunc ShardConsumerOpts
}

func getParallelConfig(opts []ParallelOption) parallelConfig {
	c := parallelConfig{
		fmtName: appendMofN,
		hashFn:  keyByEventID,
		shardConsumerOptsFunc: func(_ string) []reflex.ConsumerOption {
			return nil
		},
	}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// ParallelOption configures the consumer with different behaviour
type ParallelOption func(pc *parallelConfig)
type getCtxFn = func(m int) context.Context
type getConsumerFn = func(m int) reflex.Consumer
type getAckConsumerFn = func(m int) AckConsumer

// ConsumerShard is one of n consumers, with a formatted name and a unique EventFilter
type ConsumerShard struct {
	Name         string
	filter       EventFilter
	consumerOpts []reflex.ConsumerOption
}

// GetFilter gets the filter for this shard
func (s ConsumerShard) GetFilter() EventFilter {
	return s.filter
}

type eventKeyFn func(event *reflex.Event) ([]byte, error)

// EventFilter takes a reflex.Event and returns true if it should be allowed to be processed or
// false if it shouldn't. It can error if it fails to determine if the event should be processed.
type EventFilter func(event *reflex.Event) (bool, error)

func filterOnHash(m, n int, keyFn eventKeyFn) EventFilter {
	hsh := fnv.New32()
	return func(event *reflex.Event) (bool, error) {
		hsh.Reset()
		key, err := keyFn(event)
		if err != nil {
			return false, err
		}
		_, err = hsh.Write(key)
		if err != nil {
			return false, err
		}

		hash := hsh.Sum32()
		return hash%uint32(n) == uint32(m), nil
	}
}

func appendMofN(base string, m, n int) string {
	return fmt.Sprintf("%s_%d_of_%d", base, m+1, n)
}

// ConsumerShards gets the ConsumerShard for each of a ParallelConsumer or ParallelAckConsumer
// Each shard is configured to filter events such that each event is processed by one and only
// one shard of the returned n shards.
// You only need to call ConsumerShards if you're planning to use different consume functions for
// each shard. For most cases you can use ParallelSpecs.
func ConsumerShards(name string, n int, opts ...ParallelOption) []ConsumerShard {
	conf := getParallelConfig(opts)
	ret := make([]ConsumerShard, 0, n)
	for m := 0; m < n; m++ {
		shardName := conf.fmtName(name, m, n)
		pc := ConsumerShard{
			Name:         shardName,
			filter:       filterOnHash(m, n, conf.hashFn),
			consumerOpts: conf.shardConsumerOptsFunc(shardName),
		}
		ret = append(ret, pc)
	}
	return ret
}

// ParallelConsumer constructs a reflex.Consumer from a ConsumerShard.
// This pattern is used when you need to customise the consume function for each shard.
// This reflex.Consumer can be used in reflex.NewSpec to make it runnable.
func ParallelConsumer(
	shard ConsumerShard,
	consume func(context.Context, fate.Fate, *reflex.Event) error,
	opts ...reflex.ConsumerOption,
) reflex.Consumer {
	filteredConsume := func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		mine, err := shard.filter(event)
		if err != nil {
			return err
		}

		if !mine {
			return errors.Wrap(reflex.ErrFiltered, "", j.MKV{
				"event_id":   event.IDInt(),
				"foreign_id": event.ForeignID,
			})
		}

		return consume(ctx, fate, event)
	}
	return reflex.NewConsumer(shard.Name, filteredConsume, opts...)
}

// ParallelAckConsumer constructs a AckConsumer from a ConsumerShard.
// This AckConsumer can be used in NewAckSpec to make it runnable.
func ParallelAckConsumer(
	shard ConsumerShard,
	store reflex.CursorStore,
	consume func(context.Context, fate.Fate, *AckEvent) error,
	opts ...reflex.ConsumerOption,
) reflex.Consumer {
	filteredConsume := func(ctx context.Context, fate fate.Fate, event *AckEvent) error {
		mine, err := shard.filter(&event.Event)
		if err != nil || !mine {
			return err
		}
		return consume(ctx, fate, event)
	}
	return NewAckConsumer(shard.Name, store, filteredConsume, opts...)
}

// ParallelSpecs will create n reflex.Spec structs, one for each shard.
// stream and store are re-used for each spec.
// This pattern is used when consume is the same function for each shard.
// See ParallelOption for more details on passing through reflex.ConsumerOption or reflex.StreamOption
func ParallelSpecs(name string, n int,
	stream reflex.StreamFunc, store reflex.CursorStore,
	consume func(context.Context, fate.Fate, *reflex.Event) error,
	opts ...ParallelOption,
) []reflex.Spec {
	var specs []reflex.Spec
	conf := getParallelConfig(opts)
	for _, shard := range ConsumerShards(name, n, opts...) {
		specs = append(specs,
			reflex.NewSpec(stream, store,
				ParallelConsumer(shard, consume, shard.consumerOpts...),
				conf.streamOpts...,
			),
		)
	}
	return specs
}

// Parallel starts N consumers which consume the stream in parallel. Each event
// is consistently hashed to a consumer using the field specified in HashOption.
// Role scheduling combined with an appropriate getCtxFn can be used to
// implement distributed parallel consuming.
//
// NOTE: N should preferably be a power of 2, and modifying N will reset the cursors.
func Parallel(getCtx getCtxFn, getConsumer getConsumerFn, n int, stream reflex.StreamFunc,
	cstore reflex.CursorStore, opts ...ParallelOption) {

	conf := getParallelConfig(opts)
	for m := 0; m < n; m++ {
		m := m
		consumerM := makeConsumer(conf, m, n, getConsumer(m))
		gcf := func() context.Context {
			return getCtx(m)
		}

		spec := reflex.NewSpec(stream, cstore, consumerM, conf.streamOpts...)
		go RunForever(gcf, spec)
	}
}

// ParallelAck starts N consumers which consume the stream in parallel. Each event
// is consistently hashed to a consumer using the field specified in HashOption.
// Role scheduling combined with an appropriate getCtxFn can be used to
// implement distributed parallel consuming. Events must be acked manually.
//
// NOTE: N should preferably be a power of 2, and modifying N will reset the
// cursors.
func ParallelAck(getCtx getCtxFn, getConsumer getAckConsumerFn, n int, stream reflex.StreamFunc, opts ...ParallelOption) {
	conf := getParallelConfig(opts)
	for m := 0; m < n; m++ {
		m := m
		consumerM := makeAckConsumer(conf, m, n, getConsumer(m))
		gcf := func() context.Context {
			return getCtx(m)
		}

		spec := NewAckSpec(stream, consumerM, conf.streamOpts...)
		go RunForever(gcf, spec)
	}
}

// makeConsumer returns consumer m-of-n that will only process events
// that hash to it.
func makeConsumer(conf parallelConfig, m, n int, inner reflex.Consumer) reflex.Consumer {
	filter := filterOnHash(m, n, conf.hashFn)

	f := func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		if isInShard, err := filter(event); !isInShard || err != nil {
			return err
		}
		return inner.Consume(ctx, fate, event)
	}

	return simpleConsumer{name: inner.Name(), consumeFn: f}
}

// makeAckConsumer returns consumer m-of-n that will only process events
// that hash to it. Events must be acked manually.
func makeAckConsumer(conf parallelConfig, m, n int, inner AckConsumer) *AckConsumer {
	filter := filterOnHash(m, n, conf.hashFn)

	f := func(ctx context.Context, fate fate.Fate, event *AckEvent) error {
		if isInShard, err := filter(&event.Event); !isInShard || err != nil {
			return err
		}
		return inner.Consume(ctx, fate, &event.Event)
	}

	return NewAckConsumer(inner.Name(), inner.cstore, f)
}

type simpleConsumer struct {
	name      string
	consumeFn func(ctx context.Context, fate fate.Fate, event *reflex.Event) error
}

func (s simpleConsumer) Name() string {
	return s.name
}

func (s simpleConsumer) Consume(ctx context.Context, f fate.Fate, event *reflex.Event) error {
	return s.consumeFn(ctx, f, event)
}

// WithStreamOpts passes stream options in to the reflex.Spec
func WithStreamOpts(opts ...reflex.StreamOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.streamOpts = append(pc.streamOpts, opts...)
	}
}

type ShardConsumerOpts func(string) []reflex.ConsumerOption

// WithConsumerSpecificOpts gets consumer options per shard
func WithConsumerSpecificOpts(f ShardConsumerOpts) ParallelOption {
	return func(pc *parallelConfig) {
		pc.shardConsumerOptsFunc = f
	}
}

func keyByEventType(event *reflex.Event) ([]byte, error) {
	return []byte(strconv.Itoa(event.Type.ReflexType())), nil
}

func keyByForeignID(event *reflex.Event) ([]byte, error) {
	return []byte(event.ForeignID), nil
}

func keyByEventID(event *reflex.Event) ([]byte, error) {
	return []byte(event.ID), nil
}

// WithHashOption allows you to use one of the HashOption values to determine how events are distributed
func WithHashOption(opt HashOption) ParallelOption {
	return func(pc *parallelConfig) {
		switch opt {
		case HashOptionCustomHashFn:
			return
		case HashOptionEventType:
			pc.hashFn = keyByEventType
		case HashOptionEventForeignID:
			pc.hashFn = keyByForeignID
		case HashOptionEventID:
			pc.hashFn = keyByEventID
		}
	}
}

// WithHashFn specifies the custom hash function that will be used to distribute work to parallel consumers.
func WithHashFn(fn func(event *reflex.Event) ([]byte, error)) ParallelOption {
	return func(pc *parallelConfig) {
		if fn != nil {
			pc.hashFn = fn
		}
	}
}

// WithNameFormatter determines how each consumer name will be constructed.
// The default name formatter takes the base string and adds "_m_of_n" to the end.
// e.g. "test" becomes "test_3_of_8"
func WithNameFormatter(fn func(base string, m, n int) string) ParallelOption {
	return func(pc *parallelConfig) {
		pc.fmtName = fn
	}
}
