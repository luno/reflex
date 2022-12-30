package rpatterns

import (
	"context"
	"github.com/luno/fate"
	"github.com/luno/reflex"
	"hash/fnv"
	"strconv"
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
	HashOptionCustomHashFn HashOption = 3
)

type parallelConfig struct {
	n            int
	streamOpts   []reflex.StreamOption
	consumerOpts []reflex.ConsumerOption

	hash   HashOption
	hashFn func(event *reflex.Event) ([]byte, error)
}

type ParallelOption func(pc *parallelConfig)
type getCtxFn = func(m int) context.Context
type getConsumerFn = func(m int) reflex.Consumer
type getAckConsumerFn = func(m int) AckConsumer

// Parallel starts N consumers which consume the stream in parallel. Each event
// is consistently hashed to a consumer using the field specified in HashOption.
// Role scheduling combined with an appropriate getCtxFn can be used to
// implement distributed parallel consuming.
//
// NOTE: N should preferably be a power of 2, and modifying N will reset the cursors.
func Parallel(getCtx getCtxFn, getConsumer getConsumerFn, n int, stream reflex.StreamFunc,
	cstore reflex.CursorStore, opts ...ParallelOption) {

	conf := parallelConfig{
		n:    n,
		hash: HashOptionEventID,
	}

	for _, o := range opts {
		o(&conf)
	}

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

	conf := parallelConfig{
		n:    n,
		hash: HashOptionEventID,
	}

	for _, o := range opts {
		o(&conf)
	}

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
	checkShard := makeShardCheckingFunc(conf, n)

	f := func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		if isInShard, err := checkShard(m, event); !isInShard || err != nil {
			return err
		}
		return inner.Consume(ctx, fate, event)
	}

	return simpleConsumer{name: inner.Name(), consumeFn: f}
}

func makeShardCheckingFunc(conf parallelConfig, shardCount int) func(currentShard int, event *reflex.Event) (bool, error) {
	hasher := fnv.New32()

	return func(currentShard int, event *reflex.Event) (bool, error) {
		var hashKey []byte

		switch conf.hash {
		case HashOptionEventType:
			hashKey = []byte(strconv.Itoa(int(event.Type.ReflexType())))

		case HashOptionEventForeignID:
			hashKey = []byte(event.ForeignID)

		case HashOptionCustomHashFn:
			var err error
			hashKey, err = conf.hashFn(event)
			if err != nil {
				return false, err
			}

		case HashOptionEventID:
			fallthrough

		default:
			hashKey = []byte(event.ID)
		}

		hasher.Reset()
		_, err := hasher.Write(hashKey)
		if err != nil {
			return false, err
		}

		return hasher.Sum32()%uint32(shardCount) == uint32(currentShard), nil
	}
}

// makeAckConsumer returns consumer m-of-n that will only process events
// that hash to it. Events must be acked manually.
func makeAckConsumer(conf parallelConfig, m, n int, inner AckConsumer) *AckConsumer {
	checkShard := makeShardCheckingFunc(conf, n)

	f := func(ctx context.Context, fate fate.Fate, event *AckEvent) error {
		if isInShard, err := checkShard(m, &event.Event); !isInShard || err != nil {
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

func WithStreamOpts(opts ...reflex.StreamOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.streamOpts = append(pc.streamOpts, opts...)
	}
}

func WithHashOption(opt HashOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.hash = opt
	}
}

// WithHashFn specifies the custom hash function that will be used to distribute work to parallel
// consumers when HashOptionCustomHashFn is specified.
func WithHashFn(fn func(event *reflex.Event) ([]byte, error)) ParallelOption {
	return func(pc *parallelConfig) {
		pc.hashFn = fn
	}
}
