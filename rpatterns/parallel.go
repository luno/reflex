package rpatterns

import (
	"context"
	"hash/fnv"
	"strconv"

	"github.com/luno/fate"
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
)

type parallelConfig struct {
	n            int
	streamOpts   []reflex.StreamOption
	consumerOpts []reflex.ConsumerOption

	hash HashOption
}

type ParallelOption func(pc *parallelConfig)
type getCtxFn = func(m int) context.Context
type getConsumerFn = func(m int) reflex.Consumer

// Parallel starts N consumers which consume the stream in parallel. Each event
// is consistently hashed to a consumer using the field specified in HashOption.
// Role scheduling combined with an appropriate getCtxFn can be used to
// implement distributed parallel consuming.
//
// NOTE: N should preferably be a power of 2, and modifying N will reset the
//       cursors.
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
		consumerM := makeConsumer(conf.hash, m, n, getConsumer(m))
		gcf := func() context.Context {
			return getCtx(m)
		}

		spec := reflex.NewSpec(stream, cstore, consumerM, conf.streamOpts...)
		go RunForever(gcf, spec)
	}
}

// makeConsumer returns consumer m-of-n that will only process events
// that hash to it.
func makeConsumer(hash HashOption, m, n int, inner reflex.Consumer) reflex.Consumer {
	hasher := fnv.New32()

	f := func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		var hashKey []byte
		switch hash {
		case HashOptionEventType:
			hashKey = []byte(strconv.Itoa(int(event.Type.ReflexType())))

		case HashOptionEventForeignID:
			hashKey = []byte(event.ForeignID)

		case HashOptionEventID:
			fallthrough

		default:
			hashKey = []byte(event.ID)
		}

		hasher.Reset()
		_, err := hasher.Write(hashKey)
		if err != nil {
			return err
		}

		hash := hasher.Sum32()
		if hash%uint32(n) != uint32(m) {
			return nil
		}
		return inner.Consume(ctx, fate, event)
	}

	return simpleConsumer{name: inner.Name(), consumeFn: f}
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
