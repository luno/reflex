package rpatterns

import (
	"context"
	"fmt"
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
	name         string
	streamOpts   []reflex.StreamOption
	consumerOpts []reflex.ConsumerOption

	hash   HashOption
	handle handleFn
}

type ParallelOption func(pc *parallelConfig)
type getCtxFn = func(consumerName string) context.Context
type handleFn = func(context.Context, fate.Fate, *reflex.Event) error

// Parallel starts N consumers which consume the stream in parallel. Each event
// is consistently hashed to a consumer using the field specified in HashOption.
// Role scheduling combined with an appropriate getCtxFn can be used to
// implement distributed parallel consuming.
//
// NOTE: N should preferably be a power of 2, and modifying N will reset the
//       cursors.
func Parallel(getCtx getCtxFn, name string, n int, stream reflex.StreamFunc,
	cstore reflex.CursorStore, handle handleFn, opts ...ParallelOption) {

	conf := parallelConfig{
		n:    n,
		name: name,

		hash:   HashOptionEventID,
		handle: handle,
	}

	for _, o := range opts {
		o(&conf)
	}

	for i := 0; i < n; i++ {
		consumerM := conf.makeConsumer(i)
		gcf := func() context.Context {
			return getCtx(consumerM.Name())
		}

		spec := reflex.NewSpec(stream, cstore, consumerM, conf.streamOpts...)
		go RunForever(gcf, spec)
	}
}

// makeConsumer returns consumer m-of-n that will only process events
// that hash to it.
func (pc *parallelConfig) makeConsumer(m int) reflex.Consumer {
	name := fmt.Sprintf("%s_%d_of_%d", pc.name, m+1, pc.n)
	hasher := fnv.New32()

	f := func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
		var hashKey []byte
		switch pc.hash {
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
		if hash%uint32(pc.n) != uint32(m) {
			return nil
		}

		return pc.handle(ctx, fate, event)
	}

	return reflex.NewConsumer(name, f, pc.consumerOpts...)
}

func WithStreamOpts(opts ...reflex.StreamOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.streamOpts = append(pc.streamOpts, opts...)
	}
}

func WithConsumerOpts(opts ...reflex.ConsumerOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.consumerOpts = append(pc.consumerOpts, opts...)
	}
}

func WithHashOption(opt HashOption) ParallelOption {
	return func(pc *parallelConfig) {
		pc.hash = opt
	}
}
