package rpatterns

import (
	"context"
	"sync"
	"time"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// Batch is a batch of reflex events.
type Batch []*reflex.Event

// flushSleep duration is aliased for testing.
var flushSleep = time.Second

// BatchConsumer provides a reflex consumer that buffers events
// and flushes a batch to the consume function when either
// flushLen or flushPeriod is reached.
//
// It leverages the AckConsumer internally and acks
// (updates the cursor) after each batch is consumed.
//
// This consumer is stateful. If the underlying stream errors
// reflex needs to be reset it to clear its state. It therefore
// implements the resetter interface.
//
// Flushing of batches are async and errors are returned on reset or
// when the next event is received from the stream.
// It assumes that the stream is reset to the previous cursor
// before sending subsequent events.
type BatchConsumer struct {
	*AckConsumer
	consume func(context.Context, fate.Fate, Batch) error

	flushPeriod  time.Duration
	flushLen     int
	flushStarted bool

	buf     []*AckEvent
	start   time.Time
	ctx     context.Context
	fate    fate.Fate
	err     error
	mu      sync.Mutex
	trigger chan struct{}
}

// Reset ensures that the buffer and error is cleared
// enabling a clean run of the consumer whilst returning
// any errors that were found in the consumer.
func (c *BatchConsumer) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.buf = nil
	batchConsumerBufferLength.WithLabelValues(c.name).Set(0)

	if c.err != nil {
		err := c.err
		c.err = nil
		return err
	}
	return nil
}

// enqueue adds the event to the buffer or returns the previous flush error.
func (c *BatchConsumer) enqueue(ctx context.Context, f fate.Fate, e *AckEvent) error {
	if c.flushPeriod == 0 && c.flushLen == 0 {
		return errors.New("batchPeriod or batchLen must be non-zero")
	}

	// Flushing of the batch is async, so avoid adding too many to the batch
	// by waiting a bit if the batch is full.
	waitFlush := func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()

		return len(c.buf) >= c.flushLen
	}

	for waitFlush() {
		time.Sleep(time.Microsecond)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.err != nil {
		err := c.err
		c.err = nil
		return err
	}

	if len(c.buf) == 0 {
		c.ctx = ctx
		c.fate = f
		c.start = time.Now()
	}

	if !c.flushStarted {
		go c.flushForever()
	}

	c.buf = append(c.buf, e)
	batchConsumerBufferLength.WithLabelValues(c.name).Set(float64(len(c.buf)))

	select {
	case c.trigger <- struct{}{}:
	default:
	}

	return nil
}

func (c *BatchConsumer) flushForever() {
	for {
		select {
		case <-time.After(flushSleep):
		case <-c.trigger:
		}

		c.mu.Lock()

		reachedPeriod := !c.start.IsZero() && c.flushPeriod != 0 && time.Since(c.start) >= c.flushPeriod
		reachedSize := c.flushLen != 0 && len(c.buf) >= c.flushLen
		if !reachedPeriod && !reachedSize {
			// Not time yet, sleep some more.
			c.mu.Unlock()
			continue
		}

		// Time to flush
		buf := c.buf
		c.buf = nil
		batchConsumerBufferLength.WithLabelValues(c.name).Set(0)
		c.start = time.Time{}

		var (
			b    Batch
			last *AckEvent
		)
		for _, e := range buf {
			b = append(b, &e.Event)
			last = e
		}

		err := c.consume(c.ctx, c.fate, b)
		if err != nil {
			log.Error(c.ctx, errors.Wrap(err, "batch consumer error"))
			c.err = err
		} else if err = last.Ack(c.ctx); err != nil {
			log.Error(c.ctx, errors.Wrap(err, "batch ack error"))
			c.err = err
		}

		c.mu.Unlock()
	}
}

// NewBatchConsumer returns a new BatchConsumer. Either batchPeriod or batchLen
// must be configured (non-zero).
func NewBatchConsumer(name string, cstore reflex.CursorStore,
	consume func(context.Context, fate.Fate, Batch) error,
	batchPeriod time.Duration, batchLen int,
	opts ...reflex.ConsumerOption) *BatchConsumer {

	bc := &BatchConsumer{
		consume:     consume,
		trigger:     make(chan struct{}, 1),
		flushPeriod: batchPeriod,
		flushLen:    batchLen,
	}

	fn := func(ctx context.Context, f fate.Fate, e *AckEvent) error {
		return bc.enqueue(ctx, f, e)
	}

	bc.AckConsumer = NewAckConsumer(name, cstore, fn, opts...)

	return bc
}

// NewBatchSpec returns a reflex spec for the AckConsumer.
func NewBatchSpec(stream reflex.StreamFunc, bc *BatchConsumer,
	opts ...reflex.StreamOption) reflex.Spec {

	c := &resetConsumer{
		Consumer: reflex.NewConsumer(bc.name, bc.Consume, bc.opts...),
		reset:    bc.Reset,
	}
	return reflex.NewSpec(stream, &noSetStore{bc.cstore}, c, opts...)
}

type resetConsumer struct {
	reflex.Consumer
	reset func() error
}

func (r *resetConsumer) Reset() error {
	return r.reset()
}
