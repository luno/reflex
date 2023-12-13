package rpatterns

import (
	"context"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// Batch is a batch of reflex events.
type Batch []*reflex.Event

type BatchConsumeFn func(context.Context, Batch) error

type batchEvent struct {
	ackEvent *AckEvent
}

const minWait = time.Millisecond * 100

var ErrBatchState = errors.New("batch error state", j.C("ERR_b3053f5f1a3ecd23"))

// BatchConsumer provides a reflex consumer that buffers events
// and flushes a batch to the consume function when either
// flushLen or flushPeriod is reached.
//
// It leverages the AckConsumer internally and acks
// (updates the cursor) after each batch is consumed.
//
// This consumer is stateful. If the underlying stream errors
// reflex needs to be reset it to clear its state. It therefore
// implements the resetter interface. The consumer also implements
// the stopper interface to stop the processing go-routine when the
// run completes.
//
// When the batch reaches capacity, the processing happens synchronously
// and the result is returned with the enqueue request. When the flush
// period is reached prior to batch capacity, processing happens asynchronously
// and the result will be returned when the next event is added to the queue.
// It assumes that the stream is reset to the previous cursor
// before sending subsequent events.
type BatchConsumer struct {
	*AckConsumer
	consume BatchConsumeFn

	flushPeriod time.Duration
	flushLen    int

	chEvent  chan batchEvent
	chResult chan error
	chExit   chan any

	ctx    context.Context
	cancel context.CancelFunc
}

// Reset ensures that the buffer and error is cleared
// enabling a clean run of the consumer whilst returning
// any errors that were found in the consumer.
func (c *BatchConsumer) Reset(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	c.chResult = make(chan error)
	c.chEvent = make(chan batchEvent)
	c.chExit = make(chan any)

	go func() {
		defer close(c.chExit)
		defer close(c.chResult)

		processEvents(
			c.ctx,
			c.chEvent,
			c.chResult,
			c.flushPeriod,
			c.flushLen,
			c.consume)
	}()
	return nil
}

func (c *BatchConsumer) Stop() error {
	c.cancel()

	// Wait for processing thread to exit
	<-c.chExit

	return nil
}

// enqueue adds the event to the buffer or returns error if batch needs to be reset.
func (c *BatchConsumer) enqueue(ctx context.Context, e *AckEvent) error {
	if c.flushPeriod == 0 && c.flushLen == 0 {
		return errors.New("batchPeriod or batchLen must be non-zero")
	}

	// Add event to batch queue
	select {
	case c.chEvent <- batchEvent{ackEvent: e}:
	case _, ok := <-c.chResult:
		if !ok {
			return ErrBatchState
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait for result
	select {
	case err := <-c.chResult:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processEvents receive events until buffer is full or flush period expired. Clear buffer once processed.
func processEvents(
	ctx context.Context,

	chEvents chan batchEvent,
	chResult chan error,

	flushPeriod time.Duration,
	flushLen int,

	fnConsume BatchConsumeFn,
) {
	var (
		batch      []*AckEvent
		flushTimer <-chan time.Time
	)

	for {
		var timerExpired bool

		select {
		case ev := <-chEvents:

			if len(batch) == 0 && flushPeriod != 0 {
				wait := ev.ackEvent.Timestamp.Add(flushPeriod).Sub(time.Now())

				// If the processor is running behind, set a minimum wait time to
				// avoid events being processed one-by-one
				if wait < 0 {
					wait = minWait
				}

				flushTimer = time.After(wait)
			}

			batch = append(batch, ev.ackEvent)
			if flushLen == 0 || len(batch) < flushLen {
				select {
				case chResult <- nil:
					continue
				case <-ctx.Done():
					return
				}
			}
		case <-flushTimer:
			timerExpired = true
		case <-ctx.Done():
			return
		}

		err := processBatch(ctx, batch, fnConsume)

		if !timerExpired || err != nil && timerExpired {
			// If flushTimer expired (background processing), terminate processing and set to error state
			if timerExpired {
				log.Error(ctx, errors.Wrap(err, "batch processing error"))
				return
			}

			select {
			case chResult <- err:
			case <-ctx.Done():
				return
			}
		}

		batch = nil
		flushTimer = nil
	}
}

func processBatch(ctx context.Context, batch []*AckEvent, fnConsume BatchConsumeFn) error {
	var b Batch
	for _, e := range batch {
		b = append(b, &e.Event)
	}
	last := batch[len(batch)-1]

	err := fnConsume(ctx, b)
	if err != nil {
		return errors.Wrap(err, "batch consumer error")
	}
	if err = last.Ack(ctx); err != nil {
		return errors.Wrap(err, "batch ack error")
	}

	return nil
}

// NewBatchConsumer returns a new BatchConsumer. Either batchPeriod or batchLen
// must be configured (non-zero).
func NewBatchConsumer(name string, cstore reflex.CursorStore,
	consume func(context.Context, Batch) error,
	batchPeriod time.Duration, batchLen int,
	opts ...reflex.ConsumerOption,
) *BatchConsumer {
	exit := make(chan any)
	close(exit)
	bc := &BatchConsumer{
		consume:     consume,
		flushPeriod: batchPeriod,
		flushLen:    batchLen,
		// Setup functions so that Stop works without a Reset
		chExit: exit,
		cancel: func() {},
	}

	fn := func(ctx context.Context, e *AckEvent) error {
		return bc.enqueue(ctx, e)
	}

	bc.AckConsumer = NewAckConsumer(name, cstore, fn, opts...)

	return bc
}

// NewBatchSpec returns a reflex spec for the AckConsumer.
func NewBatchSpec(
	stream reflex.StreamFunc,
	bc *BatchConsumer,
	opts ...reflex.StreamOption,
) reflex.Spec {
	c := &resetConsumer{
		Consumer: reflex.NewConsumer(bc.name, bc.Consume, bc.opts...),
		reset:    bc.Reset,
		stop:     bc.Stop,
	}

	return reflex.NewSpec(stream, &noSetStore{bc.cstore}, c, opts...)
}

type resetConsumer struct {
	reflex.Consumer
	reset func(ctx context.Context) error
	stop  func() error
}

func (r *resetConsumer) Reset(ctx context.Context) error {
	return r.reset(ctx)
}

func (r *resetConsumer) Stop() error {
	return r.stop()
}
