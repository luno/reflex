package rpatterns

import (
	"context"
	"strconv"
	"sync"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

type eventReturn struct {
	EventID int64
	Err     error
}

// ConcurrentConsumer will consume up to maxInFlight events concurrently.
// When an event has been consumed without error a new one will be picked up.
// This consumer is ideal when you have independent events that take a significant amount
// of time to process. Note that events for the same entity (foreign_id) can be
// processed out of order.
// The cursor is updated only when all events before it have been completed without error.
// TODO(adam): Add more options or make compatible with other Consumers
type ConcurrentConsumer struct {
	consumer reflex.Consumer
	cStore   reflex.CursorStore

	inFlightWait sync.WaitGroup
	inFlight     chan int64
	maxInFlight  int

	doneEvents chan eventReturn
	bgLoop     sync.WaitGroup

	consumeErrorMtx sync.RWMutex
	consumeError    error
}

// NewConcurrentConsumer creates a new consumer with an inflight buffer of 100 events
func NewConcurrentConsumer(
	cStore reflex.CursorStore,
	consumer reflex.Consumer,
) *ConcurrentConsumer {
	return &ConcurrentConsumer{
		cStore:      cStore,
		consumer:    consumer,
		maxInFlight: 100,
	}
}

// Name returns the consumer name.
func (c *ConcurrentConsumer) Name() string {
	return c.consumer.Name()
}

func (c *ConcurrentConsumer) stop() {
	// Wait for consumers to finish
	c.inFlightWait.Wait()
	if c.doneEvents != nil {
		// Close the channel so the background loop terminates
		close(c.doneEvents)
	}
	// Wait for the background loop
	c.bgLoop.Wait()
	c.doneEvents = nil
}

func (c *ConcurrentConsumer) setError(err error) {
	c.consumeErrorMtx.Lock()
	defer c.consumeErrorMtx.Unlock()
	c.consumeError = err
}

func (c *ConcurrentConsumer) getError() error {
	c.consumeErrorMtx.RLock()
	defer c.consumeErrorMtx.RUnlock()
	return c.consumeError
}

// Reset stops processing any current events and restarts the consumer ready to start consuming again.
func (c *ConcurrentConsumer) Reset() error {
	// Stop processing inflight events
	c.stop()
	c.setError(nil)

	ctx := context.Background()
	// Flush the cursor in case there's any updates waiting
	err := c.cStore.Flush(ctx)
	if err != nil {
		return err
	}

	// Start the new background loop
	c.inFlight = make(chan int64, c.maxInFlight)
	c.doneEvents = make(chan eventReturn, c.maxInFlight)
	c.bgLoop.Add(1)
	go func() {
		c.updateCursorForever()
		c.bgLoop.Done()
	}()
	return nil
}

// Consume is used by reflex to feed events into the consumer
func (c *ConcurrentConsumer) Consume(ctx context.Context, e *reflex.Event) error {
	// Check if a previous event errored, in which case we need to stop/reset
	if err := c.getError(); err != nil {
		return err
	}

	c.inFlight <- e.IDInt()
	c.inFlightWait.Add(1)
	go func() {
		defer c.inFlightWait.Done()
		ret := eventReturn{EventID: e.IDInt()}
		err := c.consumer.Consume(ctx, e)
		if err != nil {
			// NoReturnErr: Populate the returned error
			ret.Err = err
		}
		c.doneEvents <- ret
	}()
	return nil
}

func (c *ConcurrentConsumer) updateCursorForever() {
	ctx := context.Background()
	gs := NewGapSequence()

	for ret := range c.doneEvents {
		// Allow another event to go in-flight
		// Receive the earliest event ID and set it as Doing
		// We won't update the cursor until this event ID is Done
		gs.Doing(<-c.inFlight)
		if ret.Err != nil {
			// NoReturnErr: Need to set the error to be returned on Consume
			// Set the event ID here because it will be returned on a different Consume
			err := errors.Wrap(ret.Err, "", j.MKV{"error_event_id": ret.EventID})
			c.setError(err)
			continue
		}
		pre := gs.CurrentMax()
		gs.Done(ret.EventID)
		post := gs.CurrentMax()
		if post > pre {
			err := c.cStore.SetCursor(ctx, c.consumer.Name(), strconv.FormatInt(post, 10))
			if err != nil {
				// NoReturnErr: Non-fatal error on updating the cursor needs logging though
				log.Error(ctx, err)
			}
		}
	}
}

// NewConcurrentSpec wraps the ConcurrentConsumer in a reflex.Spec
func NewConcurrentSpec(
	stream reflex.StreamFunc,
	rac *ConcurrentConsumer,
	opts ...reflex.StreamOption,
) reflex.Spec {
	return reflex.NewSpec(stream, &noSetStore{rac.cStore}, rac, opts...)
}
