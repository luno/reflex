package rpatterns

import (
	"context"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// DeadLetterFunc abstracts the recording an error with an event.
type DeadLetterFunc func(ctx context.Context, consumer string, eventID string, errMsg string) error

// NewDeadLetterConsumer returns a reflex consumer that records but ignores errors after
// the provided number of retries and therefore eventually continues to the next event.
// However, if the consumer cannot record the error it will return the error in a blocking
// fashion like a standard consumer.
func NewDeadLetterConsumer(name string, retries int, cFn reflex.ConsumerFunc, dFn DeadLetterFunc,
	opts ...reflex.ConsumerOption,
) reflex.Consumer {
	dl := &deadLetter{
		name:    name,
		process: cFn,
		retries: retries,
		record:  dFn,
	}

	return reflex.NewConsumer(name, dl.consume, opts...)
}

type deadLetter struct {
	name    string
	process reflex.ConsumerFunc
	retries int
	record  DeadLetterFunc

	retryID    string
	retryCount int
}

func (d *deadLetter) consume(ctx context.Context, e *reflex.Event) error {
	err := d.process(ctx, e)
	if err == nil {
		d.reset("")
		return nil
	}
	if d.retryID != e.ID {
		d.reset(e.ID)
	}

	d.retryCount++
	if d.retryCount > d.retries {
		d.reset("")
		return d.offload(ctx, e, err)
	}

	return err
}

func (d *deadLetter) offload(ctx context.Context, e *reflex.Event, err error) error {
	if reflex.IsExpected(err) {
		log.Info(ctx, "dead letter consumer ignoring error",
			log.WithError(errors.Wrap(err, "", j.MKS{"consumer": d.name, "eventID": e.ID, "errMsg": err.Error()})))
	} else if iErr := d.record(ctx, d.name, e.ID, err.Error()); iErr != nil {
		log.Error(ctx, errors.Wrap(err, "dead letter consumer cannot record an error"),
			j.MKS{"consumer": d.name, "eventID": e.ID, "errMsg": err.Error(), "recErrMsg": iErr.Error()})
		return err
	}
	return nil
}

func (d *deadLetter) reset(id string) {
	d.retryID = id
	d.retryCount = 0
}
