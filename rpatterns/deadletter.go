package rpatterns

import (
	"context"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"

	"github.com/luno/reflex"
)

// NewDeadLetterConsumer returns a reflex consumer that records but ignores errors after
// the provided number of retries and therefore eventually continues to the next event.
// However, if the consumer cannot record the error it will return the error in a blocking
// fashion like a standard consumer.
func NewDeadLetterConsumer(name string, retries int, fn reflex.ConsumerFunc, eFn reflex.ErrorInsertFunc,
	opts ...reflex.ConsumerOption,
) reflex.Consumer {
	dl := &deadLetter{
		name:     name,
		inner:    fn,
		retries:  retries,
		inserter: eFn,
	}

	return reflex.NewConsumer(name, dl.consume, opts...)
}

type deadLetter struct {
	name     string
	inner    reflex.ConsumerFunc
	retries  int
	inserter reflex.ErrorInsertFunc

	retryID    string
	retryCount int
}

func (d *deadLetter) consume(ctx context.Context, e *reflex.Event) error {
	err := d.inner(ctx, e)
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
		return d.record(ctx, e, err)
	}

	return err
}

func (d *deadLetter) record(ctx context.Context, ev *reflex.Event, err error) error {
	// This code will panic if a nil event or error is passed to it.
	if reflex.IsExpected(err) {
		log.Info(ctx, "dead letter consumer ignoring error",
			log.WithError(errors.Wrap(err, "", j.MKS{"consumer": d.name, "eventID": ev.ID, "errMsg": err.Error()})))
	} else if iErr := d.inserter(ctx, ev.ID, d.name, err.Error()); iErr != nil {
		log.Error(ctx, errors.Wrap(err, "dead letter consumer cannot record an error"),
			j.MKS{"consumer": d.name, "eventID": ev.ID, "errMsg": err.Error(), "recErrMsg": iErr.Error()})
		return err
	}
	return nil
}

func (d *deadLetter) reset(id string) {
	d.retryID = id
	d.retryCount = 0
}
