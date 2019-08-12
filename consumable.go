package reflex

import (
	"context"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
)

func NewConsumable(sFn StreamFunc, cstore CursorStore, opts ...StreamOption) Consumable {
	return &consumable{
		sFn:    sFn,
		cstore: cstore,
		opts:   opts,
	}
}

type consumable struct {
	sFn    StreamFunc
	cstore CursorStore
	opts   []StreamOption
}

func (c *consumable) Consume(in context.Context, consumer Consumer, opts ...StreamOption) error {
	ctx, cancel := context.WithCancel(in)
	defer cancel()
	defer c.cstore.Flush(context.Background()) // best effort flush with new context

	cursor, err := c.cstore.GetCursor(ctx, consumer.Name().String())
	if err != nil {
		return errors.Wrap(err, "get cursor error")
	}

	s, err := c.sFn(ctx, cursor, append(c.opts, opts...)...)
	if err != nil {
		return errors.Wrap(err, "streamer error")
	}

	for {
		e, err := s.Recv()
		if err != nil {
			return errors.Wrap(err, "recv error")
		}

		if err := consumer.Consume(ctx, fate.New(), e); err != nil {
			return errors.Wrap(err, "consumer error")
		}

		if err := c.cstore.SetCursor(ctx, consumer.Name().String(), e.ID); err != nil {
			return errors.Wrap(err, "set cursor error")
		}
	}
}
