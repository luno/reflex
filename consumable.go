package reflex

import (
	"context"
)

// Deprecated: Please use Run.
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

// Deprecated: Please use Run.
func (c *consumable) Consume(in context.Context, consumer Consumer, opts ...StreamOption) error {
	return Run(in, NewSpec(c.sFn, c.cstore, consumer, opts...))
}
