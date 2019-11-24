package rpatterns

import (
	"context"

	"github.com/luno/reflex"
)

type bootstrapper struct {
	cstore reflex.CursorStore
	stream reflex.StreamFunc
	opts   []reflex.StreamOption

	lastCursorZero bool
}

func (b *bootstrapper) GetCursor(ctx context.Context, consumerName string) (string, error) {
	cursor, err := b.cstore.GetCursor(ctx, consumerName)
	if err != nil {
		return "", err
	}
	b.lastCursorZero = cursor == ""
	return cursor, nil
}

func (b *bootstrapper) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	return b.cstore.SetCursor(ctx, consumerName, cursor)
}

func (b *bootstrapper) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	if b.lastCursorZero {
		opts = append(opts, reflex.WithStreamFromHead())
	}
	return b.stream(ctx, after, opts...)
}

func (b *bootstrapper) Flush(ctx context.Context) error {
	return b.cstore.Flush(ctx)
}

// NewBootstrapSpec returns a reflex spec that will start streaming
// from head if no cursor is found. This is useful if old events should be skipped
// for new consumers. Once running (bootstrapped), one can safely revert to reflex.NewSpec.
func NewBootstrapSpec(stream reflex.StreamFunc, cstore reflex.CursorStore,
	consumer reflex.Consumer, opts ...reflex.StreamOption) reflex.Spec {
	b := &bootstrapper{
		stream: stream,
		cstore: cstore,
		opts:   opts,
	}
	return reflex.NewSpec(b.Stream, b, consumer, opts...)
}

// NewBootstrapConsumable returns a reflex consumable that will start streaming
// from head if no cursor is found. This is useful if old events should be skipped
// for new consumers. Once running (bootstrapped), one can safely revert to reflex.NewConsumable.
// Deprecated: Use NewBootstrapConsumeReq.
func NewBootstrapConsumable(sFn reflex.StreamFunc, cstore reflex.CursorStore,
	opts ...reflex.StreamOption) reflex.Consumable {
	b := &bootstrapper{
		stream: sFn,
		cstore: cstore,
		opts:   opts,
	}
	return reflex.NewConsumable(b.Stream, b)
}
