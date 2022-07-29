package rpatterns

import (
	"context"
	"strconv"
	"testing"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/assert"

	"github.com/luno/reflex"
)

type wrapConsume struct {
	name    string
	consume func(context.Context, fate.Fate, *reflex.Event) error
}

func (w wrapConsume) Name() string {
	return w.name
}

func (w wrapConsume) Consume(ctx context.Context, f fate.Fate, e *reflex.Event) error {
	if w.consume != nil {
		return w.consume(ctx, f, e)
	}
	return nil
}

func TestConcurrentConsumer(t *testing.T) {
	cs := MemCursorStore()

	cons := NewConcurrentConsumer(cs, wrapConsume{name: "test"})

	err := cons.Reset()
	jtest.RequireNil(t, err)

	ctx := context.Background()
	f := fate.New()

	for i := int64(1); i <= 1000; i++ {
		id := strconv.FormatInt(i, 10)
		err := cons.Consume(ctx, f, &reflex.Event{ID: id})
		jtest.RequireNil(t, err)
	}

	cons.stop()

	curs, err := cs.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "1000", curs)
}

func TestErrorPropagatedToConsumeEventually(t *testing.T) {
	cs := MemCursorStore()

	theError := errors.New("error")

	errorOnTwo := wrapConsume{name: "test",
		consume: func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
			if e.IDInt() == 2 {
				return theError
			}
			return nil
		},
	}

	cons := NewConcurrentConsumer(cs, errorOnTwo)

	err := cons.Reset()
	jtest.RequireNil(t, err)

	ctx := context.Background()
	f := fate.New()

	err = cons.Consume(ctx, f, &reflex.Event{ID: "1"})
	jtest.RequireNil(t, err)

	err = cons.Consume(ctx, f, &reflex.Event{ID: "2"})
	// Processed asynchronously so still returns nil
	jtest.RequireNil(t, err)

	// Stop so that we are sure that we've processed the error
	cons.stop()
	// Should normally be calling Reset next, but we want to check that error

	err = cons.Consume(ctx, f, &reflex.Event{ID: "3"})
	jtest.Require(t, theError, err)

	curs, err := cs.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "1", curs)
}

func TestResetClearsError(t *testing.T) {
	cs := MemCursorStore()

	var returnNil bool
	consumeError := wrapConsume{name: "test",
		consume: func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
			if !returnNil {
				return errors.New("error")
			}
			return nil
		},
	}

	cons := NewConcurrentConsumer(cs, consumeError)

	err := cons.Reset()
	jtest.RequireNil(t, err)

	ctx := context.Background()
	f := fate.New()

	err = cons.Consume(ctx, f, &reflex.Event{ID: "1"})
	jtest.RequireNil(t, err)

	err = cons.Reset()
	jtest.RequireNil(t, err)

	// Allow the Consumer to work
	returnNil = true

	err = cons.Consume(ctx, f, &reflex.Event{ID: "1"})
	jtest.RequireNil(t, err)

	cons.stop()

	curs, err := cs.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "1", curs)
}

func TestGapIsIgnored(t *testing.T) {
	cs := MemCursorStore()

	cons := NewConcurrentConsumer(cs, wrapConsume{name: "test"})

	err := cons.Reset()
	jtest.RequireNil(t, err)

	ctx := context.Background()
	f := fate.New()

	for i := 0; i < 100; i++ {
		if i == 70 {
			continue
		}
		err = cons.Consume(ctx, f, &reflex.Event{ID: strconv.Itoa(i)})
		jtest.RequireNil(t, err)
	}

	cons.stop()

	curs, err := cs.GetCursor(ctx, "test")
	jtest.RequireNil(t, err)
	assert.Equal(t, "99", curs)
}
