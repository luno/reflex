package rpatterns_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/assert"
)

var errCursor = errors.New("no more cursors", j.C("ERR_547c6f344dce327a"))
var errEvents = errors.New("no more events", j.C("ERR_a3eac05795da49d2"))

func TestBootstrap(t *testing.T) {
	cases := []struct {
		name         string
		gets         []string
		inEvents     []int
		sets         []string
		afters       []string
		opts         []reflex.StreamOption
		consumerErr1 string
		consumerErr2 string
	}{
		{
			name:         "one zero cursor",
			gets:         []string{"", "2"},
			inEvents:     []int{1, 2, 3, 4},
			sets:         []string{"1", "2", "3", "4"},
			afters:       []string{"", "2"},
			opts:         []reflex.StreamOption{reflex.WithStreamFromHead()},
			consumerErr1: "recv error: no more events",
			consumerErr2: "recv error: no more events",
		}, {
			name:         "two zero cursors",
			gets:         []string{"", ""},
			inEvents:     []int{1, 2, 3, 4},
			sets:         []string{"1", "2", "3", "4"},
			afters:       []string{"", ""},
			opts:         []reflex.StreamOption{reflex.WithStreamFromHead(), reflex.WithStreamFromHead()},
			consumerErr1: "recv error: no more events",
			consumerErr2: "recv error: no more events",
		}, {
			name:         "zero zero cursors",
			gets:         []string{"2", "3"},
			inEvents:     []int{1, 2, 3, 4},
			sets:         []string{"1", "2", "3", "4"},
			opts:         []reflex.StreamOption{},
			afters:       []string{"2", "3"},
			consumerErr1: "recv error: no more events",
			consumerErr2: "recv error: no more events",
		}, {
			name:         "cursor get errors",
			inEvents:     []int{1, 2, 3, 4},
			opts:         []reflex.StreamOption{},
			consumerErr1: "get cursor error: no more cursors",
			consumerErr2: "get cursor error: no more cursors",
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {

			var results []*reflex.Event
			consumer := reflex.NewConsumer("test",
				func(ctx context.Context, f fate.Fate, e *reflex.Event) error {
					results = append(results, e)
					return nil
				})

			b := new(bootstrapMock)
			b.gets = test.gets
			b.events = ItoEList(test.inEvents...)
			consumable := rpatterns.NewBootstrapConsumable(b.Stream, b)

			err := consumable.Consume(context.Background(), consumer)
			assert.EqualError(t, err, test.consumerErr1)

			err = consumable.Consume(context.Background(), consumer)
			assert.EqualError(t, err, test.consumerErr2)

			assert.EqualValues(t, test.sets, b.sets)
			assert.EqualValues(t, test.afters, b.afters)
			assert.Len(t, b.opts, len(test.opts))
			for i, opt := range b.opts {
				assert.Equal(t, fmt.Sprint(test.opts[i]), fmt.Sprint(opt))
			}
		})
	}
}

type bootstrapMock struct {
	gets []string
	sets []string

	events  []*reflex.Event
	opts    []reflex.StreamOption
	afters  []string
	flushes int
}

func (b *bootstrapMock) Recv() (*reflex.Event, error) {
	if len(b.events) == 0 {
		return nil, errEvents
	}
	e := b.events[0]
	b.events = b.events[1:]
	return e, nil
}

func (b *bootstrapMock) GetCursor(ctx context.Context, consumerName string) (string, error) {
	if len(b.gets) == 0 {
		return "", errCursor
	}
	c := b.gets[0]
	b.gets = b.gets[1:]
	return c, nil
}

func (b *bootstrapMock) SetCursor(ctx context.Context, consumerName string, cursor string) error {
	b.sets = append(b.sets, cursor)
	return nil
}

func (b *bootstrapMock) Flush(ctx context.Context) error {
	b.flushes++
	return nil
}

func (b *bootstrapMock) Stream(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error) {
	b.opts = append(b.opts, opts...)
	b.afters = append(b.afters, after)
	return b, nil
}

func ItoEList(il ...int) []*reflex.Event {
	var res []*reflex.Event
	for _, i := range il {
		res = append(res, ItoE(i))
	}
	return res
}

func ItoE(i int) *reflex.Event {
	return &reflex.Event{
		ID:        strconv.Itoa(i),
		ForeignID: strconv.Itoa(i),
		Type:      testEventType(i),
	}
}
