package reflex_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/stretchr/testify/assert"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
)

type TestEventType int

func (t TestEventType) ReflexType() int {
	return int(t)
}

func TestConsumable(t *testing.T) {
	doneErr := errors.New("done")
	endErr := errors.New("end")
	const name = "tester"

	cases := []struct {
		name           string
		initEvents     []int
		startCursor    int
		outEvents      []int
		opts           []reflex.StreamOption
		streamEndError error
		fErrors        []error
		outErrMsg      string
		finalCursor    int
		timeout        bool
	}{
		{
			name:       "no events",
			initEvents: []int{},
			outErrMsg:  "recv error: context canceled",
			timeout:    true,
		}, {
			name:           "no events end error",
			initEvents:     []int{},
			streamEndError: endErr,
			outErrMsg:      "recv error: end",
			timeout:        false,
		}, {
			name:        "basic with timeout",
			initEvents:  []int{1, 2, 3, 4},
			outEvents:   []int{1, 2, 3, 4},
			finalCursor: 4,
			outErrMsg:   "recv error: context canceled",
			timeout:     true,
		}, {
			name:        "basic with done error",
			initEvents:  []int{1, 2, 3, 4},
			outEvents:   []int{1, 2, 3, 4},
			fErrors:     []error{nil, nil, nil, doneErr},
			finalCursor: 3,
			outErrMsg:   "consume error: done",
			timeout:     false,
		}, {
			name:           "basic with end error",
			initEvents:     []int{1, 2, 3, 4},
			outEvents:      []int{1, 2, 3, 4},
			streamEndError: endErr,
			finalCursor:    4,
			outErrMsg:      "recv error: end",
			timeout:        false,
		}, {
			name:           "offset with end error",
			initEvents:     []int{1, 2, 3, 4},
			startCursor:    2,
			outEvents:      []int{3, 4},
			streamEndError: endErr,
			finalCursor:    4,
			outErrMsg:      "recv error: end",
			timeout:        false,
		}, {
			name:        "early done error",
			initEvents:  []int{1, 2, 3, 4},
			outEvents:   []int{1, 2},
			fErrors:     []error{nil, doneErr},
			finalCursor: 1,
			outErrMsg:   "consume error: done",
			timeout:     false,
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			cstore := rpatterns.MemCursorStore()
			_ = cstore.SetCursor(nil, name, strconv.Itoa(test.startCursor))
			streamer := newMockStreamer(ItoEList(test.initEvents...), test.streamEndError)

			consumable := reflex.NewConsumable(streamer.Stream, cstore)

			var results []*reflex.Event
			f := func(ctx context.Context, e *reflex.Event) error {
				results = append(results, e)
				if len(test.fErrors) == 0 {
					return nil
				}
				err := test.fErrors[0]
				test.fErrors = test.fErrors[1:]
				return err
			}

			ctx, cancel := context.WithCancel(context.Background())

			// cancel consume after block
			t0 := time.Now()
			timeout := time.Millisecond * 100
			go func() {
				time.Sleep(timeout)
				cancel()
			}()

			consumer := reflex.NewConsumer(name, f)

			err := consumable.Consume(ctx, consumer, test.opts...)
			assert.EqualError(t, err, test.outErrMsg)

			timedOut := time.Now().After(t0.Add(timeout))
			assert.Equal(t, test.timeout, timedOut)

			if !assert.Len(t, results, len(test.outEvents)) {
				return
			}

			for i, e := range test.outEvents {
				actual := results[i]
				expected := ItoE(e)
				assert.Equal(t, expected.ID, actual.ID)
				assert.Equal(t, expected.ForeignID, actual.ForeignID)
				assert.Equal(t, expected.Type, actual.Type)
			}

			cursor, err := cstore.GetCursor(nil, name)
			assert.NoError(t, err)
			assert.Equal(t, strconv.Itoa(test.finalCursor), cursor)
		})
	}
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
		Type:      TestEventType(i),
	}
}
