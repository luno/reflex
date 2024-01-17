package rpatterns_test

import (
	"context"
	"testing"

	"github.com/luno/jettison/jtest"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/require"
)

var (
	consumerErr   = errors.New("consumer error")
	deadLetterErr = errors.New("dead letter error")
)

func makeExpected(size int) []string {
	var exp []string
	for i := 0; i < size; i++ {
		exp = append(exp, consumerErr.Error())
	}
	return exp
}

func TestDeadLetterConsumer(t *testing.T) {
	cases := []struct {
		name           string
		errorsPerEvent int
		retries        int
		ids            []int
		expected       []string
		consumeErr     error
		writeErr       error
		expErr         []error
	}{
		{
			name:           "0 retries 0 errors",
			errorsPerEvent: 0,
			retries:        0,
			ids:            []int{1, 2, 3},
			consumeErr:     consumerErr,
			expErr:         []error{nil, nil, nil},
		},
		{
			name:           "0 retries 1 error",
			errorsPerEvent: 1,
			retries:        0,
			ids:            []int{1, 2, 3},
			expected:       makeExpected(3),
			consumeErr:     consumerErr,
			expErr:         []error{nil, nil, nil},
		},
		{
			name:           "0 retries 1 error but expected",
			errorsPerEvent: 1,
			retries:        0,
			ids:            []int{1, 2, 3},
			consumeErr:     reflex.ErrStopped,
			expErr:         []error{nil, nil, nil},
		},
		{
			name:           "0 retries 1 error failed to write",
			errorsPerEvent: 1,
			retries:        0,
			ids:            []int{1, 2, 3},
			consumeErr:     consumerErr,
			writeErr:       deadLetterErr,
			expErr:         []error{consumerErr, consumerErr, consumerErr},
		},
		{
			name:           "1 retry 1 error",
			errorsPerEvent: 1,
			retries:        1,
			ids:            []int{1, 1, 2, 2, 3, 3, 4, 4},
			consumeErr:     consumerErr,
			expErr:         []error{consumerErr, nil, consumerErr, nil, consumerErr, nil, consumerErr, nil},
		},
		{
			name:           "2 retries 4 errors",
			errorsPerEvent: 4,
			retries:        2,
			ids:            []int{1, 1, 1, 2, 2, 2},
			expected:       makeExpected(2),
			consumeErr:     consumerErr,
			expErr:         []error{consumerErr, consumerErr, nil, consumerErr, consumerErr, nil},
		},
		{
			name:           "2 retries 4 errors would fail to write",
			errorsPerEvent: 4,
			retries:        2,
			ids:            []int{1, 1, 1, 2, 2, 2},
			// expected:       makeExpected(2),
			consumeErr: consumerErr,
			writeErr:   deadLetterErr,
			expErr:     []error{consumerErr, consumerErr, consumerErr, consumerErr, consumerErr, consumerErr},
		},
		{
			name:           "3 retries 2 errors",
			errorsPerEvent: 2,
			retries:        3,
			ids:            []int{1, 1, 1, 2, 2, 2},
			consumeErr:     consumerErr,
			expErr:         []error{consumerErr, consumerErr, nil, consumerErr, consumerErr, nil},
		},
		{
			name:           "3 retries 2 errors would fail to write",
			errorsPerEvent: 2,
			retries:        3,
			ids:            []int{1, 1, 1, 2, 2, 2},
			consumeErr:     consumerErr,
			writeErr:       deadLetterErr,
			expErr:         []error{consumerErr, consumerErr, nil, consumerErr, consumerErr, nil},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			events := make(map[string]int)
			fn := func(_ context.Context, e *reflex.Event) error {
				events[e.ID]++
				if events[e.ID] <= test.errorsPerEvent {
					return test.consumeErr
				}
				return nil
			}

			var actual []string
			eFn := func(ctx context.Context, consumer string, eventID string, errMsg string) error {
				if test.writeErr == nil {
					actual = append(actual, errMsg)
				}
				return test.writeErr
			}
			c := rpatterns.NewDeadLetterConsumer("test", test.retries, fn, eFn)
			for i, id := range test.ids {
				err := c.Consume(context.TODO(), ItoE(id))
				jtest.Require(t, test.expErr[i], err)
			}
			require.EqualValues(t, test.expected, actual)
		})
	}
}
