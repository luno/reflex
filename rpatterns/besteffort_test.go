package rpatterns_test

import (
	"context"
	"testing"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/rpatterns"
	"github.com/stretchr/testify/require"
)

func TestBestEffortConsumer(t *testing.T) {
	consumerErr := errors.New("consumer error")

	cases := []struct {
		name           string
		errorsPerEvent int
		retries        int
		input          int
		expected       []error
	}{
		{
			name:           "0 retries 0 errors",
			errorsPerEvent: 0,
			retries:        0,
			input:          3,
			expected:       []error{nil, nil, nil},
		},
		{
			name:           "0 retries 1 error",
			errorsPerEvent: 1,
			retries:        0,
			input:          3,
			expected:       []error{nil, nil, nil},
		},
		{
			name:           "1 retry 1 error",
			errorsPerEvent: 1,
			retries:        1,
			input:          4,
			expected:       []error{consumerErr, nil, consumerErr, nil},
		},
		{
			name:           "0 retries 1 error",
			errorsPerEvent: 1,
			retries:        0,
			input:          3,
			expected:       []error{nil, nil, nil},
		},
		{
			name:           "3 retries 2 errors",
			errorsPerEvent: 2,
			retries:        3,
			input:          6,
			expected:       []error{consumerErr, consumerErr, nil, consumerErr, consumerErr, nil},
		},
	}

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			events := make(map[string]int)
			fn := func(_ context.Context, e *reflex.Event) error {
				events[e.ID]++
				if events[e.ID] <= test.errorsPerEvent {
					return consumerErr
				}
				return nil
			}

			var actual []error
			c := rpatterns.NewBestEffortConsumer("test", test.retries, fn)
			next := 1
			for i := 0; i < test.input; i++ {
				err := c.Consume(context.TODO(), ItoE(next))
				actual = append(actual, err)
				if err == nil {
					next++
				}
			}

			require.EqualValues(t, test.expected, actual)
		})
	}
}
