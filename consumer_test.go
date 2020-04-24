package reflex_test

import (
	"context"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/mock"
	"github.com/stretchr/testify/assert"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
)

func TestWithConsumerBestEffort(t *testing.T) {
	endErr := errors.New("end")
	consumerErr := errors.New("consumer error")

	cases := []struct {
		name         string
		isBestEffort bool
		consumeErr   error
		expectedErr  error
	}{
		{
			name:         "best effort consumer with error",
			isBestEffort: true,
			consumeErr:   consumerErr,
			expectedErr:  endErr,
		},
		{
			name:        "default consumer with error",
			consumeErr:  consumerErr,
			expectedErr: consumerErr,
		},
		{
			name:         "best effort consumer without error",
			isBestEffort: true,
			expectedErr:  endErr,
		},
		{
			name:        "default consumer without error",
			expectedErr: endErr,
		},
	}

	for _, test := range cases {
		t.Run(t.Name(), func(t *testing.T) {

			var consumer reflex.Consumer
			if test.isBestEffort {
				consumer = reflex.NewConsumer("test", func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
					return test.consumeErr
				}, reflex.WithConsumerBestEffort())
			} else {
				consumer = reflex.NewConsumer("test", func(ctx context.Context, fate fate.Fate, event *reflex.Event) error {
					return test.consumeErr
				})
			}

			cs := mock.NewMockCStore()
			stream := newMockStreamer(ItoEList(1), endErr).Stream
			spec := reflex.NewSpec(stream, cs, consumer)

			err := reflex.Run(context.Background(), spec)
			assert.True(t, errors.Is(err, test.expectedErr))
		})
	}
}
