package rpatterns

import (
	"context"
	"testing"

	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
)

func TestIsExpected(t *testing.T) {
	tests := []struct {
		Name     string
		Err      error
		Expected bool
	}{
		{
			Name:     "nil",
			Err:      nil,
			Expected: false,
		}, {
			Name:     "fate",
			Err:      fate.ErrTempt,
			Expected: true,
		}, {
			Name:     "context.Canceled",
			Err:      context.Canceled,
			Expected: true,
		}, {
			Name:     "context.DeadlineExceeded",
			Err:      context.DeadlineExceeded,
			Expected: true,
		}, {
			Name:     "reflex.ErrStopped",
			Err:      reflex.ErrStopped,
			Expected: true,
		}, {
			Name:     "Canceled status",
			Err:      status.FromContextError(context.Canceled).Err(),
			Expected: true,
		},
		{
			Name:     "DeadlineExceeded status",
			Err:      status.FromContextError(context.DeadlineExceeded).Err(),
			Expected: true,
		},
		{
			Name:     "not me",
			Err:      errors.New("not me"),
			Expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			require.Equal(t, test.Expected, isExpected(test.Err))
		})
	}
}
