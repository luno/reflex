package reflex

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_optsToProto(t *testing.T) {
	tests := []struct {
		Name   string
		Input  []StreamOption
		Count  int
		Output StreamOptions
	}{
		{
			Name: "zero",
		},
		{
			Name:  "zero lag",
			Input: []StreamOption{WithStreamLag(0)},
		},
		{
			Name:   "non-zero lag",
			Input:  []StreamOption{WithStreamLag(time.Minute)},
			Output: StreamOptions{Lag: time.Minute},
			Count:  1,
		},
		{
			Name:   "from head",
			Input:  []StreamOption{WithStreamFromHead()},
			Output: StreamOptions{StreamFromHead: true},
			Count:  1,
		},
		{
			Name:   "lag and head",
			Input:  []StreamOption{WithStreamFromHead(), WithStreamLag(time.Second)},
			Output: StreamOptions{StreamFromHead: true, Lag: time.Second},
			Count:  2,
		},
		{
			Name:   "to head",
			Input:  []StreamOption{WithStreamToHead()},
			Output: StreamOptions{StreamToHead: true},
			Count:  1,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {

			pb, err := optsToProto(test.Input)
			require.NoError(t, err)

			opts := optsFromProto(pb)

			require.Len(t, opts, test.Count)

			var res StreamOptions
			for _, opt := range opts {
				opt(&res)
			}

			require.Equal(t, test.Output, res)
		})
	}

}
