package rpatterns

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGapSequence(t *testing.T) {
	testCases := []struct {
		name  string
		doing []int64
		done  []int64

		expDoneMax []int64

		expMax int64
	}{
		{name: "initially zero",
			expMax: 0,
		},
		{name: "doing one sets max",
			doing: []int64{1},
			done:  []int64{1},

			expDoneMax: []int64{1},
			expMax:     1,
		},
		{name: "waits for done",
			doing: []int64{1, 2, 3, 4},
			done:  []int64{},

			expMax: 0,
		},
		{name: "done but wasn't doing throws error",
			doing: []int64{1},
			done:  []int64{1, 2},

			expDoneMax: []int64{1, 1},
			expMax:     1,
		},
		{name: "doing greater values waits for doing",
			doing: []int64{3, 4, 5},
			done:  []int64{3, 5, 4},

			expDoneMax: []int64{3, 3, 5},
			expMax:     5,
		},
		{name: "gaps are ignored",
			doing: []int64{3, 100, 101},
			done:  []int64{3, 101, 100},

			expDoneMax: []int64{3, 3, 101},
			expMax:     101,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			gs := NewGapSequence()
			for _, v := range tc.doing {
				gs.Doing(v)
			}
			for i, v := range tc.done {
				gs.Done(v)
				assert.Equal(t, tc.expDoneMax[i], gs.CurrentMax())
			}

			assert.Equal(t, tc.expMax, gs.CurrentMax())
		})
	}
}

func TestMultipleAdd(t *testing.T) {
	gs := NewGapSequence()
	for i := int64(10_000); i <= 20_000; i++ {
		gs.Doing(i)
	}
	for i := int64(10_002); i <= 20_000; i++ {
		gs.Done(i)
	}
	assert.Equal(t, int64(0), gs.CurrentMax())
	gs.Done(10_000)
	assert.Equal(t, int64(10_000), gs.CurrentMax())
	// Adding this value should pop everything from the waiting list
	gs.Done(10_001)
	assert.Equal(t, int64(20_000), gs.CurrentMax())
}
