package reflex

import (
	"errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"testing"
)

var err1 = errors.New("error 1")
var err2 = errors.New("error 2")
var err3 = errors.New("error 3")

func trueFilter(err error) EventFilter {
	return func(event *Event) (bool, error) {
		return true, err
	}
}

func falseFilter(err error) EventFilter {
	return func(event *Event) (bool, error) {
		return false, err
	}
}

func filters(efs ...EventFilter) []EventFilter {
	return efs
}

func TestAllEventFilters(t *testing.T) {
	tests := []struct {
		name string
		efs  []EventFilter
		ok   bool
		err  error
	}{
		{
			name: "Nil filters",
			ok:   true,
		},
		{
			name: "True filter",
			efs:  filters(trueFilter(nil)),
			ok:   true,
		},
		{
			name: "False filter",
			efs:  filters(falseFilter(nil)),
		},
		{
			name: "True Error filter",
			efs:  filters(trueFilter(err1)),
			err:  err1,
		},
		{
			name: "False Error filter",
			efs:  filters(falseFilter(err1)),
			err:  err1,
		},
		{
			name: "All true filter",
			efs:  filters(trueFilter(nil), trueFilter(nil), trueFilter(nil)),
			ok:   true,
		},
		{
			name: "One false filter",
			efs:  filters(trueFilter(nil), falseFilter(nil), trueFilter(nil)),
			ok:   false,
		},
		{
			name: "One false One error filter",
			efs:  filters(trueFilter(err1), falseFilter(nil), trueFilter(nil)),
			ok:   false,
			err:  err1,
		},
		{
			name: "Two false No error filter",
			efs:  filters(falseFilter(nil), falseFilter(err1), trueFilter(nil)),
			ok:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := &Event{}
			ok, err := AllEventFilters(tt.efs...)(evt)
			if tt.err == nil {
				jtest.RequireNil(t, err)
			} else {
				jtest.Require(t, tt.err, err)
			}
			require.Equal(t, tt.ok, ok)
		})
	}
}

func TestAnyEventFilters(t *testing.T) {
	tests := []struct {
		name string
		efs  []EventFilter
		ok   bool
		err  error
	}{
		{
			name: "Nil filters",
		},
		{
			name: "True filter",
			efs:  filters(trueFilter(nil)),
			ok:   true,
		},
		{
			name: "False filter",
			efs:  filters(falseFilter(nil)),
		},
		{
			name: "True Error filter",
			efs:  filters(trueFilter(err1)),
			err:  err1,
		},
		{
			name: "False Error filter",
			efs:  filters(falseFilter(err1)),
			err:  err1,
		},
		{
			name: "All true filter",
			efs:  filters(trueFilter(nil), trueFilter(nil), trueFilter(nil)),
			ok:   true,
		},
		{
			name: "One false filter",
			efs:  filters(trueFilter(nil), falseFilter(nil), trueFilter(nil)),
			ok:   true,
		},
		{
			name: "Two false One error filter",
			efs:  filters(trueFilter(err1), falseFilter(nil), falseFilter(nil)),
			err:  err1,
		},
		{
			name: "One false No error filter",
			efs:  filters(trueFilter(nil), falseFilter(err1), trueFilter(nil)),
			ok:   true,
		},
		{
			name: "Two false Two error filter",
			efs:  filters(falseFilter(err1), falseFilter(err2), trueFilter(nil)),
			ok:   true,
		},
		{
			name: "Two false Three error filter",
			efs:  filters(falseFilter(err1), falseFilter(err2), trueFilter(err3)),
			err:  errors.Join(err1, err2, err3),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evt := &Event{}
			ok, err := AnyEventFilters(tt.efs...)(evt)
			if tt.err == nil {
				jtest.RequireNil(t, err)
			} else {
				require.Equal(t, tt.err.Error(), err.Error())
			}
			require.Equal(t, tt.ok, ok)
		})
	}
}
