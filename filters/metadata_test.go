package filters

import (
	"testing"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex"
)

func TestMakeMetadataEventFilter(t *testing.T) {
	type testCase struct {
		name string
		ds   Deserializer[string]
		flt  DataFilter[string]
		err  bool
	}
	tests := []testCase{
		{
			name: "No Deserializer, No DataFilter",
			err:  true,
		},
		{
			name: "No Deserializer",
			flt:  func(s string) (bool, error) { return true, nil },
			err:  true,
		},
		{
			name: "No DataFilter",
			ds:   func(b []byte) (string, error) { return string(b), nil },
			err:  true,
		},
		{
			name: "All supplied",
			ds:   func(b []byte) (string, error) { return string(b), nil },
			flt:  func(s string) (bool, error) { return true, nil },
		},
	}
	for _, tt := range tests {
		ef, err := MetadataEventFilter(tt.ds, tt.flt)
		if tt.err {
			require.True(t, IsMetadataEventFilterErr(err))
		} else {
			require.NotNil(t, ef)
			jtest.RequireNil(t, err)
		}
	}
}

func TestMetadataEventFilter(t *testing.T) {
	d := "metadata value"
	m := []byte(d)

	// Define errors once so the same pointer is used in both the closures
	// and the expected-error slices. jettison's errors.Is uses pointer identity
	// for errors without a j.C code, so a fresh errors.New in the err slice
	// would never match.
	badMetadataErr := errors.New("bad metadata")
	badFilterErr := errors.New("bad filter")

	type testCase struct {
		name string
		e    *reflex.Event
		ds   func(x *testing.T) Deserializer[string]
		flt  func(x *testing.T) DataFilter[string]
		ok   bool
		err  []error
	}
	tests := []testCase{
		{
			name: "Nil Event",
			ds: func(x *testing.T) Deserializer[string] {
				return func(b []byte) (string, error) { require.Nil(x, b); return string(b), nil }
			},
			flt: func(x *testing.T) DataFilter[string] {
				return func(s string) (bool, error) { require.Equal(x, "", s); return false, nil }
			},
		},
		{
			name: "Deserializer errors",
			e:    &reflex.Event{MetaData: m},
			ds: func(x *testing.T) Deserializer[string] {
				return func(b []byte) (string, error) { require.Equal(x, m, b); return string(b), badMetadataErr }
			},
			flt: func(x *testing.T) DataFilter[string] {
				return func(s string) (bool, error) { require.Fail(x, "should not be reached"); return true, nil }
			},
			err: []error{deserializationErr, badMetadataErr},
		},
		{
			name: "Data Filter errors",
			e:    &reflex.Event{MetaData: []byte("metadata value")},
			ds: func(x *testing.T) Deserializer[string] {
				return func(b []byte) (string, error) { require.Equal(x, m, b); return string(b), nil }
			},
			flt: func(x *testing.T) DataFilter[string] {
				return func(s string) (bool, error) { require.Equal(x, d, s); return true, badFilterErr }
			},
			err: []error{badFilterErr},
		},
		{
			name: "Exclude",
			e:    &reflex.Event{MetaData: []byte("metadata value")},
			ds: func(x *testing.T) Deserializer[string] {
				return func(b []byte) (string, error) { require.Equal(x, m, b); return string(b), nil }
			},
			flt: func(x *testing.T) DataFilter[string] {
				return func(s string) (bool, error) { require.Equal(x, d, s); return false, nil }
			},
		},
		{
			name: "Include",
			e:    &reflex.Event{MetaData: []byte("metadata value")},
			ds: func(x *testing.T) Deserializer[string] {
				return func(b []byte) (string, error) { require.Equal(x, m, b); return string(b), nil }
			},
			flt: func(x *testing.T) DataFilter[string] {
				return func(s string) (bool, error) { require.Equal(x, d, s); return true, nil }
			},
			ok: true,
		},
	}
	for _, tt := range tests {
		ef, _ := MetadataEventFilter(tt.ds(t), tt.flt(t))
		ok, err := ef(tt.e)
		if len(tt.err) != 0 {
			for _, ttErr := range tt.err {
				jtest.Require(t, ttErr, err)
			}
		} else {
			jtest.RequireNil(t, err)
			require.Equal(t, tt.ok, ok)
		}
	}
}

func TestIsDeserializationErr(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "nil error",
		},
		{
			name: "not deserialization error",
			err:  errors.New("other error"),
		},
		{
			name: "direct deserialization error",
			err:  deserializationErr,
			want: true,
		},
		{
			name: "indirect deserialization error",
			err:  errors.Wrap(deserializationErr, "other error"),
			want: true,
		},
		{
			name: "converted deserialization error",
			err:  asDeserializationErr(errors.New("other error")),
			want: true,
		},
		{
			name: "constructed deserialization error",
			err:  errors.New(deserializationErrMsg),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsDeserializationErr(tt.err))
		})
	}
}
