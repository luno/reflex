package rblob

import (
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
)

func TestModtimeCursor_String(t *testing.T) {
	tests := []struct {
		name     string
		cursor   modtimeCursor
		expected string
	}{
		{
			name:     "empty key returns empty string",
			cursor:   modtimeCursor{},
			expected: "",
		},
		{
			name:     "empty key with non-zero modtime still returns empty string",
			cursor:   modtimeCursor{Key: "", ModTime: time.Unix(0, 1234567890)},
			expected: "",
		},
		{
			name:     "key with zero modtime",
			cursor:   modtimeCursor{Key: "some/key", ModTime: time.Time{}},
			expected: "some/key|-6795364578871345152",
		},
		{
			name:     "key with positive unix-nano",
			cursor:   modtimeCursor{Key: "a/b/c", ModTime: time.Unix(0, 1_000_000_000)},
			expected: "a/b/c|1000000000",
		},
		{
			name:     "key containing pipe character",
			cursor:   modtimeCursor{Key: "file|with|pipes", ModTime: time.Unix(0, 42)},
			expected: "file|with|pipes|42",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, tc.cursor.String())
		})
	}
}

func TestParseModTimeCursor(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    modtimeCursor
		wantErr error
	}{
		{
			name:  "empty string returns zero cursor",
			input: "",
			want:  modtimeCursor{},
		},
		{
			name:    "when no key is provided",
			input:   "|borderlineSomethingSomething",
			wantErr: errModTimeCursorEmptyKey,
		},
		{
			name:    "missing separator returns error",
			input:   "nokeynoseparator",
			wantErr: errModTimeCursorMissingSeparator,
		},
		{
			name:    "non-integer unix-nano returns error",
			input:   "somekey|notanumber",
			wantErr: errModTimeCursorBadUnixNano,
		},
		{
			name:  "valid cursor parses correctly",
			input: "path/to/obj|1000000000",
			want:  modtimeCursor{Key: "path/to/obj", ModTime: time.Unix(0, 1_000_000_000).UTC()},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, err := parseModTimeCursor(tc.input)
			if tc.wantErr != nil {
				jtest.Require(t, tc.wantErr, err)
				return
			}
			jtest.Require(t, nil, err)
			require.Equal(t, tc.want, c)
		})
	}
}

func TestModtimeCursor_RoundTrip(t *testing.T) {
	original := modtimeCursor{
		Key:     "2024/01/15/event.json",
		ModTime: time.Unix(1705276800, 123456789).UTC(),
	}

	s := original.String()
	require.NotEmpty(t, s)

	parsed, err := parseModTimeCursor(s)
	jtest.RequireNil(t, err)
	require.Equal(t, original, parsed)
}
