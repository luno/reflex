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
	t.Run("empty string returns zero cursor", func(t *testing.T) {
		c, err := parseModTimeCursor("")
		jtest.RequireNil(t, err)
		require.Equal(t, modtimeCursor{}, c)
	})

	t.Run("when no key is provided", func(t *testing.T) {
		_, err := parseModTimeCursor("|borderlineSomethingSomething")
		require.Error(t, err)
		require.ErrorContains(t, err, "empty key")
	})

	t.Run("missing separator returns error", func(t *testing.T) {
		_, err := parseModTimeCursor("nokeynoseparator")
		require.Error(t, err)
		require.ErrorContains(t, err, "missing separator")
	})

	t.Run("non-integer unix-nano returns error", func(t *testing.T) {
		_, err := parseModTimeCursor("somekey|notanumber")
		require.Error(t, err)
		require.ErrorContains(t, err, "bad unix-nano")
	})

	t.Run("valid cursor parses correctly", func(t *testing.T) {
		c, err := parseModTimeCursor("path/to/obj|1000000000")
		jtest.RequireNil(t, err)
		require.Equal(t, "path/to/obj", c.Key)
		require.Equal(t, time.Unix(0, 1_000_000_000).UTC(), c.ModTime)
	})
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
