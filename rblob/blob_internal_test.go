package rblob

import (
	"context"
	"io"
	"os"
	"path"
	"sort"
	"testing"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
)

func TestClose(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	bucket, err := OpenBucket(context.Background(), "", "file:///"+path.Join(dir, "testdata"))
	require.NoError(t, err)

	sc, err := bucket.Stream(context.Background(), "2020|eof")
	require.NoError(t, err)

	_, err = sc.Recv()
	require.NoError(t, err)

	closer := sc.(io.Closer)
	require.NoError(t, closer.Close())

	_, err = sc.Recv()
	require.Error(t, err)

	require.Error(t, closer.Close())
}

func TestLegacyCursor(t *testing.T) {
	c, err := parseCursor("file|123")
	jtest.RequireNil(t, err)
	require.Equal(t, int64(123), c.Offset)
}

func TestCursor(t *testing.T) {
	test := func(t *testing.T, c cursor, expected string) {
		t.Helper()
		require.Equal(t, expected, c.String())
		actual, err := parseCursor(c.String())
		require.NoError(t, err)
		require.Equal(t, c, actual)
	}

	var order []string

	c := cursor{
		Key:    "path/to/file",
		Offset: 0,
		EOF:    false,
	}
	test(t, c, "path/to/file|01|0")
	order = append(order, c.String())

	c.Offset = 9
	test(t, c, "path/to/file|01|9")
	order = append(order, c.String())

	c.Offset = 10
	test(t, c, "path/to/file|02|10")
	order = append(order, c.String())

	c.Offset = 999
	test(t, c, "path/to/file|03|999")
	order = append(order, c.String())

	c.Offset = 0
	c.EOF = true
	test(t, c, "path/to/file|eof")
	order = append(order, c.String())

	// Ensure that the order is lexicographical
	clone := append([]string(nil), order...)
	sort.Strings(order)
	require.Equal(t, clone, order)
}
