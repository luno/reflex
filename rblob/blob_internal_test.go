package rblob

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
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

func TestWithKeyFilter(t *testing.T) {
	workDir, err := os.Getwd()
	require.NoError(t, err)

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		// Option-level: verify nil guard and custom fn wiring against b.keyFilter directly.
		{
			name: "nil falls back to default: key greater than prev",
			run: func(t *testing.T) {
				b := &Bucket{}
				WithKeyFilter(nil)(b)
				require.NotNil(t, b.keyFilter)
				require.True(t, b.keyFilter("a", &blob.ListObject{Key: "b"}))
			},
		},
		{
			name: "nil falls back to default: key not greater than prev",
			run: func(t *testing.T) {
				b := &Bucket{}
				WithKeyFilter(nil)(b)
				require.False(t, b.keyFilter("b", &blob.ListObject{Key: "a"}))
			},
		},
		{
			name: "custom fn overrides default",
			run: func(t *testing.T) {
				b := &Bucket{}
				WithKeyFilter(func(_ string, _ *blob.ListObject) bool { return false })(b)
				require.NotNil(t, b.keyFilter)
				require.False(t, b.keyFilter("a", &blob.ListObject{Key: "b"})) // default would return true
			},
		},
		// Integration: verify the filter is honoured end-to-end through the stream.
		{
			name: "key prefix excludes 2019 blobs",
			run: func(t *testing.T) {
				bucket, err := OpenBucket(context.Background(), "",
					"file:///"+path.Join(workDir, "testdata"),
					WithKeyFilter(func(prev string, o *blob.ListObject) bool {
						return o.Key > prev && !strings.HasPrefix(o.Key, "2019")
					}))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, bucket.Close()) })

				sc, err := bucket.Stream(context.Background(), "")
				require.NoError(t, err)

				for _, wantID := range []int64{4, 5, 6, 7} {
					e, err := sc.Recv()
					jtest.Require(t, nil, err)
					var got struct {
						ID int64 `json:"id"`
					}
					require.NoError(t, json.Unmarshal(e.MetaData, &got))
					require.Equal(t, wantID, got.ID)
				}
			},
		},
		{
			name: "mod time excludes old blobs",
			run: func(t *testing.T) {
				dir := t.TempDir()
				oldFile := path.Join(dir, "file-a")
				require.NoError(t, os.WriteFile(oldFile, []byte(`{"id":1,"field":"old"}`), 0o644))
				backdated := time.Now().Add(-2 * time.Hour)
				require.NoError(t, os.Chtimes(oldFile, backdated, backdated))
				require.NoError(t, os.WriteFile(path.Join(dir, "file-b"), []byte(`{"id":2,"field":"new"}`), 0o644))

				bucket, err := OpenBucket(context.Background(), "", "file:///"+dir,
					WithKeyFilter(func(prev string, o *blob.ListObject) bool {
						return o.Key > prev && o.ModTime.After(time.Now().Add(-1*time.Hour))
					}),
					WithBackoff(time.Millisecond))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, bucket.Close()) })

				sc, err := bucket.Stream(context.Background(), "")
				require.NoError(t, err)

				e, err := sc.Recv()
				jtest.Require(t, nil, err)
				var got struct {
					ID int64 `json:"id"`
				}
				require.NoError(t, json.Unmarshal(e.MetaData, &got))
				require.Equal(t, int64(2), got.ID)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { tc.run(t) })
	}
}

func Test_makeStartAfter(t *testing.T) {
	t.Run("rejected", func(t *testing.T) {
		fn := makeStartAfter("after")
		err := fn(func(v any) bool { return false })
		require.ErrorContains(t, err, "gocloud.dev rejected our ListObjectsV2Input")
	})

	t.Run("ok", func(t *testing.T) {
		fn := makeStartAfter("after")
		err := fn(func(v any) bool { return true })
		jtest.RequireNil(t, err)
	})
}
