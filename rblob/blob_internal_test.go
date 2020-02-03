package rblob

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/fileblob"
)

func TestClose(t *testing.T) {
	dir, err := os.Getwd()
	require.NoError(t, err)

	bucket, err := OpenBucket(context.Background(), "file:///"+path.Join(dir, "testdata"))
	require.NoError(t, err)

	sc, err := bucket.Stream(context.Background(), "")
	require.NoError(t, err)

	_, err = sc.Recv()
	require.NoError(t, err)

	closer := sc.(io.Closer)

	require.NoError(t, closer.Close())

	_, err = sc.Recv()
	require.Error(t, err)

	require.Error(t, closer.Close())
}
