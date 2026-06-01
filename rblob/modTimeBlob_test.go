package rblob_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"

	"github.com/luno/reflex/rblob"
)

func writeModTimeBlob(t *testing.T, dir, name string, content []byte, modTime time.Time) {
	t.Helper()
	p := filepath.Join(dir, name)
	require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
	require.NoError(t, os.WriteFile(p, content, 0o644))
	require.NoError(t, os.Chtimes(p, modTime, modTime))
}

func openModTimeBucket(t *testing.T, dir string, opts ...rblob.BucketOption[rblob.ModTimeBucket]) *rblob.ModTimeBucket {
	t.Helper()
	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	return rblob.NewModTimeBucket("test", b, opts...)
}

func TestModTimeStream_StreamsInModTimeOrder(t *testing.T) {
	dir := t.TempDir()

	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()
	t3 := time.Unix(3000, 0).UTC()

	// Written out of lexicographic order; stream must reorder by mod time.
	writeModTimeBlob(t, dir, "c-obj", []byte(`{"id":3}`), t3)
	writeModTimeBlob(t, dir, "a-obj", []byte(`{"id":1}`), t1)
	writeModTimeBlob(t, dir, "b-obj", []byte(`{"id":2}`), t2)

	mb := openModTimeBucket(t, dir, rblob.WithModTimeBackoff(time.Millisecond))
	sc, err := mb.ModTimeStream(t.Context(), "")
	require.NoError(t, err)

	for _, wantID := range []int64{1, 2, 3} {
		e, err := sc.Recv()
		jtest.Require(t, nil, err)

		var got struct {
			ID int64 `json:"id"`
		}
		require.NoError(t, json.Unmarshal(e.MetaData, &got))
		require.Equal(t, wantID, got.ID)
	}
}

func TestModTimeStream_Resume(t *testing.T) {
	dir := t.TempDir()

	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()
	t3 := time.Unix(3000, 0).UTC()

	writeModTimeBlob(t, dir, "a-obj", []byte(`{"id":1}`), t1)
	writeModTimeBlob(t, dir, "b-obj", []byte(`{"id":2}`), t2)
	writeModTimeBlob(t, dir, "c-obj", []byte(`{"id":3}`), t3)

	mb := openModTimeBucket(t, dir, rblob.WithModTimeBackoff(time.Millisecond))

	// Stream the first event and capture its cursor.
	sc, err := mb.ModTimeStream(t.Context(), "")
	require.NoError(t, err)

	first, err := sc.Recv()
	jtest.Require(t, nil, err)
	afterCursor := first.ID

	// A fresh stream resuming after the first event should only yield b-obj and c-obj.
	sc2, err := mb.ModTimeStream(t.Context(), afterCursor)
	require.NoError(t, err)

	for _, wantID := range []int64{2, 3} {
		e, err := sc2.Recv()
		jtest.Require(t, nil, err)

		var got struct {
			ID int64 `json:"id"`
		}
		require.NoError(t, json.Unmarshal(e.MetaData, &got))
		require.Equal(t, wantID, got.ID)
	}
}

func TestModTimeStream_WithPrefix(t *testing.T) {
	dir := t.TempDir()

	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()

	writeModTimeBlob(t, dir, "other-obj", []byte(`{"id":99}`), t1)
	writeModTimeBlob(t, dir, filepath.Join("prefix", "a-obj"), []byte(`{"id":1}`), t1)
	writeModTimeBlob(t, dir, filepath.Join("prefix", "b-obj"), []byte(`{"id":2}`), t2)

	mb := openModTimeBucket(t, dir,
		rblob.WithModTimePrefix("prefix/"),
		rblob.WithModTimeBackoff(time.Millisecond),
	)

	sc, err := mb.ModTimeStream(t.Context(), "")
	require.NoError(t, err)

	for _, wantID := range []int64{1, 2} {
		e, err := sc.Recv()
		jtest.Require(t, nil, err)

		var got struct {
			ID int64 `json:"id"`
		}
		require.NoError(t, json.Unmarshal(e.MetaData, &got))
		require.Equal(t, wantID, got.ID)
	}
}

func TestModTimeStream_ContextCancel(t *testing.T) {
	dir := t.TempDir()

	mb := openModTimeBucket(t, dir, rblob.WithModTimeBackoff(time.Hour))

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()

	sc, err := mb.ModTimeStream(ctx, "")
	require.NoError(t, err)

	_, err = sc.Recv()
	jtest.Require(t, context.DeadlineExceeded, err)
}

func TestModTimeBucket_Close(t *testing.T) {
	dir := t.TempDir()
	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)

	mb := rblob.NewModTimeBucket("test", b)
	require.NoError(t, mb.Close())
}

func TestModTimeStream_MultipleEventsPerFile(t *testing.T) {
	dir := t.TempDir()

	t1 := time.Unix(1000, 0).UTC()
	writeModTimeBlob(t, dir, "multi", []byte(`{"id":1}{"id":2}{"id":3}`), t1)

	mb := openModTimeBucket(t, dir, rblob.WithModTimeBackoff(time.Millisecond))
	sc, err := mb.ModTimeStream(t.Context(), "")
	require.NoError(t, err)

	var ids []int64
	var cursorIDs []string
	for range 3 {
		e, err := sc.Recv()
		jtest.Require(t, nil, err)

		var got struct {
			ID int64 `json:"id"`
		}
		require.NoError(t, json.Unmarshal(e.MetaData, &got))
		ids = append(ids, got.ID)
		cursorIDs = append(cursorIDs, e.ID)
	}

	require.Equal(t, []int64{1, 2, 3}, ids)
	// Each record in the same file gets a unique cursor with an incrementing offset.
	require.NotEqual(t, cursorIDs[0], cursorIDs[1])
	require.NotEqual(t, cursorIDs[1], cursorIDs[2])
}

func TestModTimeStream_ResumeFromMiddleOfBlob(t *testing.T) {
	dir := t.TempDir()

	t1 := time.Unix(1000, 0).UTC()
	writeModTimeBlob(t, dir, "multi", []byte(`{"id":1}{"id":2}{"id":3}`), t1)

	mb := openModTimeBucket(t, dir, rblob.WithModTimeBackoff(time.Millisecond))

	sc, err := mb.ModTimeStream(t.Context(), "")
	require.NoError(t, err)

	first, err := sc.Recv()
	jtest.Require(t, nil, err)
	var got struct {
		ID int64 `json:"id"`
	}
	require.NoError(t, json.Unmarshal(first.MetaData, &got))
	require.Equal(t, int64(1), got.ID)

	// Resume after the first record — should yield records 2 and 3 only.
	sc2, err := mb.ModTimeStream(t.Context(), first.ID)
	require.NoError(t, err)

	for _, wantID := range []int64{2, 3} {
		e, err := sc2.Recv()
		jtest.Require(t, nil, err)

		require.NoError(t, json.Unmarshal(e.MetaData, &got))
		require.Equal(t, wantID, got.ID)
	}
}
