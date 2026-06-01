package rblob

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/require"
	"gocloud.dev/blob"
	_ "gocloud.dev/blob/fileblob"
)

// errDecoder is a Decoder that always returns a fixed error.
type errDecoder struct{ err error }

func (d *errDecoder) Decode() ([]byte, error) { return nil, d.err }

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
			expected: "some/key|-6795364578871345152|0",
		},
		{
			name:     "key with positive unix-nano",
			cursor:   modtimeCursor{Key: "a/b/c", ModTime: time.Unix(0, 1_000_000_000)},
			expected: "a/b/c|1000000000|0",
		},
		{
			name:     "key containing pipe character",
			cursor:   modtimeCursor{Key: "file|with|pipes", ModTime: time.Unix(0, 42)},
			expected: "file|with|pipes|42|0",
		},
		{
			name:     "non-zero offset is encoded",
			cursor:   modtimeCursor{Key: "a/b/c", ModTime: time.Unix(0, 1_000_000_000), Offset: 3},
			expected: "a/b/c|1000000000|3",
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
			name:    "missing separator returns error",
			input:   "nokeynoseparator",
			wantErr: errModTimeCursorMissingSeparator,
		},
		{
			name:    "only one separator returns error",
			input:   "somekey|1000000000",
			wantErr: errModTimeCursorMissingSeparator,
		},
		{
			name:    "empty key returns error",
			input:   "|1000000000|0",
			wantErr: errModTimeCursorEmptyKey,
		},
		{
			name:    "non-integer unix-nano returns error",
			input:   "somekey|notanumber|0",
			wantErr: errModTimeCursorBadUnixNano,
		},
		{
			name:  "valid cursor parses correctly",
			input: "path/to/obj|1000000000|5",
			want:  modtimeCursor{Key: "path/to/obj", ModTime: time.Unix(0, 1_000_000_000).UTC(), Offset: 5},
		},
		{
			name:    "non-integer offset returns error",
			input:   "path/to/obj|1000000000|notanoffset",
			wantErr: errModTimeCursorBadOffset,
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

func TestModtimeStream_After(t *testing.T) {
	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()

	tests := []struct {
		name   string
		cursor modtimeCursor
		obj    *blob.ListObject
		want   bool
	}{
		{
			name:   "empty cursor is always after",
			cursor: modtimeCursor{},
			obj:    &blob.ListObject{Key: "any/key", ModTime: t1},
			want:   true,
		},
		{
			name:   "object modtime strictly after cursor",
			cursor: modtimeCursor{Key: "a", ModTime: t1},
			obj:    &blob.ListObject{Key: "a", ModTime: t2},
			want:   true,
		},
		{
			name:   "object modtime strictly before cursor",
			cursor: modtimeCursor{Key: "b", ModTime: t2},
			obj:    &blob.ListObject{Key: "b", ModTime: t1},
			want:   false,
		},
		{
			name:   "equal modtime key greater than cursor",
			cursor: modtimeCursor{Key: "a", ModTime: t1},
			obj:    &blob.ListObject{Key: "b", ModTime: t1},
			want:   true,
		},
		{
			name:   "equal modtime key equal to cursor",
			cursor: modtimeCursor{Key: "a", ModTime: t1},
			obj:    &blob.ListObject{Key: "a", ModTime: t1},
			want:   false,
		},
		{
			name:   "equal modtime key less than cursor",
			cursor: modtimeCursor{Key: "b", ModTime: t1},
			obj:    &blob.ListObject{Key: "a", ModTime: t1},
			want:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &modtimeStream{cursor: tc.cursor}
			require.Equal(t, tc.want, s.after(tc.obj))
		})
	}
}

func TestModtimeStream_ListSorted(t *testing.T) {
	t1 := time.Unix(1000, 0).UTC()
	t2 := time.Unix(2000, 0).UTC()
	t3 := time.Unix(3000, 0).UTC()

	writeFile := func(t *testing.T, dir, name string, modTime time.Time) {
		t.Helper()
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("{}"), 0o644))
		require.NoError(t, os.Chtimes(p, modTime, modTime))
	}

	openBucket := func(t *testing.T, dir string) *blob.Bucket {
		t.Helper()
		b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })
		return b
	}

	t.Run("sorts by modtime then key", func(t *testing.T) {
		dir := t.TempDir()
		// Intentionally written out of order.
		writeFile(t, dir, "c-file", t2) // ties with b-file on modtime
		writeFile(t, dir, "a-file", t1) // earliest
		writeFile(t, dir, "b-file", t2) // same modtime as c-file, but key sorts first
		writeFile(t, dir, "d-file", t3) // latest

		s := &modtimeStream{ctx: t.Context(), bucket: openBucket(t, dir)}
		objs, err := s.listSorted()
		require.NoError(t, err)
		require.Len(t, objs, 4)
		require.Equal(t, "a-file", objs[0].Key)
		require.Equal(t, "b-file", objs[1].Key)
		require.Equal(t, "c-file", objs[2].Key)
		require.Equal(t, "d-file", objs[3].Key)
	})

	t.Run("no matching prefix returns empty slice", func(t *testing.T) {
		dir := t.TempDir()
		writeFile(t, dir, "other-obj", t1)

		s := &modtimeStream{ctx: t.Context(), bucket: openBucket(t, dir), prefix: "nomatch/"}
		objs, err := s.listSorted()
		require.NoError(t, err)
		require.Empty(t, objs)
	})

	t.Run("closed bucket returns error", func(t *testing.T) {
		b, err := blob.OpenBucket(t.Context(), "file:///"+t.TempDir())
		require.NoError(t, err)
		require.NoError(t, b.Close())

		s := &modtimeStream{ctx: t.Context(), bucket: b}
		_, err = s.listSorted()
		require.Error(t, err)
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

func TestModtimeStream_Close(t *testing.T) {
	t.Run("idle stream returns nil", func(t *testing.T) {
		s := &modtimeStream{}
		require.NoError(t, s.Close())
	})

	t.Run("already errored returns existing error", func(t *testing.T) {
		sentinel := errors.New("existing error")
		s := &modtimeStream{err: sentinel}
		jtest.Require(t, sentinel, s.Close())
	})

	t.Run("second close returns error", func(t *testing.T) {
		s := &modtimeStream{}
		require.NoError(t, s.Close())
		require.Error(t, s.Close())
	})

	t.Run("with open reader closes cleanly", func(t *testing.T) {
		dir := t.TempDir()
		t1 := time.Unix(1000, 0).UTC()

		p := filepath.Join(dir, "obj")
		require.NoError(t, os.WriteFile(p, []byte(`{}`), 0o644))
		require.NoError(t, os.Chtimes(p, t1, t1))

		b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })

		s := &modtimeStream{
			ctx:         t.Context(),
			bucket:      b,
			decoderFunc: JSONDecoder,
			backoff:     time.Millisecond,
		}
		require.NoError(t, s.loadNextObject())
		require.NotNil(t, s.reader)
		require.NoError(t, s.Close())
	})

	t.Run("recv after close returns error", func(t *testing.T) {
		s := &modtimeStream{ctx: t.Context()}
		require.NoError(t, s.Close())
		_, err := s.Recv()
		require.Error(t, err)
	})
}

func TestModtimeEventType_ReflexType(t *testing.T) {
	require.Equal(t, 0, modtimeEventType{}.ReflexType())
}

func TestNewModTimeBucket_Fallbacks(t *testing.T) {
	b, err := blob.OpenBucket(t.Context(), "file:///"+t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	mb := NewModTimeBucket("test", b, WithModTimeDecoder(nil), WithModTimeBackoff(-1))
	require.NotNil(t, mb.decoderFunc)
	require.Equal(t, time.Minute, mb.backoff)
}

func TestModtimeStream_LoadNextObject_ListSortedError(t *testing.T) {
	b, err := blob.OpenBucket(t.Context(), "file:///"+t.TempDir())
	require.NoError(t, err)
	require.NoError(t, b.Close())

	s := &modtimeStream{
		ctx:         t.Context(),
		bucket:      b,
		decoderFunc: JSONDecoder,
		backoff:     time.Millisecond,
	}
	require.Error(t, s.loadNextObject())
}

func TestModtimeStream_LoadNextObject_NewReaderError(t *testing.T) {
	b, err := blob.OpenBucket(t.Context(), "file:///"+t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	s := &modtimeStream{
		ctx:         t.Context(),
		bucket:      b,
		decoderFunc: JSONDecoder,
		backoff:     time.Millisecond,
		objects:     []*blob.ListObject{{Key: "nonexistent", ModTime: time.Unix(1000, 0).UTC()}},
	}
	require.Error(t, s.loadNextObject())
}

func TestModtimeStream_LoadNextObject_DecoderFuncError(t *testing.T) {
	dir := t.TempDir()
	t1 := time.Unix(1000, 0).UTC()
	p := filepath.Join(dir, "obj")
	require.NoError(t, os.WriteFile(p, []byte(`{}`), 0o644))
	require.NoError(t, os.Chtimes(p, t1, t1))

	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	decoderErr := errors.New("decoder failed")
	s := &modtimeStream{
		ctx:    t.Context(),
		bucket: b,
		decoderFunc: func(r io.Reader) (Decoder, error) {
			return nil, decoderErr
		},
		backoff: time.Millisecond,
	}
	jtest.Require(t, decoderErr, s.loadNextObject())
}

func TestModtimeStream_LoadNextObject_DecodeFirstError(t *testing.T) {
	dir := t.TempDir()
	t1 := time.Unix(1000, 0).UTC()
	p := filepath.Join(dir, "obj")
	require.NoError(t, os.WriteFile(p, []byte(`not-valid-json`), 0o644))
	require.NoError(t, os.Chtimes(p, t1, t1))

	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	s := &modtimeStream{
		ctx:         t.Context(),
		bucket:      b,
		decoderFunc: JSONDecoder,
		backoff:     time.Millisecond,
	}
	err = s.loadNextObject()
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode first")
}

func TestModtimeStream_LoadNextObject_SkipOffsetExceedsBlob(t *testing.T) {
	dir := t.TempDir()
	t1 := time.Unix(1000, 0).UTC()
	p := filepath.Join(dir, "obj")
	require.NoError(t, os.WriteFile(p, []byte(`{"id":1}{"id":2}`), 0o644))
	require.NoError(t, os.Chtimes(p, t1, t1))

	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	s := &modtimeStream{
		ctx:         t.Context(),
		bucket:      b,
		decoderFunc: JSONDecoder,
		backoff:     time.Millisecond,
		cursor:      modtimeCursor{Key: "obj", ModTime: t1, Offset: 10},
	}
	require.NoError(t, s.loadNextObject())
	require.True(t, s.blobEOF)
}

func TestModtimeStream_LoadNextObject_SkipRecordError(t *testing.T) {
	dir := t.TempDir()
	t1 := time.Unix(1000, 0).UTC()
	p := filepath.Join(dir, "obj")
	require.NoError(t, os.WriteFile(p, []byte(`{}`), 0o644))
	require.NoError(t, os.Chtimes(p, t1, t1))

	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	skipErr := errors.New("skip failed")
	s := &modtimeStream{
		ctx:    t.Context(),
		bucket: b,
		decoderFunc: func(r io.Reader) (Decoder, error) {
			return &errDecoder{err: skipErr}, nil
		},
		backoff: time.Millisecond,
		cursor:  modtimeCursor{Key: "obj", ModTime: t1, Offset: 1},
	}
	jtest.Require(t, skipErr, s.loadNextObject())
}

func TestModtimeStream_Recv_DecodeErrorClosesReader(t *testing.T) {
	dir := t.TempDir()
	t1 := time.Unix(1000, 0).UTC()
	p := filepath.Join(dir, "obj")
	require.NoError(t, os.WriteFile(p, []byte(`{}`), 0o644))
	require.NoError(t, os.Chtimes(p, t1, t1))

	b, err := blob.OpenBucket(t.Context(), "file:///"+dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Close()) })

	r, err := b.NewReader(t.Context(), "obj", nil)
	require.NoError(t, err)

	decodeErr := errors.New("mid-stream decode error")
	s := &modtimeStream{
		ctx:     t.Context(),
		bucket:  b,
		backoff: time.Millisecond,
		reader:  r,
		decoder: &errDecoder{err: decodeErr},
		next:    []byte(`{}`),
		cursor:  modtimeCursor{Key: "obj", ModTime: t1},
	}

	_, err = s.Recv()
	jtest.Require(t, decodeErr, err)

	// Subsequent call returns the stored error.
	_, err = s.Recv()
	jtest.Require(t, decodeErr, err)
}
