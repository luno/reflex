package rblob

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/reflex"
	"gocloud.dev/blob"
)

// --------------------------------------------------------------------------------------------
// This differs from the regular blob functionality, in that it is designed to resume
// its cursor based on the comparisons on last modified rather than on lexicographic
// ordering. Since this is not supported in the AWS ListObjectsV2 client (https://docs.aws.amazon.com/cli/latest/reference/s3api/list-objects-v2.html)
// (on the day this is documented 2026-05-26). This should only be used for small to
// medium size S3 buckets. Reason being that the cursor will need to iterate through all
// the last modified list objects to determine its resume position.
// --------------------------------------------------------------------------------------------

// WithModTimeBackoff configures the backoff duration between polls when no new
// objects are found. Defaults to one minute.
func WithModTimeBackoff(d time.Duration) BucketOption[ModTimeBucket] {
	return func(b *ModTimeBucket) {
		b.backoff = d
	}
}

// WithModTimeDecoder configures the decoder used to read object contents.
// Defaults to JSONDecoder.
func WithModTimeDecoder(fn func(io.Reader) (Decoder, error)) BucketOption[ModTimeBucket] {
	return func(b *ModTimeBucket) {
		b.decoderFunc = fn
	}
}

// WithModTimePrefix restricts listing to objects whose keys start with prefix.
func WithModTimePrefix(prefix string) BucketOption[ModTimeBucket] {
	return func(b *ModTimeBucket) {
		b.prefix = prefix
	}
}

type ModTimeBucket struct {
	label       string
	bucket      *blob.Bucket
	prefix      string
	decoderFunc func(io.Reader) (Decoder, error)
	backoff     time.Duration
}

type modtimeStream struct {
	ctx         context.Context //nolint:containedctx //context governs stream lifetime; Recv() has no ctx param
	bucket      *blob.Bucket
	prefix      string
	decoderFunc func(io.Reader) (Decoder, error)
	backoff     time.Duration
	cursor      modtimeCursor

	// current object being decoded
	objects    []*blob.ListObject
	objIdx     int
	reader     *blob.Reader
	decoder    Decoder
	next       []byte
	blobEOF    bool
	resumeDone bool // true once the initial cursor blob has been opened for mid-blob resume

	err error
}

// modtimeCursor identifies the last-processed S3 object by its key and
// last-modified time. ModTime is used for ordering; Key breaks ties and
// provides identity for the resume skip. Offset is the number of records
// already consumed from this blob, used to resume mid-blob.
type modtimeCursor struct {
	Key     string
	ModTime time.Time
	Offset  int
}
type modtimeEventType struct{}

func (eventType modtimeEventType) ReflexType() int { return 0 }

// OpenModTimeBucket opens and returns a bucket for the provided URL.
//
// label: defines the bucket label used for metrics.
// URLString: defines the URL of the blob bucket. See the gocloud
// URLOpener documentation in driver subpackages for details
// on supported URL formats. Also see https://gocloud.dev/concepts/urls/
// and https://gocloud.dev/howto/blob/.
func OpenModTimeBucket(
	ctx context.Context,
	label,
	URLString string,
	opts ...BucketOption[ModTimeBucket],
) (*ModTimeBucket, error) {
	bucket, err := blob.OpenBucket(ctx, URLString)
	if err != nil {
		return nil, err
	}

	return NewModTimeBucket(label, bucket, opts...), nil
}

func NewModTimeBucket(label string, bucket *blob.Bucket, opts ...BucketOption[ModTimeBucket]) *ModTimeBucket {
	b := &ModTimeBucket{
		label:       label,
		bucket:      bucket,
		decoderFunc: JSONDecoder,
		backoff:     time.Minute,
	}

	for _, opt := range opts {
		opt(b)
	}

	if b.decoderFunc == nil {
		b.decoderFunc = JSONDecoder
	}
	if b.backoff <= 0 {
		b.backoff = time.Minute
	}

	return b
}

// Close releases any resources used by the underlying modTime bucket
func (b *ModTimeBucket) Close() error { return b.bucket.Close() }

// String returns the string representation of the modtimeCursor.
//
// If the cursor key is empty, an empty string is returned.
// Otherwise, the result is formatted as:
//
//	<key>|<modtime_unix_nano>|<offset>
//
// where modtime_unix_nano is the modification time in Unix nanoseconds and
// offset is the number of records already consumed from this blob.
func (c modtimeCursor) String() string {
	if c.Key == "" {
		return ""
	}
	return fmt.Sprintf("%s|%d|%d", c.Key, c.ModTime.UnixNano(), c.Offset)
}

// parseModTimeCursor parses a modtime cursor string into a modtimeCursor value.
//
// The expected format is:
//
//	<key>|<modtime_unix_nano>|<offset>
//
// where modtime_unix_nano is a Unix timestamp in nanoseconds and offset is the
// number of records already consumed from this blob.
//
// An empty string returns an empty modtimeCursor and no error.
// An error is returned if the cursor format is invalid or fields cannot be parsed.
func parseModTimeCursor(s string) (modtimeCursor, error) {
	if s == "" {
		return modtimeCursor{}, nil
	}

	lastSep := strings.LastIndex(s, "|")
	if lastSep < 0 {
		return modtimeCursor{}, errors.Wrap(errModTimeCursorMissingSeparator, "", j.MKV{"cursor": s, "lastSep": lastSep})
	}

	tail := s[lastSep+1:]
	prefix := s[:lastSep]

	secondSep := strings.LastIndex(prefix, "|")
	if secondSep < 0 {
		return modtimeCursor{}, errors.Wrap(errModTimeCursorMissingSeparator, "", j.MKV{"cursor": s, "secondSep": secondSep, "prefix": prefix})
	}

	key := prefix[:secondSep]
	nanoStr := prefix[secondSep+1:]
	if key == "" {
		return modtimeCursor{}, errors.Wrap(errModTimeCursorEmptyKey, "", j.MKV{"cursor": s, "nanoStr": nanoStr})
	}
	ns, err := strconv.ParseInt(nanoStr, 10, 64)
	if err != nil {
		return modtimeCursor{}, errors.Wrap(errModTimeCursorBadUnixNano, "", j.MKV{"cursor": s, "ns": ns})
	}
	offset, err := strconv.ParseInt(tail, 10, 64)
	if err != nil {
		return modtimeCursor{}, errors.Wrap(errModTimeCursorBadOffset, "", j.MKV{"cursor": s, "offset": offset})
	}
	return modtimeCursor{
		Key:     key,
		ModTime: time.Unix(0, ns).UTC(),
		Offset:  int(offset),
	}, nil
}

// listSorted returns a list S3 objects (filtered by the prefix) in timestamp ascending order
func (s *modtimeStream) listSorted() ([]*blob.ListObject, error) {
	var objs []*blob.ListObject
	iter := s.bucket.List(&blob.ListOptions{Prefix: s.prefix})
	for {
		o, err := iter.Next(s.ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, errors.Wrap(err, "list object error", j.KS("prefix", s.prefix))
		}
		objs = append(objs, o)
	}
	slices.SortFunc(objs, func(a, b *blob.ListObject) int {
		if c := a.ModTime.UTC().Compare(b.ModTime.UTC()); c != 0 {
			return c
		}
		return strings.Compare(a.Key, b.Key)
	})
	return objs, nil
}

// after returns true when object comes strictly after the current cursor.
func (s *modtimeStream) after(obj *blob.ListObject) bool {
	if s.cursor.Key == "" {
		return true
	}

	modTime := obj.ModTime.UTC()
	cursorTime := s.cursor.ModTime
	if modTime.Equal(cursorTime) {
		return obj.Key > s.cursor.Key
	}
	return modTime.After(cursorTime)
}

// isCursorBlob returns true when obj is the same blob the cursor points to.
func (s *modtimeStream) isCursorBlob(obj *blob.ListObject) bool {
	return obj.Key == s.cursor.Key && obj.ModTime.UTC().Equal(s.cursor.ModTime)
}

// loadNextObject advances to the next blob object in sorted order, opening its
// reader and pre-loading the first event. If the sorted object list is
// exhausted it re-lists and waits (with backoff) until a new object appears or
// the context is canceled.
func (s *modtimeStream) loadNextObject() error {
	for {
		// Close previous reader if any.
		if s.reader != nil {
			if err := s.reader.Close(); err != nil {
				return err
			}
			s.reader = nil
			s.decoder = nil
			s.blobEOF = false
		}

		// Refill sorted list if exhausted.
		if s.objIdx >= len(s.objects) {
			objs, err := s.listSorted()
			if err != nil {
				return err
			}
			// Skip anything at or before the current cursor, but retain the
			// cursor blob itself when resuming mid-blob.
			start := 0
			for start < len(objs) && !s.after(objs[start]) {
				if !s.resumeDone && s.cursor.Offset > 0 && s.isCursorBlob(objs[start]) {
					break
				}
				start++
			}
			s.objects = objs[start:]
			s.objIdx = 0
		}

		if s.objIdx < len(s.objects) {
			break
		}

		// Nothing new yet — wait and retry.
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-time.After(s.backoff):
		}
	}

	obj := s.objects[s.objIdx]
	s.objIdx++

	r, err := s.bucket.NewReader(s.ctx, obj.Key, nil)
	if err != nil {
		return errors.Wrap(err, "new reader")
	}

	d, err := s.decoderFunc(r)
	if err != nil {
		_ = r.Close()
		return err
	}

	isCursor := !s.resumeDone && s.cursor.Offset > 0 && s.isCursorBlob(obj)
	if isCursor {
		s.resumeDone = true
		for i := 0; i < s.cursor.Offset; i++ {
			if _, skipErr := d.Decode(); errors.Is(skipErr, io.EOF) {
				// Offset exceeds the blob's record count — treat as fully consumed.
				s.reader = r
				s.decoder = d
				s.blobEOF = true
				return nil
			} else if skipErr != nil {
				_ = r.Close()
				return errors.Wrap(skipErr, "skip record")
			}
		}
	} else {
		s.cursor = modtimeCursor{Key: obj.Key, ModTime: obj.ModTime.UTC()}
	}

	first, err := d.Decode()
	if errors.Is(err, io.EOF) {
		s.blobEOF = true
	} else if err != nil {
		_ = r.Close()
		return errors.Wrap(err, "decode first")
	}

	s.reader = r
	s.decoder = d
	s.next = first

	return nil
}

func (s *modtimeStream) Recv() (*reflex.Event, error) {
	if s.err != nil {
		return nil, s.err
	}
	e, err := s.recv()
	if err != nil {
		s.err = err
		if s.reader != nil {
			_ = s.reader.Close()
		}
	}
	return e, err
}

func (s *modtimeStream) recv() (*reflex.Event, error) {
	// Advance to the next available object if needed.
	for s.decoder == nil || s.blobEOF {
		if err := s.loadNextObject(); err != nil {
			return nil, err
		}
	}

	payload := s.next
	peek, err := s.decoder.Decode()
	if errors.Is(err, io.EOF) {
		s.blobEOF = true
	} else if err != nil {
		return nil, errors.Wrap(err, "decode")
	}
	s.next = peek
	s.cursor.Offset++

	e := &reflex.Event{
		ID:        s.cursor.String(),
		Type:      modtimeEventType{},
		Timestamp: s.cursor.ModTime,
		MetaData:  payload,
	}
	return e, nil
}

// ModTimeStream returns a reflex.StreamClient that streams events from the
// bucket in modification-time order. The after parameter is an opaque cursor
// string (as returned by previous events) that resumes streaming from where
// the caller left off; pass an empty string to start from the beginning.
// Stream options are not currently supported and will return an error if provided.
func (b *ModTimeBucket) ModTimeStream(
	ctx context.Context, after string, opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	if len(opts) > 0 {
		return nil, errors.New("options not supported")
	}
	cursor, err := parseModTimeCursor(after)
	if err != nil {
		return nil, err
	}
	return &modtimeStream{
		ctx:         ctx,
		bucket:      b.bucket,
		prefix:      b.prefix,
		decoderFunc: b.decoderFunc,
		backoff:     b.backoff,
		cursor:      cursor,
	}, nil
}

// Close closes the modtime stream and its reader.
func (s *modtimeStream) Close() error {
	// Check if stream already closed
	if s.err != nil {
		return s.err
	}

	// Set err to be closed
	s.err = errors.New("closed")

	// Check if stream reader is closed
	if s.reader == nil {
		return nil
	}

	return s.reader.Close()
}
