package rblob

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"gocloud.dev/blob"
)

// Decoder decodes a blob into event byte slices (usually DTOs) which
// are streamed as event metadata.
type Decoder interface {
	// Decode returns the next non-empty byte slice or an error. It returns io.EOF if no more
	// are available.
	Decode() ([]byte, error)
}

// WithBackoff returns a option to configure the backoff duration
// before querying the underlying bucket for new blobs. It defaults
// to one minute.
func WithBackoff(d time.Duration) option {
	return func(b *Bucket) {
		b.backoff = d
	}
}

// WithDecoder returns a option to configure the blob content decoder
// function. It defaults to the JSONDecoder.
func WithDecoder(fn func(io.Reader) (Decoder, error)) option {
	return func(b *Bucket) {
		b.decoderFunc = fn
	}
}

type option func(*Bucket)

// OpenBucket opens and returns a bucket for the provided url.
//
// URL defines the url of the blob bucket. See the gocloud
// URLOpener documentation in driver subpackages for details
// on supported URL formats. Also see https://gocloud.dev/concepts/urls/
// and https://gocloud.dev/howto/blob/.
func OpenBucket(ctx context.Context, urlstr string,
	opts ...option) (*Bucket, error) {

	u, err := url.Parse(urlstr)
	if err != nil {
		return nil, errors.Wrap(err, "parse url string")
	}

	prefix := u.Query().Get("prefix")
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		return nil, errors.New("prefix should end with '/'")
	}

	bucket, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return nil, err
	}

	return newBucket(bucket, opts...), nil
}

func newBucket(bucket *blob.Bucket, opts ...option) *Bucket {

	s := &Bucket{
		bucket:      bucket,
		decoderFunc: JSONDecoder,
		backoff:     time.Minute,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Bucket defines a bucket from which to stream the content of
// consecutive blobs as events.
type Bucket struct {
	bucket      *blob.Bucket
	decoderFunc func(io.Reader) (Decoder, error)
	backoff     time.Duration

	cursor  cursor
	decoder Decoder
}

// Close releases any resources used by the underlying bucket.
func (b *Bucket) Close() error {
	return b.bucket.Close()
}

// Stream implements reflex.StreamFunc and returns a StreamClient that
// streams events from bucket blobs after the provided cursor.
// Stream is safe to call from multiple goroutines, but the returned
// StreamClient is only safe for a single goroutine to use.
//
// Note: The returned StreamClient implementation also exposes a
// Close method which releases underlying resources. Close is
// called internally when Recv returns an error.
func (b *Bucket) Stream(ctx context.Context, after string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	if len(opts) > 0 {
		return nil, errors.New("options not supported yet")
	}

	cursor, err := parseCursor(after)
	if err != nil {
		return nil, err
	}

	return &stream{
		ctx:         ctx,
		bucket:      b.bucket,
		decoderFunc: b.decoderFunc,
		backoff:     b.backoff,
		cursor:      cursor,
	}, nil
}

var (
	_ reflex.StreamClient = (*stream)(nil)
	_ io.Closer           = (*stream)(nil)
)

type stream struct {
	ctx         context.Context
	bucket      *blob.Bucket
	decoderFunc func(io.Reader) (Decoder, error)
	backoff     time.Duration

	next     []byte
	cursor   cursor
	blobTime time.Time
	reader   *blob.Reader
	decoder  Decoder
	err      error
}

// Close closes this stream and the current reader.
// Subsequent calls to Close or Recv will return an error.
func (s *stream) Close() error {
	if s.err != nil {
		// Already closed.
		return s.err
	}

	s.err = errors.New("closed")

	if s.reader == nil {
		return nil
	}

	return s.reader.Close()
}

func (s *stream) Recv() (*reflex.Event, error) {
	if s.err != nil {
		return nil, s.err
	}

	e, err := s.recv()
	if err != nil {
		s.err = err
		if s.reader != nil {
			// Close current reader.
			if err2 := s.reader.Close(); err2 != nil {
				return nil, err2
			}
		}
		return nil, err
	}

	return e, nil
}

func (s *stream) recv() (*reflex.Event, error) {
	for s.cursor.Key == "" || s.cursor.Last {
		// Starting from scratch or at end of a blob.
		if err := s.loadNextBlob(); err != nil {
			return nil, err
		}
	}

	if s.decoder == nil {
		// Starting from middle of a blob.
		if err := s.loadCurrentBlob(); err != nil {
			return nil, err
		}
	}

	temp, err := s.decoder.Decode()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errors.Wrap(err, "decode error")
	}

	s.cursor.Offset++
	s.cursor.Last = temp == nil

	e := &reflex.Event{
		ID:        s.cursor.String(),
		Type:      etype{},
		ForeignID: "",
		Timestamp: s.blobTime,
		MetaData:  s.next,
	}

	s.next = temp

	return e, nil
}

// loadCurrentBlob loads the blob decoder for the current cursor.
// It assumes the cursor is not at the end of the blob.
func (s *stream) loadCurrentBlob() error {

	if !s.blobTime.IsZero() {
		return errors.New("loading current while time set")
	}

	r, err := s.bucket.NewReader(s.ctx, s.cursor.Key, nil)
	if err != nil {
		return err
	}

	d, err := s.decoderFunc(r)
	if err != nil {
		return err
	}

	var i int64
	for {
		// Gobble events up to cursor.
		_, err := d.Decode()
		if errors.Is(err, io.EOF) {
			return errors.New("cursor out of range")
		} else if err != nil {
			return err
		}

		if i == s.cursor.Offset {
			break
		}
		i++
	}

	s.reader = r
	s.decoder = d
	s.blobTime = r.ModTime()
	s.next, err = d.Decode()
	if errors.Is(err, io.EOF) {
		return errors.New("current cursor is last")
	} else if err != nil {
		return err
	}

	return nil
}

// loadNextBlob waits until a subsequent blob is available then
// loads a decoder and cursor for it.
func (s *stream) loadNextBlob() error {
	for {
		next, err := getNextKey(s.ctx, s.bucket, s.cursor.Key)
		if errors.Is(err, io.EOF) {
			// No next keys, wait.
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(s.backoff):
				continue
			}
		} else if err != nil {
			return err
		}

		r, err := s.bucket.NewReader(s.ctx, next, nil)
		if err != nil {
			return err
		}

		d, err := s.decoderFunc(r)
		if err != nil {
			return err
		}

		s.next, err = d.Decode()
		if err != nil && !errors.Is(err, io.EOF) {
			return err
		}

		if s.reader != nil {
			// Close previous reader.
			if err := s.reader.Close(); err != nil {
				return err
			}
		}

		s.reader = r
		s.decoder = d
		s.blobTime = r.ModTime()
		s.cursor = cursor{
			Key:    next,
			Offset: -1,
			Last:   s.next == nil,
		}
		break
	}

	return nil
}

func getNextKey(ctx context.Context, bucket *blob.Bucket, prev string) (string, error) {
	iter := bucket.List(&blob.ListOptions{
		BeforeList: makeStartAfter(prev),
	})
	for {
		o, err := iter.Next(ctx)
		if err != nil {
			return "", err
		}

		if o.Key > prev {
			return o.Key, nil
		}
		// TODO(corver): Add metrics for non-optimal listing.
	}
}

// makeStartAfter returns a blob.BeforeList function that starts listing after
// the provided key for improved performance when scanning large buckets.
func makeStartAfter(key string) func(func(interface{}) bool) error {
	return func(asFunc func(interface{}) bool) error {
		s3input := new(s3.ListObjectsV2Input)
		if asFunc(&s3input) {
			after := path.Join(*s3input.Prefix, key)
			s3input.StartAfter = &after
		}
		return nil
	}
}

// cursor uniquely defines an event in a bucket of
// append-only ordered blobs.
type cursor struct {
	Key    string // Key of blob in the bucket.
	Offset int64  // Offset of event in the blob.
	Last   bool   // Last event in the blob.
}

func (c cursor) String() string {
	res := fmt.Sprintf("%s|%d", c.Key, c.Offset)
	if c.Last {
		res += "|last"
	}
	return res
}

func parseCursor(cur string) (cursor, error) {
	if cur == "" {
		return cursor{}, nil
	}

	split := strings.Split(cur, "|")
	if len(split) < 2 || len(split) > 3 {
		return cursor{}, errors.New("invalid cursor")
	}

	i, err := strconv.ParseInt(split[1], 10, 64)
	if err != nil {
		return cursor{}, errors.New("invalid cursor offset")
	}

	var last bool
	if len(split) == 3 {
		if split[2] != "last" {
			return cursor{}, errors.New("invalid cursor end")
		}
		last = true
	}

	return cursor{
		Key:    split[0],
		Offset: i,
		Last:   last,
	}, err
}

type etype struct{}

func (e etype) ReflexType() int {
	return 0
}
