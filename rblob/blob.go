package rblob

import (
	"context"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"gocloud.dev/blob"

	"github.com/luno/reflex"
)

// Decoder decodes a blob into event byte slices (usually DTOs) which
// are streamed as event metadata.
type Decoder interface {
	// Decode returns the next non-empty byte slice or an error. It returns io.EOF if no more
	// are available.
	Decode() ([]byte, error)
}

// WithBackoff returns an option to configure the backoff duration
// before querying the underlying bucket for new blobs. It defaults
// to one minute.
func WithBackoff(d time.Duration) Option {
	return func(b *Bucket) {
		b.backoff = d
	}
}

// WithDecoder returns an option to configure the blob content decoder
// function. It defaults to the JSONDecoder.
func WithDecoder(fn func(io.Reader) (Decoder, error)) Option {
	return func(b *Bucket) {
		b.decoderFunc = fn
	}
}

// Option is a functional option that configures a bucket.
type Option func(*Bucket)

// OpenBucket opens and returns a bucket for the provided url.
//
// label defines the bucket label used for metrics.
//
// urlstr defines the url of the blob bucket. See the gocloud
// URLOpener documentation in driver subpackages for details
// on supported URL formats. Also see https://gocloud.dev/concepts/urls/
// and https://gocloud.dev/howto/blob/.
func OpenBucket(ctx context.Context, label, urlstr string,
	opts ...Option,
) (*Bucket, error) {
	bucket, err := blob.OpenBucket(ctx, urlstr)
	if err != nil {
		return nil, err
	}

	return NewBucket(label, bucket, opts...), nil
}

// NewBucket returns a bucket using the provided underlying bucket.
func NewBucket(label string, bucket *blob.Bucket, opts ...Option) *Bucket {
	b := &Bucket{
		label:       label,
		bucket:      bucket,
		decoderFunc: JSONDecoder,
		backoff:     time.Minute,
	}

	for _, opt := range opts {
		opt(b)
	}

	return b
}

// Bucket defines a bucket from which to stream the content of
// consecutive blobs as events.
type Bucket struct {
	label       string
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
	opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	if len(opts) > 0 {
		return nil, errors.New("options not supported yet")
	}

	cursor, err := parseCursor(after)
	if err != nil {
		return nil, err
	}

	return &stream{
		ctx:         ctx,
		label:       b.label,
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
	label       string
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
// Subsequent calls to Close or Recv always return an error.
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
	if err == nil {
		return e, nil
	}

	// Handle receive error
	s.err = err

	if s.reader != nil {
		// Close current reader.
		if closeErr := s.reader.Close(); closeErr != nil {
			log.Error(s.ctx, errors.Wrap(closeErr, "reader close"))
		}
	}

	return nil, err
}

func (s *stream) recv() (*reflex.Event, error) {
	for s.cursor.Key == "" || s.cursor.EOF {
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

	peek, err := s.decoder.Decode()
	if errors.Is(err, io.EOF) {
		s.cursor.EOF = true
	} else if err != nil {
		return nil, errors.Wrap(err, "decode")
	}

	s.cursor.Offset++

	e := &reflex.Event{
		ID:        s.cursor.String(),
		Type:      etype{},
		ForeignID: "",
		Timestamp: s.blobTime,
		MetaData:  s.next,
	}

	s.next = peek

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
		return errors.Wrap(err, "new reader")
	}

	readCounter.WithLabelValues(s.label).Inc()

	d, err := s.decoderFunc(r)
	if err != nil {
		return err
	}

	// Gobble events up to cursor.
	for i := int64(0); i <= s.cursor.Offset; i++ {
		_, err := d.Decode()
		if errors.Is(err, io.EOF) {
			return errors.New("cursor out of range")
		} else if err != nil {
			return errors.Wrap(err, "decode")
		}
	}

	s.reader = r
	s.decoder = d
	s.blobTime = r.ModTime()
	s.next, err = d.Decode()
	if errors.Is(err, io.EOF) {
		return errors.New("cursor was eof")
	} else if err != nil {
		return errors.Wrap(err, "decode")
	}

	return nil
}

// loadNextBlob waits until a subsequent blob is available then
// loads a decoder and cursor for it.
func (s *stream) loadNextBlob() error {
	var key string
	for {
		var err error
		key, err = getNextKey(s.ctx, s.label, s.bucket, s.cursor.Key)
		if errors.Is(err, io.EOF) {
			// No key keys, wait.
			select {
			case <-s.ctx.Done():
				return s.ctx.Err()
			case <-time.After(s.backoff):
				continue
			}
		} else if err != nil {
			return err
		}
		break
	}

	c := cursor{
		Key:    key,
		Offset: -1,
	}

	r, err := s.bucket.NewReader(s.ctx, key, nil)
	if err != nil {
		return errors.Wrap(err, "new reader")
	}

	readCounter.WithLabelValues(s.label).Inc()

	d, err := s.decoderFunc(r)
	if err != nil {
		return err
	}

	next, err := d.Decode()
	if errors.Is(err, io.EOF) {
		c.EOF = true
	} else if err != nil {
		return errors.Wrap(err, "decode")
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
	s.cursor = c
	s.next = next

	return nil
}

func getNextKey(ctx context.Context, label string, bucket *blob.Bucket, prev string) (string, error) {
	iter := bucket.List(&blob.ListOptions{
		BeforeList: makeStartAfter(prev),
	})

	for {
		o, err := iter.Next(ctx)
		if err != nil {
			return "", errors.Wrap(err, "list iter")
		}

		if o.Key > prev {
			return o.Key, nil
		}

		listSkipCounter.WithLabelValues(label).Inc()
	}
}

// makeStartAfter returns a blob.BeforeList function that starts listing after
// the provided key for improved performance when scanning large buckets.
func makeStartAfter(key string) func(func(interface{}) bool) error {
	return func(asFunc func(interface{}) bool) error {
		s3input := new(s3.ListObjectsV2Input)
		if !asFunc(&s3input) {
			// We always expect asFunc to return true.
			return errors.New("gocloud.dev rejected our ListObjectsV2Input - check gocloud.dev/blob/s3blob")
		}
		if s3input.Prefix != nil {
			key = path.Join(*s3input.Prefix, key)
		}
		s3input.StartAfter = &key
		return nil
	}
}

// cursor uniquely defines an event in a bucket of
// append-only ordered blobs.
type cursor struct {
	Key    string // Key of blob in the bucket.
	Offset int64  // Offset of event in the blob.
	EOF    bool   // End of blob reached (overrides Offset).
}

// eof as cursor offset indicates it has reached the enf of a blob.
const eof = "eof"

// String returns a string format of the cursor which is lexigraphically orderable.
// Ex. path/to/file|01|9 or path/to/file|03|123 or path/to/file|eof.
func (c cursor) String() string {
	if c.EOF {
		return fmt.Sprintf("%s|%s", c.Key, eof)
	}

	offset := strconv.FormatInt(c.Offset, 10)

	return fmt.Sprintf("%s|%02d|%s", c.Key, len(offset), offset)
}

func parseCursor(cur string) (cursor, error) {
	if cur == "" {
		return cursor{}, nil
	}

	split := strings.Split(cur, "|")
	if len(split) < 2 || len(split) > 3 {
		return cursor{}, errors.New("invalid cursor", j.KS("cursor", cur))
	}

	if split[1] == eof {
		return cursor{
			Key: split[0],
			EOF: true,
		}, nil
	}

	i, err := strconv.ParseInt(split[len(split)-1], 10, 64)
	if err != nil {
		return cursor{}, errors.New("invalid cursor offset", j.KS("cursor", cur))
	}

	return cursor{
		Key:    split[0],
		Offset: i,
	}, nil
}

type etype struct{}

func (e etype) ReflexType() int {
	return 0
}
