package rblob

// Decoder decodes a blob into event byte slices (usually DTOs) which
// are streamed as event metadata.
type Decoder interface {
	// Decode returns the next non-empty byte slice or an error. It returns io.EOF if no more
	// are available.
	Decode() ([]byte, error)
}

// BucketOption is a generic functional option that configures a bucket of type T.
type BucketOption[T any] func(*T)
