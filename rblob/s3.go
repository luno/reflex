package rblob

import (
	"context"

	"github.com/aws/aws-sdk-go/aws/session"
	"gocloud.dev/blob/s3blob"
)

// OpenS3Bucket opens and returns a s3 bucket for the provided session and url.
// See Openbucket which can also open s3 bucket urls, but obtains the AWS session from the
// environment.
func OpenS3Bucket(ctx context.Context, sess *session.Session, url string,
	opts ...option) (*Bucket, error) {

	bucket, err := s3blob.OpenBucket(ctx, sess, url, nil)
	if err != nil {
		return nil, err
	}

	return newBucket(bucket, opts...), nil
}
