package rblob_test

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	_ "gocloud.dev/blob/s3blob" // Driver for test

	"github.com/luno/reflex/rblob"
)

var (
	s3url   = flag.String("test_s3_url", "", "Define to enable s3 integration test")
	s3after = flag.String("test_s3_after", "", "Define to stream after this event id")

	modTimeS3URL    = flag.String("test_modtime_s3_url", "", "Define to enable modtime s3 integration test")
	modTimeS3After  = flag.String("test_modtime_s3_after", "", "Modtime cursor to resume streaming from")
	modTimeS3Prefix = flag.String("test_modtime_s3_prefix", "prefix", "Optional key prefix filter for modtime s3 test")
	modTimeS3Date   = flag.String("test_modtime_s3_date", "", "Date in YYYYMMDD format for date-prefix subtest (defaults to today UTC)")
)

// TestS3 provides an integration test for streaming json events from a s3 bucket. It prints
// event ids and metadata (content). It obtains the AWS session from the environment.
//
// Usage:
//
//	export URL="s3://my_bucket?prefix=optional/prefix/"
//	export AFTER_ID="" # Ex. set to '2020|eof' to start from 2020 if first part of key is year.
//	go test github.com/luno/reflex/rblob -v -run TestS3 -test_s3_url="$URL" -test_s3_after="$AFTER_ID"
func TestS3(t *testing.T) {
	if *s3url == "" {
		t.Skip("Skipping s3 integration test, test_s3_url flag empty.")
		return
	}

	if !strings.HasPrefix(*s3url, "s3://") {
		t.Errorf("test_s3_url requires 's3://' prefix")
		return
	}

	ctx := context.Background()

	b, err := rblob.OpenBucket(ctx, "", *s3url)
	require.NoError(t, err)

	sc, err := b.Stream(ctx, *s3after)
	require.NoError(t, err)

	for {
		e, err := sc.Recv()
		require.NoError(t, err)

		fmt.Println(e.ID)
		fmt.Printf("%s\n\n", e.MetaData)
	}
}

// TestModTimeS3 provides an integration test for streaming json events from an S3 bucket
// ordered by last-modified time. It prints event ids and metadata. AWS credentials are
// obtained from the environment.
//
// Run a specific subtest with -run TestModTimeS3/<name>. Each subtest streams indefinitely.
//
// Usage:
//
//	export URL="s3://my_bucket"
//	go test github.com/luno/reflex/rblob -v -run TestModTimeS3/stream_with_prefix \
//	  -test_modtime_s3_url="$URL" \
//	  -test_modtime_s3_after="$AFTER_ID" \
//	  -test_modtime_s3_prefix="some/path/"
//
//	go test github.com/luno/reflex/rblob -v -run TestModTimeS3/stream_with_date_prefix \
//	  -test_modtime_s3_url="$URL" \
//	  -test_modtime_s3_after="$AFTER_ID" \
//	  -test_modtime_s3_date="20260529"
func TestModTimeS3(t *testing.T) {
	if *modTimeS3URL == "" {
		t.Skip("Skipping modtime s3 integration test, test_modtime_s3_url flag empty.")
		return
	}

	if !strings.HasPrefix(*modTimeS3URL, "s3://") {
		t.Errorf("test_modtime_s3_url requires 's3://' prefix")
		return
	}

	ctx := context.Background()

	openBucket := func(t *testing.T, opts ...rblob.BucketOption[rblob.ModTimeBucket]) *rblob.ModTimeBucket {
		t.Helper()
		b, err := rblob.OpenModTimeBucket(ctx, "s3-modtime-test", *modTimeS3URL, opts...)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, b.Close()) })
		return b
	}

	stream := func(t *testing.T, b *rblob.ModTimeBucket) {
		t.Helper()
		sc, err := b.ModTimeStream(ctx, *modTimeS3After)
		require.NoError(t, err)
		for {
			e, err := sc.Recv()
			require.NoError(t, err)
			fmt.Println(e.ID)
			fmt.Printf("%s\n\n", e.MetaData)
		}
	}

	t.Run("stream with prefix", func(t *testing.T) {
		var opts []rblob.BucketOption[rblob.ModTimeBucket]
		if *modTimeS3Prefix != "" {
			opts = append(opts, rblob.WithModTimePrefix(*modTimeS3Prefix))
		}
		stream(t, openBucket(t, opts...))
	})

	t.Run("stream with date prefix", func(t *testing.T) {
		date := *modTimeS3Date
		if date == "" {
			date = time.Now().UTC().Format("20060502")
		}
		stream(t, openBucket(t, rblob.WithModTimePrefix(date+".")))
	})
}
