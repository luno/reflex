package rblob

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
)

// --------------------------------------------------------------------------------------------
// This differs from the regular blob functionality, in that it is designed to resume
// its cursor based on the comparisions on last modified rather than on lexicographic
// ordering. Since this is not supported in the AWS ListObjectsV2 client (https://docs.aws.amazon.com/cli/latest/reference/s3api/list-objects-v2.html)
// (on the day this is documented 2026-05-26). This should only be used for small to
// medium size S3 buckets. Reason being that the cursor will need to iterate through all
// the last modified list objects to determine its resume position.
// --------------------------------------------------------------------------------------------

// modtimeCursor identifies the last-processed S3 object by its key and
// last-modified time. ModTime is used for ordering; Key breaks ties and
// provides identity for the resume skip.
type modtimeCursor struct {
	Key     string
	ModTime time.Time
}

// String returns the string representation of the modtimeCursor.
//
// If the cursor key is empty, an empty string is returned.
// Otherwise, the result is formatted as:
//
//	<key>|<modtime_unix_nano>
//
// where modtime_unix_nano is the modification time in Unix nanoseconds.
func (c modtimeCursor) String() string {
	if c.Key == "" {
		return ""
	}
	return fmt.Sprintf("%s|%d", c.Key, c.ModTime.UnixNano())
}

// parseModTimeCursor parses a modtime cursor string into a modtimeCursor value.
//
// The expected format is:
//
//	<key>|<modtime_unix_nano>
//
// where modtime_unix_nano is a Unix timestamp in nanoseconds.
//
// An empty string returns an empty modtimeCursor and no error.
// An error is returned if the cursor format is invalid or the timestamp
// cannot be parsed.
func parseModTimeCursor(s string) (modtimeCursor, error) {
	if s == "" {
		return modtimeCursor{}, nil
	}

	idx := strings.LastIndex(s, "|")
	if idx < 0 {
		return modtimeCursor{}, errors.New("invalid modtime cursor: missing separator")
	}
	key := s[:idx]
	rest := s[idx+1:]
	if key == "" {
		return modtimeCursor{}, errors.New("invalid modtime cursor: empty key")
	}

	ns, err := strconv.ParseInt(rest, 10, 64)
	if err != nil {
		return modtimeCursor{}, errors.New("invalid modtime cursor: bad unix-nano")
	}

	return modtimeCursor{
		Key:     key,
		ModTime: time.Unix(0, ns).UTC(),
	}, nil
}
