package rblob

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/luno/jettison/errors"
)

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

	key, rest, found := strings.Cut(s, "|")
	if !found {
		return modtimeCursor{}, errors.New("invalid modtime cursor: missing separator")
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
