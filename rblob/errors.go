package rblob

import "github.com/luno/jettison/errors"

var (
	errModTimeCursorMissingSeparator = errors.New("invalid modtime cursor: missing separator")
	errModTimeCursorEmptyKey         = errors.New("invalid modtime cursor: empty key")
	errModTimeCursorBadUnixNano      = errors.New("invalid modtime cursor: bad unix-nano")
	errModTimeCursorBadOffset        = errors.New("invalid modtime cursor: bad offset")
)
