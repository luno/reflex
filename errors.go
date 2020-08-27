package reflex

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

// ErrStopped is returned when an events table is stopped, usually
// when the grpc server is stopped. Clients should check for this error
// and reconnect.
var (
	ErrStopped     = errors.New("the event stream has been stopped", j.C("ERR_09290f5944cb8671"))
	ErrHeadReached = errors.New("the event stream has reached the current head", j.C("ERR_b4b155d2a91cfcd0"))
)

func IsStoppedErr(err error) bool {
	return errors.Is(err, ErrStopped)
}

func IsHeadReachedErr(err error) bool {
	return errors.Is(err, ErrHeadReached)
}
