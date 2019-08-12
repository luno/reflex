package reflex

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

// ErrStopped is returned when an events table is stopped, usually
// when the grpc server is stopped. Clients should check for this error
// and reconnect.
var ErrStopped = errors.New("the event stream has been stopped", j.C("ERR_09290f5944cb8671"))

func IsStoppedErr(err error) bool {
	return errors.Is(err, ErrStopped)
}
