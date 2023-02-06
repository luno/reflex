package rsql

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

var (
	// ErrConsecEvent occurs when the difference between the ids of two consecutive events is not 1.
	ErrConsecEvent = errors.New("non-consecutive event ids", j.C("ERR_bc3dcacb92b9761f"))
	// ErrInvalidIntID occurs when a non-int value is specified for an integer id
	ErrInvalidIntID = errors.New("invalid id, only int supported", j.C("ERR_82d0368b5478d378"))
)
