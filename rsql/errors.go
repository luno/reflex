package rsql

import (
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
)

var (
	ErrConsecEvent        = errors.New("non-consecutive event ids", j.C("ERR_bc3dcacb92b9761f"))
	ErrInvalidIntID       = errors.New("invalid id, only int supported", j.C("ERR_82d0368b5478d378"))
	ErrNextCursorMismatch = errors.New("next cursor and last event id mismatch", j.C("ERR_f647fa25c00140d2"))
)
