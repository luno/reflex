package reflex

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"google.golang.org/grpc/status"
)

const (
	filterErrMsg = "error in filter"
)

var (
	// ErrStopped is returned when an events table is stopped, usually
	// when the grpc server is stopped. Clients should check for this error
	// and reconnect.
	ErrStopped = errors.New("the event stream has been stopped", j.C("ERR_09290f5944cb8671"))

	// ErrHeadReached is returned when the events table reaches the end of
	// the stream of events.
	ErrHeadReached = errors.New("the event stream has reached the current head", j.C("ERR_b4b155d2a91cfcd0"))

	cancelProto   = status.FromContextError(context.Canceled).Proto()
	deadlineProto = status.FromContextError(context.DeadlineExceeded).Proto()

	filterErr = errors.New(filterErrMsg)
)

// IsStoppedErr checks whether err is an ErrStopped
func IsStoppedErr(err error) bool {
	return errors.Is(err, ErrStopped)
}

// IsHeadReachedErr checks whether err is an ErrHeadReached
func IsHeadReachedErr(err error) bool {
	return errors.Is(err, ErrHeadReached)
}

// IsExpected returns true if the error is expected during normal streaming operation.
func IsExpected(err error) bool {
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded, ErrStopped, fate.ErrTempt) {
		return true
	}

	// Check if this is a grpc status error.
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		pb := se.GRPCStatus().Proto()
		return proto.Equal(pb, cancelProto) || proto.Equal(pb, deadlineProto)
	}

	return false
}

// IsFilterErr returns true if the error occurred during Event filtering operations.
func IsFilterErr(err error) bool {
	return errors.Is(err, filterErr)
}

func asFilterErr(err error) error {
	return errors.Wrap(err, filterErrMsg)
}
