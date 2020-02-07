package rpatterns

import (
	"context"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/j"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"google.golang.org/grpc/status"
)

// RunForever continuously calls the run function, backing off
// and logging on unexpected errors.
func RunForever(getCtx func() context.Context, req reflex.Spec) {
	for {
		ctx := getCtx()

		err := reflex.Run(ctx, req)
		if isExpected(err) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "run forever error"),
			j.KS("consumer", req.Name()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}

var (
	cancelProto   = status.FromContextError(context.Canceled).Proto()
	deadlineProto = status.FromContextError(context.DeadlineExceeded).Proto()
)

// isExpected returns true if the error is expected during normal streaming operation.
func isExpected(err error) bool {
	if errors.IsAny(err, context.Canceled, context.DeadlineExceeded, reflex.ErrStopped, fate.ErrTempt) {
		return true
	}

	// Check if this is a grpc status error.
	if se, ok := err.(interface{ GRPCStatus() *status.Status }); ok {
		pb := se.GRPCStatus().Proto()
		return proto.Equal(pb, cancelProto) || proto.Equal(pb, deadlineProto)
	}

	return false
}

// ConsumeForever continuously runs the consume function, backing off
// and logging on unexpected errors.
// Deprecated: Please use RunForever.
func ConsumeForever(getCtx func() context.Context, consume reflex.ConsumeFunc,
	consumer reflex.Consumer, opts ...reflex.StreamOption) {
	for {
		ctx := getCtx()

		err := consume(ctx, consumer, opts...)
		if errors.IsAny(err, context.Canceled, reflex.ErrStopped, fate.ErrTempt) {
			// Just retry on expected errors.
			time.Sleep(time.Millisecond * 100) // Don't spin
			continue
		}

		log.Error(ctx, errors.Wrap(err, "consume forever error"),
			j.KS("consumer", consumer.Name()))
		time.Sleep(time.Minute) // 1 min backoff on errors
	}
}
