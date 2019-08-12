package reflex

import (
	"context"
	"strconv"

	"github.com/golang/protobuf/ptypes"
	"github.com/luno/fate"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex/reflexpb"
)

// StreamClientPB defines a common interface for reflex stream gRPC
// generated implementations.
type StreamClientPB interface {
	Recv() (*reflexpb.Event, error)
}

// WrapStreamPB wraps a gRPC client's Stream method and returns a StreamFunc.
func WrapStreamPB(wrap func(context.Context, *reflexpb.StreamRequest) (
	StreamClientPB, error)) StreamFunc {
	return func(ctx context.Context, after string, opts ...StreamOption) (StreamClient, error) {
		optionspb, err := streamOptionsToProto(opts)
		if err != nil {
			return nil, err
		}

		afterInt, _ := strconv.ParseInt(after, 10, 64) // Remove after migration

		cspb, err := wrap(ctx, &reflexpb.StreamRequest{
			After:    after,
			AfterInt: afterInt,
			Options:  optionspb,
		})
		if err != nil {
			return nil, err
		}

		return streamClientFromProto(cspb), nil
	}
}

// ConsumeClientPB defines a common interface for reflex consume gRPC
// generated implementations.
type ConsumeClientPB interface {
	Recv() (*reflexpb.Event, error)
	Send(*reflexpb.ConsumeRequest) error
}

// WrapConsumePB wraps a gRPC client's Consume method and returns a Consumable method.
func WrapConsumePB(wrap func(context.Context) (ConsumeClientPB, error)) func(context.Context,
	Consumer, ...StreamOption) error {
	return func(in context.Context, c Consumer, opts ...StreamOption) error {
		ctx, cancel := context.WithCancel(in)
		defer cancel()

		ccpb, err := wrap(ctx)
		if err != nil {
			return err
		}

		cc := consumeClientFromProto(ccpb)

		if err := cc.Init(c.Name().String(), opts...); err != nil {
			return errors.Wrap(err, "init error")
		}

		for {
			e, err := cc.Recv()
			if err != nil {
				return errors.Wrap(err, "recv error")
			}

			if err := c.Consume(ctx, fate.New(), e); err != nil {
				return errors.Wrap(err, "consumer error")
			}

			if err := cc.Ack(e.ID); err != nil {
				return errors.Wrap(err, "ack error")
			}
		}
	}
}

type consumeClient interface {
	Init(consumerID string, opts ...StreamOption) error
	Recv() (*Event, error)
	Ack(id string) error
}

func streamOptionsToProto(opts []StreamOption) (*reflexpb.StreamOptions, error) {
	options := new(StreamOptions)
	for _, o := range opts {
		o(options)
	}

	return &reflexpb.StreamOptions{
		Lag:      ptypes.DurationProto(options.Lag),
		FromHead: options.StreamFromHead,
	}, nil
}
