package grpctest

import (
	"context"
	"fmt"
	"testing"

	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

type Client struct {
	clpb reflexpb.ReflexClient
	conn *grpc.ClientConn
}

func NewClient(_ testing.TB, url string) *Client {
	conn, err := grpc.Dial(url, grpc.WithInsecure())
	if err != nil {
		panic(fmt.Errorf("grpc.Dial error: %s", err.Error()))
	}

	return &Client{
		conn: conn,
		clpb: reflexpb.NewReflexClient(conn),
	}
}

func (cl *Client) StreamEvents(ctx context.Context, after string,
	opts ...reflex.StreamOption) (reflex.StreamClient, error) {

	sFn := reflex.WrapStreamPB(func(ctx context.Context,
		req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return cl.clpb.Stream(ctx, req)
	})

	return sFn(ctx, after, opts...)
}

func (cl *Client) ConsumeEvents(ctx context.Context,
	c reflex.Consumer, opts ...reflex.StreamOption) error {

	cFn := reflex.WrapConsumePB(func(ctx context.Context) (reflex.ConsumeClientPB, error) {
		return cl.clpb.Consume(ctx)
	})

	return cFn(ctx, c, opts...)
}

func (cl *Client) Close() error {
	return cl.conn.Close()
}
