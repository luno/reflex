package grpctest

import (
	"context"
	"fmt"
	"testing"

	grpc_jettison "github.com/luno/jettison/grpc"
	"google.golang.org/grpc"

	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

// Client wraps a gRPC client connection
type Client struct {
	clpb reflexpb.ReflexClient
	conn *grpc.ClientConn
}

// NewClient connects to a gRPC server and creates the reflex client
func NewClient(_ testing.TB, url string) *Client {
	conn, err := grpc.Dial(url, grpc.WithInsecure(),
		grpc.WithUnaryInterceptor(grpc_jettison.UnaryClientInterceptor),
		grpc.WithStreamInterceptor(grpc_jettison.StreamClientInterceptor))
	if err != nil {
		panic(fmt.Errorf("grpc.Dial error: %s", err.Error()))
	}

	return &Client{
		conn: conn,
		clpb: reflexpb.NewReflexClient(conn),
	}
}

// StreamEvents wraps a gRPC stream, feeding the events to reflex
func (cl *Client) StreamEvents(ctx context.Context, after string,
	opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	sFn := reflex.WrapStreamPB(func(ctx context.Context,
		req *reflexpb.StreamRequest,
	) (reflex.StreamClientPB, error) {
		return cl.clpb.Stream(ctx, req)
	})

	return sFn(ctx, after, opts...)
}

// Close ends the stream
func (cl *Client) Close() error {
	return cl.conn.Close()
}
