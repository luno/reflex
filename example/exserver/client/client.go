package client

import (
	"context"
	"flag"

	"google.golang.org/grpc"

	"github.com/luno/reflex"
	"github.com/luno/reflex/example/exserver"
	"github.com/luno/reflex/example/exserver/exserverpb"
	"github.com/luno/reflex/reflexpb"
)

var serverAddr = flag.String("server_addr", ":1234",
	"Address of an exserver gRPC service.")

type client struct {
	conn *grpc.ClientConn
	clpb exserverpb.ExServerClient
}

// New returns a new exserver client.
func New() (exserver.Client, error) {
	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	return &client{
		conn: conn,
		clpb: exserverpb.NewExServerClient(conn),
	}, nil
}

func (cl *client) StreamEvents1(ctx context.Context, after string,
	opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	sFn := reflex.WrapStreamPB(func(ctx context.Context, req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return cl.clpb.StreamEvent1(ctx, req)
	})

	return sFn(ctx, after, opts...)
}

func (cl *client) StreamEvents2(ctx context.Context, after string,
	opts ...reflex.StreamOption,
) (reflex.StreamClient, error) {
	sFn := reflex.WrapStreamPB(func(ctx context.Context, req *reflexpb.StreamRequest) (reflex.StreamClientPB, error) {
		return cl.clpb.StreamEvent2(ctx, req)
	})

	return sFn(ctx, after, opts...)
}

func (cl *client) Echo(ctx context.Context, msg string) (string, error) {
	res, err := cl.clpb.Echo(ctx, &exserverpb.EchoMsg{Message: msg})
	if err != nil {
		return "", err
	}
	return res.Message, nil
}
