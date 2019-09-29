package server

import (
	"context"
	"database/sql"
	"net"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex"
	"github.com/luno/reflex/example/exserver/db"
	pb "github.com/luno/reflex/example/exserver/exserverpb"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

// Server is a gRPC server to serve RPC requests.
type Server struct {
	events1StreamFunc reflex.StreamFunc
	events2StreamFunc reflex.StreamFunc
	cursorStore       reflex.CursorStore
	rserver           *reflex.Server
	grpcServer        *grpc.Server
}

// Compile time check that Server satisfies pb.YangServer interface.
var _ pb.ExServerServer = (*Server)(nil)

// New returns a new server.
func New(dbc *sql.DB) *Server {
	return &Server{
		events1StreamFunc: db.Events1.ToStream(dbc),
		events2StreamFunc: db.Events2.ToStream(dbc),
		cursorStore:       db.Cursors.ToStore(dbc),
		rserver:           reflex.NewServer(),
	}
}

// ServeForever creates and runs a gprc server.
func (srv *Server) ServeForever(grpcAddress string) error {
	if srv.grpcServer != nil {
		return errors.New("server already started")
	}
	lis, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()

	srv.grpcServer = grpcServer

	pb.RegisterExServerServer(grpcServer, srv)
	return grpcServer.Serve(lis)
}

func (srv *Server) StreamEvent1(req *reflexpb.StreamRequest, ss pb.ExServer_StreamEvent1Server) error {
	return srv.rserver.Stream(srv.events1StreamFunc, req, ss)
}

func (srv *Server) StreamEvent2(req *reflexpb.StreamRequest, ss pb.ExServer_StreamEvent2Server) error {
	return srv.rserver.Stream(srv.events2StreamFunc, req, ss)
}

func (srv *Server) Echo(_ context.Context, req *pb.EchoMsg) (*pb.EchoMsg, error) {
	return req, nil
}

func (srv *Server) Stop() {
	srv.rserver.Stop()
	srv.grpcServer.GracefulStop()
}
