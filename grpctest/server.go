package grpctest

import (
	"fmt"
	"net"
	"testing"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/log"
	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
	"google.golang.org/grpc"
)

// NewServer starts and returns a reflex server and its address.
func NewServer(_ testing.TB, stream reflex.StreamFunc,
	cstore reflex.CursorStore) (*Server, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("net.Listen error: %v", err))
	}

	grpcServer := grpc.NewServer()

	srv := &Server{
		stream:     stream,
		cstore:     cstore,
		grpcServer: grpcServer,
		rserver:    reflex.NewServer(),
	}

	reflexpb.RegisterReflexServer(grpcServer, srv)

	go func() {
		err := grpcServer.Serve(l)
		if err != nil {
			log.Error(nil, errors.Wrap(err, "grpcServer.Server error: %v"))
		}
	}()

	return srv, l.Addr().String()
}

var _ reflexpb.ReflexServer = (*Server)(nil)

type Server struct {
	grpcServer *grpc.Server
	stream     reflex.StreamFunc
	cstore     reflex.CursorStore
	rserver    *reflex.Server
}

func (srv *Server) Stream(req *reflexpb.StreamRequest,
	ss reflexpb.Reflex_StreamServer) error {
	return srv.rserver.Stream(srv.stream, req, ss)
}

func (srv *Server) Consume(cs reflexpb.Reflex_ConsumeServer) error {
	return srv.rserver.Consume(srv.stream, srv.cstore, cs)
}

func (srv *Server) Stop() {
	srv.rserver.Stop()
	srv.grpcServer.GracefulStop()
}
