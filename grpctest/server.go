package grpctest

import (
	"fmt"
	"net"
	"testing"

	"github.com/luno/jettison/errors"
	"github.com/luno/jettison/interceptors"
	"github.com/luno/jettison/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"

	"github.com/luno/reflex"
	"github.com/luno/reflex/reflexpb"
)

// NewServer starts and returns a reflex server and its address.
func NewServer(_ testing.TB, stream reflex.StreamFunc,
	cstore reflex.CursorStore,
) (*Server, string) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("net.Listen error: %v", err))
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(interceptors.UnaryServerInterceptor),
		grpc.StreamInterceptor(interceptors.StreamServerInterceptor))

	srv := &Server{
		stream:      stream,
		cstore:      cstore,
		grpcServer:  grpcServer,
		rserver:     reflex.NewServer(),
		sentCounter: prometheus.NewCounter(prometheus.CounterOpts{Name: "sent_total"}),
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

// Server is an gRPC streaming server for testing
type Server struct {
	grpcServer  *grpc.Server
	stream      reflex.StreamFunc
	cstore      reflex.CursorStore
	rserver     *reflex.Server
	sentCounter prometheus.Counter
	reflexpb.UnimplementedReflexServer
}

// Stream creates a new stream
func (srv *Server) Stream(req *reflexpb.StreamRequest,
	ss reflexpb.Reflex_StreamServer,
) error {
	return srv.rserver.Stream(srv.stream, req, &counter{ss, srv.sentCounter})
}

// SentCount is the number of events sent
func (srv *Server) SentCount() float64 {
	return testutil.ToFloat64(srv.sentCounter)
}

// Stop ends any streams and stops the gRPC server
func (srv *Server) Stop() {
	srv.rserver.Stop()
	srv.grpcServer.GracefulStop()
}

type counter struct {
	reflexpb.Reflex_StreamServer
	counter prometheus.Counter
}

func (c *counter) Send(e *reflexpb.Event) error {
	c.counter.Inc()
	return c.Reflex_StreamServer.Send(e)
}
