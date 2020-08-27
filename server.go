package reflex

import (
	"context"
	"io"

	"github.com/luno/jettison/errors"
	"github.com/luno/reflex/reflexpb"
)

type streamServerPB interface {
	Context() context.Context
	Send(*reflexpb.Event) error
}

// NewServer returns a new server.
func NewServer() *Server {
	return &Server{
		stop: make(chan struct{}, 0),
	}
}

// Server provides stream, consume and graceful shutdown functionality
// for use in a gRPC server.
type Server struct {
	stop chan struct{}
}

// Stop stops serving gRPC stream and consume methods returning ErrStopped.
// It should be used for graceful shutdown. It panics if called more than once.
func (s *Server) Stop() {
	close(s.stop)
}

func (s *Server) maybeErrStopped() error {
	select {
	case <-s.stop:
		return ErrStopped
	default:
		return nil
	}
}

// Stream streams events for a gRPC stream method. It always returns a non-nil error.
// It returns ErrStopped if the server is stopped.
// Note that back pressure is achieved by gRPC Streams' 64KB send and receive buffers.
// Note that gRPC does not guarantee buffered messages being sent on the wire, see
// https://github.com/grpc/grpc-go/issues/2159
func (s *Server) Stream(sFn StreamFunc, req *reflexpb.StreamRequest, sspb streamServerPB) error {
	if err := s.maybeErrStopped(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(sspb.Context())
	defer cancel()

	stopper := func() error {
		return awaitStop(ctx, s.stop)
	}

	streamer := func() error {
		sc, err := sFn(ctx, req.After, optsFromProto(req.Options)...)
		if err != nil {
			return err
		}
		return serveStream(sspb, sc)
	}

	var err error
	select {
	case err = <-goChan(stopper):
	case err = <-goChan(streamer):
	}
	return err
}

// serveStream streams the events from StreamClient to streamServerPB.
// To stop, cancel the streamServerPB's context.
// It always returns a non-nil error.
func serveStream(ss streamServerPB, sc StreamClient) error {
	ctx := ss.Context()

	// Ensure close if stream client is a closer.
	if closer, ok := sc.(io.Closer); ok {
		defer closer.Close()
	}

	for {
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "context error")
		}

		e, err := sc.Recv()
		if err != nil {
			return errors.Wrap(err, "recv error 2")
		}

		pb, err := eventToProto(e)
		if err != nil {
			return errors.Wrap(err, "to proto error")
		}

		if err := ss.Send(pb); err != nil {
			return errors.Wrap(err, "send error")
		}
	}
}

func goChan(f func() error) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- f() // will never block since buffered
		close(ch)
	}()
	return ch
}

func awaitStop(ctx context.Context, stop <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return ErrStopped
	}
}
