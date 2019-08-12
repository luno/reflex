package reflex

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/golang/protobuf/ptypes"
	"github.com/luno/jettison/errors"
	"github.com/luno/reflex/reflexpb"
)

const consumeInitTimeout = time.Second * 30 // timeout waiting for consumer client to init stream.

type streamServerPB interface {
	Context() context.Context
	Send(*reflexpb.Event) error
}

type consumeServerPB interface {
	Context() context.Context
	Send(*reflexpb.Event) error
	Recv() (*reflexpb.ConsumeRequest, error)
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

// Stream streams events for a gRPC Stream method. It always returns a non-nil error.
// It returns ErrStopped if the server is stopped.
// Note that back pressure is achieved by gRPC Streams' 64KB send and receive buffers.
// Note that gRPC does not guarantee buffered messages being sent on the wire, see
// https://github.com/grpc/grpc-go/issues/2159
func (s *Server) Stream(sFn StreamFunc, req *reflexpb.StreamRequest, sspb streamServerPB) error {
	if err := s.maybeErrStopped(); err != nil {
		return err
	}

	after := req.After
	if after == "" && req.AfterInt != 0 {
		after = strconv.FormatInt(req.AfterInt, 10)
	}

	ctx, cancel := context.WithCancel(sspb.Context())
	defer cancel()

	stopper := func() error {
		return awaitStop(ctx, s.stop)
	}

	streamer := func() error {
		sc, err := sFn(ctx, after, optsFromProto(req.Options)...)
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

// Consume proxies consumable by streaming events and persisting cursor acks
// for a gRPC Consume method. It always returns a non-nil error.
func (s *Server) Consume(sFn StreamFunc, cs CursorStore, cspb consumeServerPB) error {
	if err := s.maybeErrStopped(); err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(cspb.Context())
	defer cs.Flush(context.Background()) // best effort flush with new context
	defer cancel()

	// init: read consumerID and options
	var req *reflexpb.ConsumeRequest
	err := withTimeout(consumeInitTimeout, func() error {
		var err error
		req, err = cspb.Recv()
		return err
	})
	if err != nil {
		return errors.Wrap(err, "error receiving init")
	}
	if req.ConsumerId == "" {
		return errors.New("init request missing consumerID")
	}

	cursor, err := cs.GetCursor(ctx, req.ConsumerId)
	if err != nil {
		return errors.Wrap(err, "get cursor error")
	}

	stopper := func() error {
		return awaitStop(ctx, s.stop)
	}

	acker := func() error {
		return handleAcks(ctx, cspb, cs, req.ConsumerId)
	}

	streamer := func() error {
		sc, err := sFn(ctx, cursor, optsFromProto(req.Options)...)
		if err != nil {
			return err
		}
		return serveStream(cspb, sc)
	}

	select {
	case err = <-goChan(stopper):
	case err = <-goChan(acker):
	case err = <-goChan(streamer):
	}
	return err
}

// serveStream streams the events from StreamClient to streamServerPB.
// To stop, cancel the streamServerPB's context.
// It always returns a non-nil error.
func serveStream(ss streamServerPB, sc StreamClient) error {
	ctx := ss.Context()

	for {
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "context error")
		}

		e, err := sc.Recv()
		if err != nil {
			return errors.Wrap(err, "recv error")
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

func withTimeout(d time.Duration, f func() error) error {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case err := <-goChan(f):
		return err
	case <-t.C:
		return errors.New("timeout")
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

// optsFromProto returns a slice of StreamOptions converted from the proto
// message options. Conversion errors are unexpected, so only logged.
func optsFromProto(options *reflexpb.StreamOptions) []StreamOption {
	var opts []StreamOption
	if options == nil {
		return opts
	}

	if options.Lag != nil {
		d, err := ptypes.Duration(options.Lag)
		if err != nil {
			log.Printf("reflex: Error parsing request option lag: %v", err)
		} else {
			opts = append(opts, WithStreamLag(d))
		}
	}

	if options.FromHead {
		opts = append(opts, WithStreamFromHead())
	}

	return opts
}

func handleAcks(ctx context.Context, cspb consumeServerPB,
	cs CursorStore, consumerID string) error {
	for {
		if ctx.Err() != nil {
			return errors.Wrap(ctx.Err(), "ack context error")
		}

		req, err := cspb.Recv()
		if err != nil {
			return errors.Wrap(err, "error receiving ack")
		}

		ack := req.AckId
		if ack == "" && req.AckIdInt != 0 {
			ack = strconv.FormatInt(req.AckIdInt, 10)
		}

		if err := cs.SetCursor(ctx, consumerID, ack); err != nil {
			return errors.Wrap(err, "error setting cursor")
		}
	}
}

func awaitStop(ctx context.Context, stop <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stop:
		return ErrStopped
	}
}
