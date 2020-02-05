package reflex

import (
	"log"
	"strconv"

	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/duration"
	"github.com/luno/reflex/reflexpb"
)

func eventToProto(e *Event) (*reflexpb.Event, error) {
	ts, err := ptypes.TimestampProto(e.Timestamp)
	if err != nil {
		return nil, err
	}

	return &reflexpb.Event{
		Id:        e.ID,
		IdInt:     e.IDInt(),
		ForeignId: e.ForeignID,
		Type:      int32(e.Type.ReflexType()),
		Timestamp: ts,
		Metadata:  e.MetaData,
	}, nil
}

func eventFromProto(e *reflexpb.Event) (*Event, error) {
	ts, err := ptypes.Timestamp(e.Timestamp)
	if err != nil {
		return nil, err
	}

	id := e.Id
	if id == "" {
		id = strconv.FormatInt(e.IdInt, 10)
	}

	return &Event{
		ID:        id,
		ForeignID: e.ForeignId,
		Type:      eventType(e.Type),
		Timestamp: ts,
		MetaData:  e.Metadata,
	}, nil
}

type streamclientpb struct {
	StreamClientPB
}

func (c streamclientpb) Recv() (*Event, error) {
	e, err := c.StreamClientPB.Recv()
	if err != nil {
		return nil, err
	}
	return eventFromProto(e)
}

func streamClientFromProto(sc StreamClientPB) StreamClient {
	return &streamclientpb{sc}
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
		} else if d > 0 {
			opts = append(opts, WithStreamLag(d))
		}
	}

	if options.FromHead {
		opts = append(opts, WithStreamFromHead())
	}

	return opts
}

func optsToProto(opts []StreamOption) (*reflexpb.StreamOptions, error) {
	options := new(StreamOptions)
	for _, o := range opts {
		o(options)
	}

	var lag *duration.Duration
	if options.Lag > 0 {
		lag = ptypes.DurationProto(options.Lag)
	}

	return &reflexpb.StreamOptions{
		Lag:      lag,
		FromHead: options.StreamFromHead,
	}, nil
}
