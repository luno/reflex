package reflex

import (
	"strconv"

	"github.com/golang/protobuf/ptypes"
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
