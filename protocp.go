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

type consumeclientpb struct {
	ConsumeClientPB
}

func (sc consumeclientpb) Recv() (*Event, error) {
	e, err := sc.ConsumeClientPB.Recv()
	if err != nil {
		return nil, err
	}
	return eventFromProto(e)
}

func (sc consumeclientpb) Ack(id string) error {
	idInt, _ := strconv.ParseInt(id, 10, 64) // Remove after migration
	req := &reflexpb.ConsumeRequest{
		AckId:    id,
		AckIdInt: idInt,
	}
	return sc.ConsumeClientPB.Send(req)
}

func (sc consumeclientpb) Init(consumerID string, opts ...StreamOption) error {
	optionspb, err := streamOptionsToProto(opts)
	if err != nil {
		return err
	}

	return sc.ConsumeClientPB.Send(&reflexpb.ConsumeRequest{
		ConsumerId: consumerID,
		Options:    optionspb,
	})
}

func consumeClientFromProto(sc ConsumeClientPB) consumeClient {
	return &consumeclientpb{sc}
}
