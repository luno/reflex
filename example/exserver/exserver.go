package exserver

import (
	"context"

	"github.com/luno/reflex"
)

type ExEvent = reflex.Event

type ExEventType int

func (t ExEventType) ReflexType() int {
	return int(t)
}

const (
	EventTypeUnknown ExEventType = 0
	EventTypeInsert  ExEventType = 1
	EventTypeUpdate  ExEventType = 2
)

type ExConsumerName = reflex.ConsumerName

const (
	ConsumerNameUnknown          ExConsumerName = "unknown"
	ConsumerNameInternalLoop     ExConsumerName = "internal_exserver_loop"
	ConsumerNameInternalConsumer ExConsumerName = "internal_exclient_consumer"
	ConsumerNameExternalConsumer ExConsumerName = "external_exclient_consumer"
)

type Client interface {
	StreamEvents1(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
	StreamEvents2(ctx context.Context, after string, opts ...reflex.StreamOption) (reflex.StreamClient, error)
}
