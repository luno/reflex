package main

import (
	"context"
	"database/sql"
	"flag"
	"log"
	"time"

	"github.com/luno/fate"
	"github.com/luno/reflex"
	"github.com/luno/reflex/example/exclient/db"
	"github.com/luno/reflex/example/exserver"
	exserver_client "github.com/luno/reflex/example/exserver/client"
)

func main() {
	flag.Parse()

	ec, err := exserver_client.New()
	if err != nil {
		log.Fatalf("exclient: error creating exserver client: %v", err)
	}

	dbc, err := db.Connect()
	if err != nil {
		log.Fatalf("exclient: error connecting to db: %v", err)
	}

	go GRPCConsumeEventsForever(ec)
	ConsumeGRPCStreamForever(dbc, ec)
}

// GRPCConsumeEventsForever consumes events from a remote gRPC service and
// stores the consumer cursor on the remote grpc server.
func GRPCConsumeEventsForever(ec exserver.Client) {
	f := func(ctx context.Context, fate fate.Fate, event *exserver.ExEvent) error {

		typ := exserver.ExEventType(event.Type.ReflexType())
		log.Printf("exclient: consuming exserver event %s of type %v", event.ID, typ)

		return fate.Tempt()
	}

	consumer := reflex.NewConsumer(exserver.ConsumerNameExternalConsumer, f)

	for {
		err := ec.ConsumeEvents1(context.Background(), consumer)
		if reflex.IsStoppedErr(err) {
			// On stopped error, just reconnect.
			continue
		}

		log.Printf("exclient: external_event_consumer error: %v", err)
		time.Sleep(time.Second * 5)
	}
}

// ConsumeGRPCStreamForever consumes events from a remote gRPC service and
// stores the consumer cursor locally.
func ConsumeGRPCStreamForever(dbc *sql.DB, ec exserver.Client) {
	f := func(ctx context.Context, fate fate.Fate, event *exserver.ExEvent) error {

		typ := exserver.ExEventType(event.Type.ReflexType())
		log.Printf("ops: consuming event %s of type %v", event.ID, typ)

		return fate.Tempt()
	}

	consumer := reflex.NewConsumer(exserver.ConsumerNameInternalConsumer, f)
	consumable := reflex.NewConsumable(ec.StreamEvents1, db.Cursors.ToStore(dbc))

	for {
		err := consumable.Consume(context.Background(), consumer)
		if reflex.IsStoppedErr(err) {
			// Just reconnect on server stop
			continue
		}
		log.Printf("ops: internal_exserver_loop error: %v", err)
		time.Sleep(time.Second * 5)
	}

}
