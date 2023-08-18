package ops

import (
	"context"
	"database/sql"
	"log"

	"github.com/luno/fate"

	"github.com/luno/reflex"
	"github.com/luno/reflex/example/exserver"
	"github.com/luno/reflex/example/exserver/db"
)

// ConsumeLocalStreamForever connects to the local server and prints out each event
func ConsumeLocalStreamForever(dbc *sql.DB) {
	f := func(ctx context.Context, fate fate.Fate, event *exserver.ExEvent) error {
		typ := exserver.ExEventType(event.Type.ReflexType())
		log.Printf("ops: consuming event %s of type %v", event.ID, typ)

		return fate.Tempt()
	}

	consumer := reflex.NewConsumer(exserver.ConsumerNameInternalLoop, f)
	consumable := reflex.NewConsumable(db.Events1.ToStream(dbc), db.Cursors.ToStore(dbc))

	for {
		err := consumable.Consume(context.Background(), consumer)
		if reflex.IsStoppedErr(err) {
			// On stopped error, this app is shutting down.
			return
		}
		log.Printf("ops: internal_exserver_loop error: %v", err)
	}
}
