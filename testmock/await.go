package testmock

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/luno/jettison/jtest"

	"github.com/luno/reflex"
)

// AwaitConsumer waits for a maximum of 15 seconds for a consumer, with the name provided as "consumerName", to consume
// a specific event which is done by specifying the event ID. The event ID needs to be less than or equal to the
// consumer's cursor store value in order for AwaitConsumer to return and unblock.
func AwaitConsumer(t *testing.T, cs reflex.CursorStore, consumerName string, eventID int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)

	for ctx.Err() == nil {
		val, err := cs.GetCursor(ctx, consumerName)
		jtest.RequireNil(t, err)

		eID := int64(0)
		if val != "" {
			eID, err = strconv.ParseInt(val, 10, 64)
			jtest.RequireNil(t, err)
		}

		if eID >= eventID {
			break
		}

		time.Sleep(5 * time.Millisecond)
	}
}
