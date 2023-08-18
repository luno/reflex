package rsql_test

import (
	"context"
	"database/sql"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/luno/reflex"
	"github.com/luno/reflex/rsql"
)

type testEventType int

func (t testEventType) ReflexType() int {
	return int(t)
}

func insertTestEvent(dbc *sql.DB, table *rsql.EventsTable, foreignID string, typ reflex.EventType) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	notify, err := table.Insert(context.Background(), tx, foreignID, typ)
	if err != nil {
		return err
	}
	defer notify()

	return tx.Commit()
}

func insertTestEventMeta(dbc *sql.DB, table *rsql.EventsTable, foreignID string,
	typ reflex.EventType, metadata []byte,
) error {
	tx, err := dbc.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	notify, err := table.InsertWithMetadata(context.Background(), tx, foreignID,
		typ, metadata)
	if err != nil {
		return err
	}
	defer notify()

	return tx.Commit()
}

func i2s(i int) string {
	return strconv.Itoa(i)
}

func assertEqualI2S(t *testing.T, expected int, actual string, msgAndArgs ...interface{}) {
	assert.Equal(t, i2s(expected), actual, msgAndArgs)
}

func waitFor(t *testing.T, d time.Duration, f func() bool) {
	t0 := time.Now()

	for {
		if f() {
			return
		}
		if time.Now().Sub(t0) > d {
			assert.Fail(t, "Timeout waiting for f")
			return
		}
		time.Sleep(time.Millisecond) // don't spin
	}
}
