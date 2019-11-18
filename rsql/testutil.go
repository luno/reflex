package rsql

import (
	"context"
	"database/sql"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testConsumerID      = "test_consumer"
	testForeignIDInt    = "12341234"
	testForeignIDString = "abcd-1234"
	numberOfEvents      = 3
)

func TestCursorsTable(t *testing.T, dbc *sql.DB, table CursorsTable) {
	createdLastEventID := ""
	updatedLastEventID := "10"

	lastEventID, err := table.GetCursor(context.Background(), dbc, testConsumerID)
	assert.NoError(t, err)
	assert.Equal(t, createdLastEventID, lastEventID)

	assert.NoError(t, table.SetCursor(context.Background(), dbc, testConsumerID, updatedLastEventID))
	assert.NoError(t, table.Flush(context.Background()))

	lastEventID, err = table.GetCursor(context.Background(), dbc, testConsumerID)
	assert.NoError(t, err)
	assert.Equal(t, updatedLastEventID, lastEventID)
}

// TestEventsTable provides a helper function to test event tables
// with int foreign id columns.
func TestEventsTableInt(t *testing.T, dbc *sql.DB, table *EventsTableInt) {
	TestEventsTableWithID(t, dbc, table.EventsTable, testForeignIDInt)
}

func TestEventsTable(t *testing.T, dbc *sql.DB, table *EventsTable) {
	TestEventsTableWithID(t, dbc, table, testForeignIDString)
}

func TestEventsTableWithID(t *testing.T, dbc *sql.DB, table *EventsTable, foreignID string) {
	tx, err := dbc.Begin()
	require.NoError(t, err)
	for i := 1; i <= numberOfEvents; i++ {
		_, err = table.Insert(context.Background(), tx, foreignID, eventType(i))
		require.NoError(t, err)
	}
	assert.NoError(t, tx.Commit())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := table.ToStream(dbc)(ctx, "")
	require.NoError(t, err)

	for i := 1; i <= numberOfEvents; i++ {
		event, err := client.Recv()
		require.NoError(t, err)
		require.Equal(t, i, event.Type.ReflexType())
		require.Equal(t, foreignID, event.ForeignID)
		foreignIDInt, _ := strconv.ParseInt(foreignID, 10, 64)
		require.Equal(t, foreignIDInt, event.ForeignIDInt())
	}
}
