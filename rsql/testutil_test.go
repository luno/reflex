package rsql_test

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestTestEventsTable(t *testing.T) {
	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable))
}

func TestCustomIDField(t *testing.T) {
	cache := eventsIDField
	defer func() {
		eventsIDField = cache
	}()

	eventsIDField = "custom_id"

	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventIDField(eventsIDField)))
}

func TestCustomTimeField(t *testing.T) {
	cache := eventsTimeField
	defer func() {
		eventsTimeField = cache
	}()

	eventsTimeField = "custom_timestamp"

	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventTimeField(eventsTimeField)))
}

func TestCustomTypeField(t *testing.T) {
	cache := eventsTypeField
	defer func() {
		eventsTypeField = cache
	}()

	eventsTypeField = "custom_type"

	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventTypeField(eventsTypeField)))
}

func TestCustomForeignIDField(t *testing.T) {
	cache := eventsForeignIDField
	defer func() {
		eventsForeignIDField = cache
	}()

	eventsForeignIDField = "custom_foreign_id"

	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventForeignIDField(eventsForeignIDField)))
}

func TestForeignIDIntField(t *testing.T) {
	cache := isEventForeignIDInt
	defer func() {
		isEventForeignIDInt = cache
	}()

	isEventForeignIDInt = true

	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	rsql.TestEventsTableWithID(t, dbc, rsql.NewEventsTable(eventsTable), "9999")
}

func TestTestcursorsTable(t *testing.T) {
	dbc := ConnectTestDB(t, "", cursorsTable)
	defer dbc.Close()

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable))
}

func TestCustomCursorField(t *testing.T) {
	cache := cursorsCursorField
	defer func() {
		cursorsCursorField = cache
	}()

	cursorsCursorField = "`cursor`"

	dbc := ConnectTestDB(t, "", cursorsTable)
	defer dbc.Close()

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorCursorField(cursorsCursorField)))
}

func TestCustomCursorIDField(t *testing.T) {
	cache := cursorsIDField
	defer func() {
		cursorsIDField = cache
	}()

	cursorsIDField = "name"

	dbc := ConnectTestDB(t, "", cursorsTable)
	defer dbc.Close()

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorIDField(cursorsIDField)))
}

func TestCustomCursorTimeField(t *testing.T) {
	cache := cursorsTimeField
	defer func() {
		cursorsTimeField = cache
	}()

	cursorsTimeField = "timestamp"

	dbc := ConnectTestDB(t, "", cursorsTable)
	defer dbc.Close()

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorTimeField(cursorsTimeField)))
}
