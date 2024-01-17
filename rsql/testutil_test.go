package rsql_test

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestTestEventsTable(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable))
}

func TestCustomIDField(t *testing.T) {
	ev := DefaultEventTable()
	ev.IDField = "custom_id"
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventIDField(ev.IDField)))
}

func TestCustomTimeField(t *testing.T) {
	ev := DefaultEventTable()
	ev.TimeField = "custom_timestamp"
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventTimeField(ev.TimeField)))
}

func TestCustomTypeField(t *testing.T) {
	ev := DefaultEventTable()
	ev.TypeField = "custom_type"
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventTypeField(ev.TypeField)))
}

func TestCustomForeignIDField(t *testing.T) {
	ev := DefaultEventTable()
	ev.ForeignIDField = "custom_foreign_id"
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTable(t, dbc, rsql.NewEventsTable(eventsTable,
		rsql.WithEventForeignIDField(ev.ForeignIDField)))
}

func TestForeignIDIntField(t *testing.T) {
	ev := DefaultEventTable()
	ev.IsForeignIDInt = true
	dbc := ConnectTestDB(t, ev, DefaultCursorTable(), DefaultErrorTable())

	rsql.TestEventsTableWithID(t, dbc, rsql.NewEventsTable(eventsTable), "9999")
}

func TestTestCursorsTable(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable(), DefaultErrorTable())
	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable))
}

func TestCustomCursorField(t *testing.T) {
	crs := DefaultCursorTable()
	crs.CursorField = "`cursor`"
	dbc := ConnectTestDB(t, DefaultEventTable(), crs, DefaultErrorTable())

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorCursorField(crs.CursorField)))
}

func TestCustomCursorIDField(t *testing.T) {
	crs := DefaultCursorTable()
	crs.IDField = "name"
	dbc := ConnectTestDB(t, DefaultEventTable(), crs, DefaultErrorTable())

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorIDField(crs.IDField)))
}

func TestCustomCursorTimeField(t *testing.T) {
	crs := DefaultCursorTable()
	crs.TimeField = "timestamp"
	dbc := ConnectTestDB(t, DefaultEventTable(), crs, DefaultErrorTable())

	rsql.TestCursorsTable(t, dbc, rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorTimeField(crs.TimeField)))
}
