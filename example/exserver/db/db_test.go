package db

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestEvents1Table(t *testing.T) {
	dbc := ConnectForTesting(t)
	t.Cleanup(func() { _ = dbc.Close() })

	rsql.TestEventsTable(t, dbc, Events1)
}

func TestEvents2Table(t *testing.T) {
	dbc := ConnectForTesting(t)
	t.Cleanup(func() { _ = dbc.Close() })

	rsql.TestEventsTable(t, dbc, Events2)
}

func TestCursorsTable(t *testing.T) {
	dbc := ConnectForTesting(t)
	t.Cleanup(func() { _ = dbc.Close() })

	rsql.TestCursorsTable(t, dbc, Cursors)
}
