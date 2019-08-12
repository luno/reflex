package db

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestEvents1Table(t *testing.T) {
	dbc, err := ConnectForTesting(t)
	if err != nil {
		t.Error(err)
	}

	rsql.TestEventsTable(t, dbc, Events1)
}

func TestEvents2Table(t *testing.T) {
	dbc, err := ConnectForTesting(t)
	if err != nil {
		t.Error(err)
	}

	rsql.TestEventsTable(t, dbc, Events2)
}

func TestCursorsTable(t *testing.T) {
	dbc, err := ConnectForTesting(t)
	if err != nil {
		t.Error(err)
	}

	rsql.TestCursorsTable(t, dbc, Cursors)
}
