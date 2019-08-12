package db

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestCursorsTable(t *testing.T) {
	dbc, err := ConnectForTesting(t)
	if err != nil {
		t.Error(err)
	}

	rsql.TestCursorsTable(t, dbc, Cursors)
}
