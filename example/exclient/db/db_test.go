package db

import (
	"testing"

	"github.com/luno/reflex/rsql"
)

func TestCursorsTable(t *testing.T) {
	dbc := ConnectForTesting(t)
	rsql.TestCursorsTable(t, dbc, Cursors)
}
