package db

import (
	"database/sql"
	"flag"
	"os"
	"testing"

	// Imported for driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex/example/internal/db"
	"github.com/luno/reflex/rsql"
)

var (
	dbURI = flag.String("db_example_uri", getDefaultURI(), "URI of reflex example server DB")

	// Events1 is an events table
	Events1 = rsql.NewEventsTable("server_events1")
	// Events2 is an events table
	Events2 = rsql.NewEventsTable("server_events2")
	// Cursors is a cursor table
	Cursors = rsql.NewCursorsTable("server_cursors")
)

// Connect connects to the db
func Connect() (*sql.DB, error) {
	return db.Connect(*dbURI)
}

// ConnectForTesting connects to the db and resets to the schema in schema.sql
func ConnectForTesting(t *testing.T) *sql.DB {
	dbc, err := db.ConnectForTesting(t, *dbURI+"?parseTime=true", "schema.sql")
	require.NoError(t, err, "uri: %s\nerr: %v", *dbURI, err)
	return dbc
}

func getDefaultURI() string {
	uri := os.Getenv("DB_EXAMPLE_SERVER_URI")
	if uri != "" {
		return uri
	}

	return "root@tcp(localhost:3306)/test"
}
