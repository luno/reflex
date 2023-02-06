package db

import (
	"database/sql"
	"flag"
	"os"
	"testing"

	// Import for mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/luno/reflex/example/internal/db"
	"github.com/luno/reflex/rsql"
)

var dbURI = flag.String("db_example_uri", getDefaultURI(), "URI of reflex example client DB")

// Cursors is an example cursors table
var Cursors = rsql.NewCursorsTable("client_cursors")

// Connect to the mysql db
func Connect() (*sql.DB, error) {
	return db.Connect(*dbURI)
}

// ConnectForTesting connects to the database and resets to the schema in schema.sql
func ConnectForTesting(t *testing.T) *sql.DB {
	dbc, err := db.ConnectForTesting(t, *dbURI+"?parseTime=true", "schema.sql")
	require.NoError(t, err, "uri: %s\nerr: %v", *dbURI, err)
	return dbc
}

func getDefaultURI() string {
	uri := os.Getenv("DB_EXAMPLE_CLIENT_URI")
	if uri != "" {
		return uri
	}

	return "root@tcp(localhost:3306)/test"
}
