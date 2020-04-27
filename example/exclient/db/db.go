package db

import (
	"database/sql"
	"flag"
	"os"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/luno/reflex/example/internal/db"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/require"
)

var dbURI = flag.String("db_example_uri", getDefaultURI(), "URI of reflex example client DB")
var Cursors = rsql.NewCursorsTable("client_cursors")

func Connect() (*sql.DB, error) {
	return db.Connect(*dbURI)
}

func ConnectForTesting(t *testing.T) *sql.DB {
	dbc, err := db.ConnectForTesting(t, *dbURI+"parseTime=true", "schema.sql")
	require.NoError(t, err, "uri: %s\nerr: %v", *dbURI, err)
	return dbc
}

func getDefaultURI() string {
	uri := os.Getenv("DB_EXAMPLE_CLIENT_URI")
	if uri != "" {
		return uri
	}

	return "root@tcp(localhost:3306)/test?"
}
