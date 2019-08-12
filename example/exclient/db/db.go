package db

import (
	"database/sql"
	"flag"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/luno/reflex/example/internal/db"
	"github.com/luno/reflex/rsql"
)

var dbURI = flag.String("db_uri",
	"root:@tcp(localhost:3306)/test?parseTime=true",
	"URI of reflex example client DB")

var Cursors = rsql.NewCursorsTable("client_cursors")

func Connect() (*sql.DB, error) {
	return db.Connect(*dbURI)
}

func ConnectForTesting(t *testing.T) (*sql.DB, error) {
	return db.ConnectForTesting(t, *dbURI, "schema.sql")
}
