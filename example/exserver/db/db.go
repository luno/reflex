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
	"URI of reflex example server DB")

var Events1 = rsql.NewEventsTable("server_events1")
var Events2 = rsql.NewEventsTable("server_events2")
var Cursors = rsql.NewCursorsTable("server_cursors")

func Connect() (*sql.DB, error) {
	return db.Connect(*dbURI)
}

func ConnectForTesting(t *testing.T) (*sql.DB, error) {
	return db.ConnectForTesting(t, *dbURI, "schema.sql")
}
