package rsql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/luno/jettison/jtest"
	"github.com/stretchr/testify/assert"

	"github.com/luno/reflex/rsql"
)

var (
	dbTestURI = flag.String("db_test_uri", getDefaultURI(), "Test database uri")
)

const (
	eventsTable  = "events"
	cursorsTable = "cursors"
)

type EventTableSchema struct {
	Name      string
	Temporary bool

	IDField   string
	TimeField string
	TypeField string

	ForeignIDField string
	IsForeignIDInt bool

	MetadataField string
}

func (s EventTableSchema) CreateTable(t *testing.T, dbc *sql.DB) {
	tt := ""
	if s.Temporary {
		tt = "temporary"
	}
	fidType := "varchar(255)"
	if s.IsForeignIDInt {
		fidType = "bigint"
	}
	metadata := ""
	if s.MetadataField != "" {
		metadata = s.MetadataField + " blob null,"
	}

	schema := fmt.Sprintf(`create %s table %s (
	%s bigint not null auto_increment,
	%s %s not null,
	%s datetime not null,
	%s int not null,

	%s

	primary key (%s)
);`,
		tt, s.Name,
		s.IDField,
		s.ForeignIDField, fidType,
		s.TimeField,
		s.TypeField,
		metadata,
		s.IDField,
	)
	_, err := dbc.Exec(schema)
	jtest.RequireNil(t, err)
}

type CursorTableSchema struct {
	Name      string
	Temporary bool

	CursorField string
	CursorType  string
	IDField     string
	TimeField   string
}

func (s CursorTableSchema) CreateTable(t *testing.T, dbc *sql.DB) {
	tt := ""
	if s.Temporary {
		tt = "temporary"
	}
	schema := fmt.Sprintf(`create %s table %s (
  %s varchar(255) not null,
  %s %s not null,
  %s datetime not null,

  primary key (%s)
);`,
		tt, s.Name,
		s.IDField,
		s.CursorField, s.CursorType,
		s.TimeField,
		s.IDField,
	)
	_, err := dbc.Exec(schema)
	jtest.RequireNil(t, err)
}

func DefaultEventTable() EventTableSchema {
	return EventTableSchema{
		Name:           eventsTable,
		IDField:        "id",
		TimeField:      "timestamp",
		TypeField:      "type",
		ForeignIDField: "foreign_id",
	}
}

func DefaultCursorTable() CursorTableSchema {
	return CursorTableSchema{
		Name:        cursorsTable,
		CursorField: "last_event_id",
		CursorType:  "bigint",
		IDField:     "id",
		TimeField:   "updated_at",
	}
}

// ConnectTestDB returns a db connection with non-temporary DB tables that support multiple connections.
func ConnectTestDB(t *testing.T, ev EventTableSchema, cursor CursorTableSchema) *sql.DB {
	admin, err := sql.Open("mysql", *dbTestURI)
	jtest.RequireNil(t, err)

	dbName := fmt.Sprintf("test_%d", rand.Int())
	_, err = admin.ExecContext(context.Background(), "create database "+dbName)
	jtest.RequireNil(t, err)

	t.Log("created database: " + dbName)

	t.Cleanup(func() {
		_, err := admin.ExecContext(context.Background(), "drop database "+dbName)
		jtest.RequireNil(t, err)
		err = admin.Close()
		jtest.RequireNil(t, err)
	})

	str := *dbTestURI + dbName + "?parseTime=true&collation=utf8mb4_general_ci"
	dbc, err := sql.Open("mysql", str)
	jtest.RequireNil(t, err)

	t.Cleanup(func() {
		err := dbc.Close()
		jtest.RequireNil(t, err)
	})

	ev.CreateTable(t, dbc)
	cursor.CreateTable(t, dbc)

	dbc.SetMaxOpenConns(10)
	_, err = dbc.Exec("set time_zone='+00:00';")
	jtest.RequireNil(t, err)

	return dbc
}

func getDefaultURI() string {
	uri := os.Getenv("DB_TEST_URI")
	if uri != "" {
		return uri
	}

	return "root@unix(" + getSocketFile() + ")/"
}

func getSocketFile() string {
	sock := "/tmp/mysql.sock"
	if _, err := os.Stat(sock); os.IsNotExist(err) {
		// try common linux/Ubuntu socket file location
		return "/var/run/mysqld/mysqld.sock"
	}
	return sock
}

func TestGetNextEventNoRows(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	table := rsql.NewEventsTable(eventsTable)

	el, err := rsql.GetNextEventsForTesting(context.TODO(), t, dbc, table, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, el, 0)
}

func TestStreamRecv(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	table := rsql.NewEventsTable("events")

	ctx, cancel := context.WithCancel(context.Background())

	sc, err := table.ToStream(dbc)(ctx, "")
	assert.NoError(t, err)

	cancelDelay := 100 * time.Millisecond
	go func() {
		time.Sleep(cancelDelay)
		cancel()
	}()

	// this will block until we cancel context
	t0 := time.Now()
	e, err := sc.Recv()
	assert.True(t, time.Now().Sub(t0) > cancelDelay)
	assert.Error(t, err, "context canceled")
	assert.Nil(t, e)
}

func TestGetLatestID(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	id, err := rsql.GetLatestIDForTesting(context.Background(), t, dbc, "events", "id")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)

	n := 10
	for i := 0; i < n; i++ {
		err := insertTestEvent(dbc, rsql.NewEventsTable("events"), "strid", testEventType(0))
		assert.NoError(t, err)
	}

	id, err = rsql.GetLatestIDForTesting(context.Background(), t, dbc, "events", "id")
	assert.NoError(t, err)
	assert.Equal(t, int64(n), id)
}

func TestCursorTableInt(t *testing.T) {
	const id = "test"

	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	ctable := rsql.NewCursorsTable(cursorsTable, rsql.WithCursorAsyncDisabled())

	// Initial get is empty ("")
	c, err := ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "", c)

	// Set to "1"
	err = ctable.SetCursor(context.Background(), dbc, id, "1")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "1", c)

	// Set to "100"
	err = ctable.SetCursor(context.Background(), dbc, id, "100")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "100", c)

	// Set to "99" fails (can't set back)
	err = ctable.SetCursor(context.Background(), dbc, id, "100")
	assert.Error(t, err)

	// Set to "abc" fails (not an int)
	err = ctable.SetCursor(context.Background(), dbc, id, "abc")
	assert.Error(t, err)
}

func TestCursorTableString(t *testing.T) {
	cursors := DefaultCursorTable()
	cursors.CursorType = "varchar(255)"

	const id = "test"
	dbc := ConnectTestDB(t, DefaultEventTable(), cursors)

	ctable := rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorAsyncDisabled(), rsql.WithCursorStrings())

	// Initial get is empty ("")
	c, err := ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "", c)

	// Set to "abc"
	err = ctable.SetCursor(context.Background(), dbc, id, "abc")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "abc", c)

	// Set to "abcd"
	err = ctable.SetCursor(context.Background(), dbc, id, "abcd")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "abcd", c)

	// Set to "aaa" fails (can't set back)
	err = ctable.SetCursor(context.Background(), dbc, id, "aaa")
	assert.Error(t, err)
}

func TestCursorTableStringWithInts(t *testing.T) {
	const id = "test"
	cursors := DefaultCursorTable()
	cursors.CursorType = "varchar(255)"
	dbc := ConnectTestDB(t, DefaultEventTable(), cursors)

	ctable := rsql.NewCursorsTable(cursorsTable,
		rsql.WithCursorAsyncDisabled())

	// Initial get is empty ("")
	c, err := ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "", c)

	// Set to "8"
	err = ctable.SetCursor(context.Background(), dbc, id, "8")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "8", c)

	// Set to "10"
	err = ctable.SetCursor(context.Background(), dbc, id, "10")
	assert.NoError(t, err)

	c, err = ctable.GetCursor(context.Background(), dbc, id)
	assert.NoError(t, err)
	assert.Equal(t, "10", c)

	// Set to "9" fails (can't set back)
	err = ctable.SetCursor(context.Background(), dbc, id, "9")
	assert.Error(t, err)
}

func TestSetCursorBack(t *testing.T) {
	dbc := ConnectTestDB(t, DefaultEventTable(), DefaultCursorTable())

	ctable := rsql.NewCursorsTable(cursorsTable, rsql.WithCursorAsyncDisabled())

	err := ctable.SetCursor(context.Background(), dbc, "someID", "20")
	assert.NoError(t, err)
	// Cannot set cursor to same value as existing cursor.
	err = ctable.SetCursor(context.Background(), dbc, "someID", "20")
	assert.Error(t, err)
	// Cannot set cursor to value smaller than previous value.
	err = ctable.SetCursor(context.Background(), dbc, "someID", "3")
	assert.Error(t, err)
	// Can set cursor to value greater than previous value.
	err = ctable.SetCursor(context.Background(), dbc, "someID", "21")
	assert.NoError(t, err)
	err = ctable.SetCursor(context.Background(), dbc, "someID", "110")
	assert.NoError(t, err)
}
