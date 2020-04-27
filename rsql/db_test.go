package rsql_test

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/luno/reflex/rsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	db_test_uri = flag.String("db_test_uri", getDefaultURI(), "Test database uri")

	eventsTimeField          = "timestamp"
	eventsTypeField          = "type"
	eventsForeignIDField     = "foreign_id"
	eventsMetadataField      = ""
	cursorsCursorField       = "last_event_id"
	cursorsCursorType        = "bigint"
	cursorsIDField           = "id"
	cursorsTimeField         = "updated_at"
	isEventForeignIDInt      = false
	eventForeignIDFieldTypes = map[bool]string{
		false: "varchar(255)",
		true:  "bigint",
	}
)

const cursorsTable = "cursors"

const eventsSchema = `
create %s table %s (
  id bigint not null auto_increment,
  %s %s not null,
  %s datetime not null,
  %s int not null,

  %s

  primary key (id)
);`

const eventMetadata = "%s blob null,"

const cursorsSchema = `
create %s table %s (
  %s varchar(255) not null,
  %s %s not null,
  %s datetime not null,

  primary key (%s)
);
`

const drop = "drop table if exists %s;"

// ConnectAndCloseTestDB returns a db connection with actual DB tables (not temporary)
// that support multiple connection, but that must be closed.
func ConnectAndCloseTestDB(t *testing.T, eventsTable, cursorTable string) (*sql.DB, func()) {
	dbc, err := connect(10)
	require.NoError(t, err)

	maybeDrop := func() {
		if eventsTable != "" {
			_, err := dbc.Exec(fmt.Sprintf(drop, eventsTable))
			require.NoError(t, err)
		}
		if cursorTable != "" {
			_, err := dbc.Exec(fmt.Sprintf(drop, cursorTable))
			require.NoError(t, err)
		}
	}

	maybeDrop()
	createEventsTable(t, dbc, eventsTable, false)
	createCursorsTable(t, dbc, cursorTable, false)

	return dbc, func() {
		maybeDrop()
		require.NoError(t, dbc.Close())
	}
}

func ConnectTestDB(t *testing.T, eventsTable, cursorTable string) *sql.DB {
	dbc, err := connect(1)
	if err != nil {
		log.Fatalf("error connecting to db: %v", err)
	}

	createEventsTable(t, dbc, eventsTable, true)
	createCursorsTable(t, dbc, cursorTable, true)
	return dbc
}

func createEventsTable(t *testing.T, dbc *sql.DB, name string, temp bool) {
	if name == "" {
		return
	}

	tt := "temporary"
	if !temp {
		tt = ""
	}

	metaSchema := ""
	if eventsMetadataField != "" {
		metaSchema = fmt.Sprintf(eventMetadata, eventsMetadataField)
	}

	q := fmt.Sprintf(eventsSchema, tt, name, eventsForeignIDField,
		eventForeignIDFieldTypes[isEventForeignIDInt], eventsTimeField,
		eventsTypeField, metaSchema)

	_, err := dbc.Exec(q)
	require.NoError(t, err)
}

func createCursorsTable(t *testing.T, dbc *sql.DB, name string, temp bool) {
	if name == "" {
		return
	}

	tt := "temporary"
	if !temp {
		tt = ""
	}

	q := fmt.Sprintf(cursorsSchema, tt, name, cursorsIDField,
		cursorsCursorField, cursorsCursorType, cursorsTimeField, cursorsIDField)
	_, err := dbc.Exec(q)
	require.NoError(t, err)
}

func connect(n int) (*sql.DB, error) {
	str := *db_test_uri + "parseTime=true&collation=utf8mb4_general_ci"
	dbc, err := sql.Open("mysql", str)
	if err != nil {
		return nil, err
	}

	dbc.SetMaxOpenConns(n)

	if _, err := dbc.Exec("set time_zone='+00:00';"); err != nil {
		log.Fatalf("error setting db time_zone: %v", err)
	}

	return dbc, nil
}

func getDefaultURI() string {
	uri := os.Getenv("DB_TEST_URI")
	if uri != "" {
		return uri
	}

	return "root@unix(" + getSocketFile() + ")/test?"
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
	dbc := ConnectTestDB(t, eventsTable, "")
	defer dbc.Close()

	table := rsql.NewEventsTable(eventsTable)

	el, err := rsql.GetNextEventsForTesting(t, context.TODO(), dbc, table, 0, 0)
	assert.NoError(t, err)
	assert.Len(t, el, 0)
}

func TestStreamRecv(t *testing.T) {
	dbc := ConnectTestDB(t, "events", "")
	defer dbc.Close()

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
	dbc := ConnectTestDB(t, "events", "")
	defer dbc.Close()

	id, err := rsql.GetLatestIDForTesting(t, context.Background(), dbc, "events")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), id)

	n := 10
	for i := 0; i < n; i++ {
		err := insertTestEvent(dbc, rsql.NewEventsTable("events"), "strid", testEventType(0))
		assert.NoError(t, err)
	}

	id, err = rsql.GetLatestIDForTesting(t, context.Background(), dbc, "events")
	assert.NoError(t, err)
	assert.Equal(t, int64(n), id)
}

func TestCursorTableInt(t *testing.T) {
	const id = "test"
	const name = "cursors"

	dbc := ConnectTestDB(t, "", name)
	defer dbc.Close()

	ctable := rsql.NewCursorsTable(name, rsql.WithCursorAsyncDisabled())

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
	cache := cursorsCursorType
	defer func() {
		cursorsCursorType = cache
	}()
	cursorsCursorType = "varchar(255)"

	const id = "test"
	const name = "cursors"
	dbc := ConnectTestDB(t, "", name)
	defer dbc.Close()

	ctable := rsql.NewCursorsTable(name,
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
	cache := cursorsCursorType
	defer func() {
		cursorsCursorType = cache
	}()
	cursorsCursorType = "varchar(255)"

	const id = "test"
	const name = "cursors"
	dbc := ConnectTestDB(t, "", name)
	defer dbc.Close()

	ctable := rsql.NewCursorsTable(name,
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
	const name = "cursors"

	dbc := ConnectTestDB(t, "", name)
	defer dbc.Close()

	ctable := rsql.NewCursorsTable(name, rsql.WithCursorAsyncDisabled())

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
