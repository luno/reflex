// Package db implements common DB functions for the reflex example servers.
package db

import (
	"database/sql"
	"io/ioutil"
	"strings"
	"testing"

	// Import for the driver
	_ "github.com/go-sql-driver/mysql"
)

// Connect to the db
func Connect(uri string) (*sql.DB, error) {
	dbc, err := sql.Open("mysql", uri)
	if err != nil {
		return nil, err
	}

	return dbc, dbc.Ping()
}

// ConnectForTesting to the db and create a temporary schema
func ConnectForTesting(t *testing.T, uri, schemaPath string) (*sql.DB, error) {
	dbc, err := Connect(uri)
	if err != nil {
		return nil, err
	}

	schema, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		return nil, err
	}

	// We want to work with a clean DB for tests - creating temporary tables
	// ensures that existing data isn't affected.
	query := strings.Replace(string(schema), "create table",
		"create temporary table", -1)
	for _, cmd := range strings.Split(query, ";") {
		cmd := strings.TrimSpace(cmd)
		if cmd == "" {
			continue
		}

		_, err := dbc.Exec(cmd)
		if err != nil {
			return nil, err
		}
	}

	return dbc, nil
}
