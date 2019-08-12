package main

import (
	"flag"
	"log"

	"github.com/luno/reflex/example/exserver/db"
	"github.com/luno/reflex/example/exserver/ops"
	"github.com/luno/reflex/example/exserver/server"
)

var listenAddr = flag.String("listen_addr", ":1234",
	"Address to listen for gRPC requests on.")

func main() {
	flag.Parse()

	dbc, err := db.Connect()
	if err != nil {
		log.Fatalf("Error connecting to db: %v", err)
	}

	go ops.ConsumeLocalStreamForever(dbc)

	server := server.New(dbc)
	server.ServeForever(*listenAddr)
}
