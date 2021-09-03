package main

import (
	"log"

	"github.com/jarviliam/commitlog/internal/server"
)

func main() {
	srv := server.NewHTTPServer(":8080")
	log.Fatal(srv.ListenAndServe())

}
