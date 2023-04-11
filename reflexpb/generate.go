// This file is used to compile the reflexpb package's proto files.
// Usage: go generate <path to this directory>

//go:generate protoc --go_out=. --go-grpc_out=. --go_opt=paths=source_relative --go-grpc_opt=paths=source_relative ./reflex.proto

package reflexpb
