// This file is used to compile the apikeyspb package's proto files.
// Usage: go generate <path to this directory>

//go:generate protoc -I=${GOPATH}/src -I=. --go_out=plugins=grpc:. ./reflex.proto

package reflexpb
