#/bin/bash


protoc --go_out=plugins=grpc:. common.proto
