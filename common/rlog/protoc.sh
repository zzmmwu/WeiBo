#/bin/bash


protoc --go_out=plugins=grpc:. rlog.proto
