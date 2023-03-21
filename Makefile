# Shell to use with Make
SHELL = /bin/sh

grpc:
	@echo Generating gRPC files:
	./generate.sh