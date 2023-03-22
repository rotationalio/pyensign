# Shell to use with Make
SHELL = /bin/sh

clean:
	rm -rf .pytest_cache
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

grpc:
	@echo Generating gRPC files:
	./generate.sh