# Shell to use with Make
SHELL = /bin/sh

clean:
	rm -rf .pytest_cache
	rm -rf build
	rm -rf dist
	rm -rf *.egg-info
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

grpc:
	@echo Generating gRPC files:
	./generate.sh

build:
	@echo Building wheel and source distribution:
	python3 setup.py sdist bdist_wheel

publish:
	@echo Publishing to PyPI:
	python3 -m twine upload dist/*