# pyensign
Welcome to pyensign!

This repository contains the Ensign driver, SDK, and helpers for Python. For the main ensign repo, go [here](https://github.com/rotationalio/ensign). We also have SDKs for [Javascript](https://github.com/rotationalio/ensignjs) and [Go](https://github.com/rotationalio/goensign).

## Dependencies

The project dependencies can be installed with pip.

```
$ pip install -r requirements.txt
```

## Building the protocol buffers

This repo relies on [protocol buffers](https://protobuf.dev/) for code generation. If you need to rebuild the protocol buffers, clone the ensign repo to the parent directory.

```bash
$ git clone git@github.com:rotationalio/ensign.git ../ensign
```

Then run make to build the protocol buffers from the .proto defintions.

```
$ make grpc
```