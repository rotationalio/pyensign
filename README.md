# pyensign
Welcome to pyensign!

This repository contains the Ensign driver, SDK, and helpers for Python. For the main ensign repo, go [here](https://github.com/rotationalio/ensign). We also have SDKs for [Javascript](https://github.com/rotationalio/ensignjs) and [Go](https://github.com/rotationalio/goensign).

## Installation

```
pip install pyensign
```

## Configuration

The `Ensign` client provides access to the unified API for managing topics and publishing/subscribing to topics. Creating a client requires a client ID and client secret (your API key).

```python
from pyensign.ensign import Ensign

client = Ensign(client_id=<your client ID>, client_secret=<your client secret>)
```

If not provided the client ID and client secret will be obtained from the `ENSIGN_CLIENT_ID` and `ENSIGN_CLIENT_SECRET` environment variables.

## Publishing

Use `Ensign.publish()` to publish events to a topic. All events must contain some data and a mimetype. If the topic doesn't exist in your project, it will be automatically created.

```python
from pyensign.events import Event

# Publishing a single event
event = Event(b'{"temp": 72, "units": "fahrenheit"}', "APPLICATION_JSON")
await client.publish("weather", event)

# Publishing mulitple events
events = [
    Event(b'{"temp": 72, "units": "fahrenheit"}', "APPLICATION_JSON")
    Event(b'{"temp": 76, "units": "fahrenheit"}', "APPLICATION_JSON")
]
await client.publish("weather", events)
```

## Subscribing

Use `Ensign.subscribe()` to subscribe to one or more topics.

```python
async for event in client.subscribe("weather", "forecast")
    print("Received event: {}".format(event))
```

## Design patterns

Most event-driven applications require some form of concurrency. Therefore, the `Ensign` class is designed to be used asynchronously by defining coroutines. You can use Python's builtin `asyncio` package to schedule and run coroutines from the main thread.

```python
import asyncio
from pyensign.ensign import Ensign

async def subscriber(topic):
    ...

    async for event in client.subscribe(topic):
        # Do something with the event

def main():
    asyncio.run(subscribe(topic))
```

## Building the protocol buffers

This repo relies on [protocol buffers](https://protobuf.dev/) for code generation. If you need to rebuild the protocol buffers, clone the ensign repo to the parent directory.

```bash
$ git clone git@github.com:rotationalio/ensign.git ../ensign
```

Then run make to build the protocol buffers from the .proto definitions.

```
$ make grpc
```

## Running the tests

If you wish to run the tests, you must first install the test dependencies.

```
$ pip install -r tests/requirements.txt
```

Then run the tests using pytest.

```
$ python -m pytest
```