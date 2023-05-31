# pyensign
Welcome to pyensign!

This repository contains the Ensign driver, SDK, and helpers for Python. For the main ensign repo, go [here](https://github.com/rotationalio/ensign). We also have SDKs for [Javascript](https://github.com/rotationalio/ensignjs) and [Go](https://github.com/rotationalio/goensign).

## Installation

PyEnsign is compatible with Python >= 3.7 (Note: we can't guarantee PyEnsign's compatibility with earlier versions of Python due to PyEnsign's dependence on the [`grpcio` package](https://pypi.org/project/grpcio/)). The simplest way to install PyEnsign and its dependencies is from PyPI with pip, Python's preferred package installer.

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

## Getting to know the PyEnsign API

The sample code below describes some of the core PyEnsign API, but if you're looking for a minimal end-to-example, [check this out first](https://github.com/rotationalio/ensign-examples/tree/main/python/minimal).

### Publishing

Use `Ensign.publish()` to publish events to a topic. All events must contain some data and a mimetype. If the topic doesn't exist in your project, it will be automatically created.

```python
from pyensign.events import Event

# Publishing a single event
event = Event(b'{"temp": 72, "units": "fahrenheit"}', "APPLICATION_JSON")
await client.publish("weather", event)

# Publishing multiple events
events = [
    Event(b'{"temp": 72, "units": "fahrenheit"}', "APPLICATION_JSON"),
    Event(b'{"temp": 76, "units": "fahrenheit"}', "APPLICATION_JSON")
]
await client.publish("weather", events)
```

### Subscribing

Use `Ensign.subscribe()` to subscribe to one or more topic IDs. Topic IDs are assigned by Ensign, so a common pattern is to retrieve the IDs first.

```python
weather_id = await client.topic_id("weather")
forecast_id = await client.topic_id("forecast")
async for event in client.subscribe(weather_id, forecast_id)
    print("Received event: {}".format(event))
```

### Design patterns

Most event-driven applications require some form of concurrency. Therefore, the `Ensign` class is designed to be used asynchronously by defining coroutines. You can use Python's builtin `asyncio` package to schedule and run coroutines from the main thread.

```python
import asyncio
from pyensign.ensign import Ensign

async def subscriber(topic):
    ...

    async for event in client.subscribe(topic):
        # Do something with the event

def main():
    asyncio.get_event_loop().run_until_complete(subscribe(topic))
```

How do you know if an event was actually published? The `publish` coroutine allows callbacks to be specified when the client receives acks and nacks from the server. The first argument in the callback is the `Ack` or `Nack`. An `Ack` contains the timestamp when the event was committed. A `Nack` is returned if the event couldn't be committed and contains the ID of the event along with an error describing what went wrong.


```python
async def handle_ack(self, ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(f"Event committed at {ts}")

async def handle_nack(self, nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

await client.publish("weather", event, ack_callback=handle_ack, nack_callback=handle_nack)
```

## Contributing to PyEnsign

Wow, you want to contribute to PyEnsign? 😍 We would absolutely love that!

PyEnsign is an open source project that is supported by a community who will gratefully and humbly accept any contributions you might make to the project. Large or small, any contribution makes a big difference; and if you've never contributed to an open source project before, we hope you will start with PyEnsign!

Please check out our Contributor's Guide in `CONTRIBUTING.md` to get a quick orientation first.

We can't wait to hear from you!