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

Use `Ensign.publish()` to publish events to a topic. All events must contain some data (the event payload) in binary format and a mimetype. The mimetype helps subscribers consuming the event determine how to decode the payload.

```python
from pyensign.events import Event

# Publishing a single event
event = Event(b'{"temp": 72, "units": "fahrenheit"}', "application/json")
await client.publish("weather", event)

# Publishing multiple events
events = [
    Event(b'{"temp": 72, "units": "fahrenheit"}', "application/json"),
    Event(b'{"temp": 76, "units": "fahrenheit"}', "application/json")
]
await client.publish("weather", events)
```

This will raise an exception if the topic doesn't exist. If you aren't sure that a topic exists, you can use `Ensign.ensure_topic_exists()` to create the topic if it doesn't exist.

```python
await client.ensure_topic_exists("weather")
```

How do you know if an event was actually published? `Ensign.publish` allows callbacks to be specified when the client receives acks and nacks from the server. The first argument in the callback is the `Ack` or `Nack`. An `Ack` contains the timestamp when the event was committed. A `Nack` is returned if the event couldn't be committed and contains the ID of the event along with an error describing what went wrong.


```python
async def handle_ack(self, ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(f"Event committed at {ts}")

async def handle_nack(self, nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

await client.publish("weather", event, on_ack=handle_ack, on_nack=handle_nack)
```

### Subscribing

Use `Ensign.subscribe()` to subscribe to one or more topics.

```python
async for event in client.subscribe("weather", "forecast"):
    print(event)
    await event.ack()
```

```
Event:
	id: b'\x01\x89\xd2\x1a?,A\x03\xf2\x04\xa6yd\xdf\x0b<'
	data: b'{"temp": "72", "units": "fahrenheit"}'
	mimetype: application/json
	schema: WeatherUpdate v1.0.0
	state: EventState.SUBSCRIBED
	created: 2023-08-07 17:24:41
	committed: 2023-08-07 17:24:42.930920
```

The `Event` object contains coroutines for acking and nacking an event back to the Ensign service. Subscribers should normally invoke `Event.ack()` once the event has been successfully consumed, or `Event.nack()` if the event needs to be redelivered.

## Decorators

PyEnsign has decorators to quickly add pub/sub to your async functions. For example, if you have a common function that you use to retrieve weather data, you could mark it with `@publish` to automatically publish the returned object to Ensign.

```python
from pyensign.ensign import authenticate, publish

@authenticate()
@publish("weather")
async def current_weather():
    return {
        "temp": 72,
        "units": "fahrenheit"
    }
```

is equivalent to

```python
from pyensign.ensign import Ensign

client = Ensign()
event = Event(b'{"temp": 72, "units": "fahrenheit"}', "application/json")
await client.publish("weather", event)
```

You can also specify an alternative mimetype for the byte encoding. For example, pickle is a common serialization format that's an alternative to JSON.

```python
@publish("weather", mimetype="application/python-pickle")
```

`@authenticate` should be specified at least once, usually on your `main` function or at the entry point of your application. By default it uses credentials from your environment, but you can also specify them directly or load them from a JSON file.

```python
@authenticate(client_id="my-client-id", client_secret="my-client_secret")

@authenticate(cred_path="my-project-credentials.json")
```

### Design patterns

Most event-driven applications require some form of concurrency. Therefore, the `Ensign` class is designed to be used asynchronously by defining coroutines. You can use Python's builtin `asyncio` package to schedule and run coroutines from the main thread.

```python
import asyncio
from pyensign.ensign import Ensign

async def subscriber(topic):
    ...

    async for event in client.subscribe(topic):
        # Handle the event

def main():
    asyncio.run(subscriber(topic))
```

If you aren't comfortable with `asyncio` or need a more object-oriented interface, you can use the `Publisher` and `Subscriber` classes to implement your own publisher and subscriber apps.

```python
import time
from pyensign.events import Event
from pyensign.publisher import Publisher

class MyPublisher(Publisher):
    def source_events(self):
        while True:
            # Call an API and yield some events!
            data = self.fetch_data()
            yield Event(data=data, mimetype="application/json")
            time.sleep(60)

    def run_forever(self):
        self.run(self.source_events())

publisher = MyPublisher("my-topic")
publisher.run_forever()
```

```python
from pyensign.subscriber import Subscriber

class MySubscriber(Subscriber):
    async def on_event(self, event):
        # Process the event
        ...

        # Ack the event back to Ensign
        event.ack()

subscriber = MySubscriber("my-topic")
subscriber.run()
```


## Contributing to PyEnsign

Wow, you want to contribute to PyEnsign? 😍 We would absolutely love that!

PyEnsign is an open source project that is supported by a community who will gratefully and humbly accept any contributions you might make to the project. Large or small, any contribution makes a big difference; and if you've never contributed to an open source project before, we hope you will start with PyEnsign!

Please check out our Contributor's Guide in `CONTRIBUTING.md` to get a quick orientation first.

We can't wait to hear from you!