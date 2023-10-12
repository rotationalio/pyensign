# PyEnsign

PyEnsign is the official Python SDK for [Ensign](https://rotational.io/ensign), a distributed event store and stream-processing platform. This library allows you to interact with the Ensign API directly from Python in order to create [publishers](https://ensign.rotational.dev/eventing/glossary/#publisher) and [subscribers](https://ensign.rotational.dev/eventing/glossary/#subscriber).

## Installation

```
pip install pyensign
```

## Usage

Create a client from a client ID and client secret. If not provided, these will be obtained from the `ENSIGN_CLIENT_ID` and `ENSIGN_CLIENT_SECRET` variables.

```python
from pyensign.ensign import Ensign

client = Ensign(client_id=<your client ID>, client_secret=<your client secret>)
```

The `Event` class can be used to create events from the raw data and mimetype.

```python
from pyensign.events import Event

event = Event(b'{"temp": 72, "units": "fahrenheit"}', "application/json")
```

Publish events to a topic. This coroutine accepts one or more events, so the following uses are all valid.

```python
await client.publish("weather", event)
await client.publish("weather", event1, event2)
await client.publish("weather", [event1, event2])
```

Publish is asynchronous. You should generally call `flush()` before your program exits to ensure that all events are published to the server.

```python
# Wait for events to be published with a default timeout of 2 seconds.
await client.flush()
```

For more precision, you can wait for individual events to be acked by the server.

```python
ack = await event.wait_for_ack()
print(ack)
```

Subscribe to one or more topics by providing the topic name(s) or ID(s).

```python
async for event in client.subscribe("weather"):
    print("Received event with data: {}".format(event.data))
    await event.ack()
```

## Advanced Usage

The `publish` coroutine accepts asynchronous callbacks so the client can distinguish between committed and uncommitted events. Callbacks are invoked when acks and nacks are received from the server and the first argument passed to the callback is the `Ack` or `Nack` itself. An `Ack` contains a committed timestamp. A `Nack` is returned if the event couldn't be committed and contains the ID of the event along with an error describing what went wrong.

```python
async def handle_ack(self, ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(f"Event committed at {ts}")

async def handle_nack(self, nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

await client.publish("weather", event, on_ack=handle_ack, on_nack=handle_nack)
```