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

event = Event(b'{"temp": 72, "units": "fahrenheit"}', "APPLICATION_JSON")
```

Publish events to a topic. This function accepts anything that is iterable. Errors are concatenated into a list and returned after all the events have been published.

```python
errors = await client.publish("weather", event)
```

Subcribe to a topic or list of topics.

```python
async for event in client.subscribe("weather"):
    print("Received event: {}".format(event))
```