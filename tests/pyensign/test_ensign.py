import os
import json
import pytest

from pyensign.ensign import Ensign
from pyensign.api.v1beta1 import event_pb2
from pyensign.mimetype.v1beta1.mimetype_pb2 import MIME


@pytest.fixture
def live(request):
    return request.config.getoption("--live", default=False)


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def ensignserver():
    return os.environ.get("ENSIGN_SERVER")


class TestEnsign:
    """
    Tests for the Ensign client.
    """

    def test_live_pubsub(self, live, authserver, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not authserver:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.skip("Skipping live test")

        ensign = Ensign(endpoint=ensignserver, auth_url=authserver)

        data = json.dumps({"foo": "bar"}).encode("utf-8")
        type = event_pb2.Type(name="Generic", version=1)
        event = event_pb2.Event(data=data, mime_type=MIME.APPLICATION_JSON, type=type)
        errors = ensign.publish(event)
        assert len(errors) == 0

        recv = [e for e in ensign.subscribe("test_live_pubsub")]
        assert len(recv) == 1
        assert recv[0].data == data
