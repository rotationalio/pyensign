import os
import pytest
from pytest_httpserver import HTTPServer

from pyensign.auth.client import AuthClient
from pyensign.connection import Connection
from pyensign.connection import Client


@pytest.fixture
def live(request):
    return request.config.getoption("--live")


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def ensignserver():
    return os.environ.get("ENSIGN_SERVER")


@pytest.fixture
def auth(httpserver: HTTPServer):
    creds = {"client_id": "id", "client_secret": "secret"}
    return AuthClient(httpserver.url_for(""), creds)


@pytest.fixture
def creds():
    return {
        "client_id": os.environ.get("ENSIGN_CLIENT_ID"),
        "client_secret": os.environ.get("ENSIGN_CLIENT_SECRET"),
    }


class TestConnection:
    """
    Test establishing a connection to an Ensign server.
    """

    def test_connect(self):
        conn = Connection("localhost:5356")
        assert conn.channel is not None

    def test_connect_insecure(self):
        conn = Connection("localhost:5356", insecure=True)
        assert conn.channel is not None

    def test_connect_secure(self, auth):
        conn = Connection("localhost:5356", auth=auth)
        assert conn.channel is not None

    @pytest.mark.parametrize(
        "addrport",
        [
            "localhost",
            "localhost:",
            ":5356",
            "localhost:5356:5356" "https://localhost:5356",
        ],
    )
    def test_connect_bad_addr(self, addrport):
        with pytest.raises(ValueError):
            Connection(addrport)

    def test_connect_bad_args(self):
        with pytest.raises(ValueError):
            Connection(
                "localhost:5356", insecure=True, auth=AuthClient("localhost:5356", {})
            )


class TestClient:
    """
    Test calling live gRPC endpoints.
    """

    def test_insecure(self, live, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")

        ensign = Client(Connection(ensignserver, insecure=True))
        status, version, uptime, not_before, not_after = ensign.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    def test_status(self, live, ensignserver):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")

        ensign = Client(Connection(ensignserver))
        status, version, uptime, not_before, not_after = ensign.status()
        assert status is not None
        assert version is not None
        assert uptime is not None
        assert not_before is not None
        assert not_after is not None

    def test_auth_endpoint(self, live, ensignserver, authserver, creds):
        if not live:
            pytest.skip("Skipping live test")
        if not ensignserver:
            pytest.fail("ENSIGN_SERVER environment variable not set")
        if not authserver:
            pytest.fail("ENSIGN_AUTH_SERVER environment variable not set")
        if not creds:
            pytest.fail(
                "ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET environment variables not set"
            )

        ensign = Client(Connection(ensignserver, auth=AuthClient(authserver, creds)))
        topics, next_page_token = ensign.list_topics()
        assert topics is not None
        assert next_page_token is not None
