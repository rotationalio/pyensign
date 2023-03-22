import os
import pytest
from pytest_httpserver import HTTPServer

from pyensign.auth.client import AuthClient
from pyensign.exceptions import AuthenticationError


@pytest.fixture
def live(request):
    return request.config.getoption("--live", default=False)


@pytest.fixture
def authserver():
    return os.environ.get("ENSIGN_AUTH_SERVER")


@pytest.fixture
def creds():
    return {
        "client_id": os.environ.get("ENSIGN_CLIENT_ID"),
        "client_secret": os.environ.get("ENSIGN_CLIENT_SECRET"),
    }


class TestAuthClient:
    """
    Test authenticating with a mock server
    """

    def test_authenticate(self, httpserver: HTTPServer):
        response = {"access_token": "access", "refresh_token": "refresh"}
        httpserver.expect_request("/v1/authenticate").respond_with_json(response)

        creds = {"client_id": "id", "client_secret": "secret"}
        client = AuthClient(httpserver.url_for(""), creds)
        client.authenticate()
        assert client._access_token == "access"
        assert client._refresh_token == "refresh"

    def test_authenticate_bad_credentials(self, httpserver: HTTPServer):
        httpserver.expect_request("/v1/authenticate").respond_with_json(
            {"error": "bad API credentials"}, status=400
        )

        client = AuthClient(httpserver.url_for(""), {})
        with pytest.raises(AuthenticationError):
            client.authenticate()

    def test_authenticate_no_access(self, httpserver: HTTPServer):
        response = {"refresh_token": "refresh"}
        httpserver.expect_request("/v1/authenticate").respond_with_json(response)

        client = AuthClient(httpserver.url_for(""), {})
        with pytest.raises(AuthenticationError):
            client.authenticate()

    def test_refresh(self, httpserver: HTTPServer):
        response = {"access_token": "access", "refresh_token": "new refresh"}
        httpserver.expect_request("/v1/refresh").respond_with_json(response)

        creds = {"client_id": "id", "client_secret": "secret"}
        if not creds:
            raise ValueError("Missing credentials")
        client = AuthClient(httpserver.url_for(""), creds)
        client._refresh_token = "refresh"
        client.refresh()
        assert client._access_token == "access"
        assert client._refresh_token == "new refresh"

    def test_refresh_no_refresh(self, httpserver: HTTPServer):
        client = AuthClient(httpserver.url_for(""), {})
        with pytest.raises(AuthenticationError):
            client.refresh()

    def test_live(self, live, authserver, creds):
        if not live:
            pytest.skip("Skipping live tests")

        if not authserver:
            pytest.fail("ENSIGN_AUTH_SERVER must be set to run live tests")

        if "client_id" not in creds or "client_secret" not in creds:
            pytest.fail(
                "ENSIGN_CLIENT_ID and ENSIGN_CLIENT_SECRET must be set to run live tests"
            )

        client = AuthClient(authserver, creds)
        client.authenticate()
        assert client._access_token != ""
        assert client._refresh_token != ""
