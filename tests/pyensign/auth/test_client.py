import os
import jwt
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


@pytest.fixture
def client(httpserver: HTTPServer, creds):
    return AuthClient(httpserver.url_for(""), creds)


def valid_token(sub):
    return jwt.encode({"nbf": 0, "exp": 9999999999, "sub": sub}, key="secret")


def nbf_token(sub):
    return jwt.encode({"nbf": 9999999999, "exp": 9999999999, "sub": sub}, key="secret")


def exp_token(sub):
    return jwt.encode({"nbf": 0, "exp": 0, "sub": sub}, key="secret")


def valid_access():
    return valid_token("access")


def valid_refresh():
    return valid_token("refresh")


def nbf_access():
    return nbf_token("access")


def nbf_refresh():
    return nbf_token("refresh")


def exp_access():
    return exp_token("access")


def exp_refresh():
    return exp_token("refresh")


def authbearer(token):
    return ("authorization", "Bearer " + token)


class TestAuthClient:
    """
    Test authenticating with a mock server
    """

    def test_credentials_cached(self, client):
        client._access_token = valid_access()
        client._refresh_token = valid_refresh()
        assert client.credentials() == authbearer(valid_access())

    @pytest.mark.parametrize(
        "access, refresh",
        [
            (valid_access(), ""),
            ("", valid_refresh()),
            ("", ""),
        ],
    )
    def test_credentials_missing(self, httpserver: HTTPServer, client, access, refresh):
        httpserver.expect_request("/v1/authenticate").respond_with_json(
            {"access_token": valid_access(), "refresh_token": valid_refresh()}
        )

        client._access_token = access
        client._refresh_token = refresh
        assert client.credentials() == authbearer(valid_access())

    def test_credentials_access_invalid(self, httpserver: HTTPServer, client):
        httpserver.expect_request("/v1/refresh").respond_with_json(
            {"access_token": valid_access(), "refresh_token": valid_refresh()}
        )

        client._access_token = exp_access()
        client._refresh_token = valid_refresh()
        assert client.credentials() == authbearer(valid_access())

    @pytest.mark.parametrize(
        "refresh",
        [
            nbf_refresh(),
            exp_refresh(),
        ],
    )
    def test_credentials_refresh_invalid(self, httpserver: HTTPServer, client, refresh):
        httpserver.expect_request("/v1/authenticate").respond_with_json(
            {"access_token": valid_access(), "refresh_token": valid_refresh()}
        )

        client._access_token = exp_access()
        client._refresh_token = refresh
        assert client.credentials() == authbearer(valid_access())

    def test_authenticate(self, httpserver: HTTPServer, client):
        response = {"access_token": valid_access(), "refresh_token": valid_refresh()}
        httpserver.expect_request("/v1/authenticate").respond_with_json(response)

        client.authenticate()
        assert client._access_token == valid_access()
        assert client._refresh_token == valid_refresh()

    def test_authenticate_callback(self, httpserver: HTTPServer, client):
        response = {"access_token": valid_access(), "refresh_token": valid_refresh()}
        httpserver.expect_request("/v1/authenticate").respond_with_json(response)

        # Test capturing metadata from the callback
        meta = None
        error = None

        def fetch_meta(m, e):
            nonlocal meta
            nonlocal error
            meta = m
            error = e

        client("test_authenticate_callback", fetch_meta)
        assert meta[0] == authbearer(valid_access())
        assert error is None

        # Second call uses the cached access token
        meta = None
        error = None
        client("test_authenticate_callback", fetch_meta)
        assert meta[0] == authbearer(valid_access())
        assert error is None

    def test_authenticate_callback_error(self, httpserver: HTTPServer):
        httpserver.expect_request("/v1/authenticate").respond_with_json(
            {"error": "bad API credentials"}, status=400
        )
        client = AuthClient(httpserver.url_for(""), {})

        # Test errors are captured instead of raised
        meta = None
        error = None

        def fetch_meta(m, e):
            nonlocal meta
            nonlocal error
            meta = m
            error = e

        client("test_authenticate_callback_error", fetch_meta)
        assert len(meta) == 0
        assert isinstance(error, AuthenticationError)

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

    def test_refresh(self, httpserver: HTTPServer, client):
        response = {
            "access_token": valid_access(),
            "refresh_token": valid_token("new refresh"),
        }
        httpserver.expect_request("/v1/refresh").respond_with_json(response)

        client._refresh_token = valid_refresh()
        client.refresh()
        assert client._access_token == valid_access()
        assert client._refresh_token == valid_token("new refresh")

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
