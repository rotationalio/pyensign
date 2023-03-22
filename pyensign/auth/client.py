import requests

from pyensign.exceptions import AuthenticationError

ACCESS_TOKEN = "access_token"
REFRESH_TOKEN = "refresh_token"


class AuthClient:
    """
    AuthClient provides an interface for token-based HTTP authentication by requesting
    authentication tokens from a server. Authenticate should be called to obtain an
    initial access token. Refresh should be called periodically to obtain a new access
    token by providing the refresh token to ensure the API connection is uninterrupted.
    """

    def __init__(self, url, creds):
        """
        Create AuthClient from the server URL and API credentials.

        Parameters
        ----------
        url : str
            The full URL of the authentication server including the port (e.g. "http://localhost:8080")
        creds : dict[str, str]
            The credentials to use for authentication
        """

        if url.endswith("/"):
            url = url[:-1]
        self.url = url
        self.creds = creds
        self._access_token = ""
        self._refresh_token = ""
        self._last_login = None

    def authenticate(self):
        # TODO: Check if access token is expired
        self._do(self.url + "/v1/authenticate", self.creds)

    def refresh(self):
        # TODO: Check if refresh token is expired
        if not self._refresh_token:
            raise AuthenticationError("No refresh token available")
        body = {"refresh_token": self._refresh_token}
        self._do(self.url + "/v1/refresh", body)

    def _do(self, url, body):
        response = requests.post(url, json=body)
        if response.status_code != 200:
            raise AuthenticationError(
                "Failed to authenticate with API credentials: %s" % response.text
            )
        rep = response.json()
        if ACCESS_TOKEN not in rep:
            raise AuthenticationError("Missing access_token in response")
        self._access_token = rep[ACCESS_TOKEN]
        if REFRESH_TOKEN in rep:
            self._refresh_token = rep[REFRESH_TOKEN]
