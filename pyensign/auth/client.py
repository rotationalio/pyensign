import grpc
import time
import requests

from pyensign.exceptions import AuthenticationError
from pyensign.auth.tokens import expires_at, not_before

ACCESS_TOKEN = "access_token"
REFRESH_TOKEN = "refresh_token"


class AuthClient(grpc.AuthMetadataPlugin):
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

    def __call__(self, _, callback):
        """
        gRPC callback to add the access token on each request.
        TODO: Refresh credentials in the background to avoid blocking RPC calls
        """
        meta = []
        exception = None
        try:
            meta.append(self.credentials())
        except AuthenticationError as e:
            exception = e
        callback(meta, exception)

    def credentials(self):
        """
        Returns the list of credentials to use for token-based authentication. This
        uses the tokens that are already available if possible, otherwise this function
        will attempt to request tokens from the authentication server.
        """

        # If tokens are missing or parital, authenticate
        if not self._access_token or not self._refresh_token:
            self.authenticate()

        # Refresh the access token if not valid
        if not self._access_valid():
            # If refresh is valid then it can be used to get a new access token
            # Otherwise, full authentication is required
            if self._refresh_valid():
                self.refresh()
            else:
                self.authenticate()

        # Return the credentials
        return ("authorization", "Bearer " + self._access_token)

    def authenticate(self):
        self._do(self.url + "/v1/authenticate", self.creds)

    def refresh(self):
        if not self._refresh_token:
            raise AuthenticationError("No refresh token available")
        body = {"refresh_token": self._refresh_token}
        self._do(self.url + "/v1/refresh", body)

    def _access_valid(self):
        """
        Returns True if the access token is not expired.
        """
        if not self._access_token:
            return False
        return time.time() < expires_at(self._access_token)

    def _refresh_valid(self):
        """
        Returns True if the refresh token has not expired and it is after the not
        before time.
        """
        if not self._refresh_token:
            return False
        now = time.time()
        return now > not_before(self._refresh_token) and now < expires_at(
            self._refresh_token
        )

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
