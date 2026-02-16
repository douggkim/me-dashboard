"""Spotify Resource for interacting with Spotify API."""

import time
from typing import Any, ClassVar

import dagster as dg
import requests
from furl import furl
from pydantic import PrivateAttr


class SpotifyResource(dg.ConfigurableResource):
    """
    A Dagster resource for interacting with the Spotify API.

    This resource handles authentication, token refresh, and API calls to Spotify.
    It maintains a valid access token and automatically refreshes it when needed.

    Parameters
    ----------
    client_id : str
        The Spotify API client ID
    client_secret : str
        The Spotify API client secret
    refresh_token : str
        The refresh token used to obtain new access tokens

    Attributes
    ----------
    _access_token : str
        The current access token (private)
    _token_expiry : float
        Timestamp when the current token expires (private)
    _headers : Dict[str, str]
        Authorization headers with the current access token (private)
    """

    SPOTIFY_API_BASE: ClassVar[str] = "https://api.spotify.com"
    SPOTIFY_ACCOUNTS_BASE: ClassVar[str] = "https://accounts.spotify.com"

    # Configuration parameters
    client_id: str
    client_secret: str
    refresh_token: str

    # Private attributes to track token state
    _access_token: str = PrivateAttr(default=None)
    _token_expiry: float = PrivateAttr(default=0)
    _headers: dict[str, str] = PrivateAttr(default=None)

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """
        Initialize the resource during Dagster execution.

        This method is called by Dagster when the resource is initialized.
        It obtains an initial access token using the refresh token.

        Parameters
        ----------
        context : dg.InitResourceContext
            The Dagster initialization context

        Raises
        ------
        Exception
            If token refresh fails during initialization
        """
        # Initialize token during resource setup
        self._refresh_access_token()
        context.log.info("Spotify API token initialized")

    def _refresh_access_token(self) -> None:
        """
        Refresh the access token using the refresh token.

        Makes a request to Spotify's token endpoint to obtain a new access token
        using the stored refresh token. Updates the internal token state.

        Parameters
        ----------
        None

        Raises
        ------
        ValueError
            If the token refresh request fails or returns a non-200 status code
        """
        # Construct token URL using furl
        url = furl(self.SPOTIFY_ACCOUNTS_BASE)
        url.path.add("api/token")

        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response = requests.post(url.url, data=data, timeout=30)
        if response.status_code != 200:
            raise ValueError(f"Failed to refresh token: {response.text}")

        token_data = response.json()
        self._access_token = token_data["access_token"]
        # Default expires_in is 3600 seconds (1 hour)
        # Subtract 5 minutes as buffer
        self._token_expiry = time.time() + token_data.get("expires_in", 3600) - 300

        # Update headers with new access token
        self._headers = {"Authorization": f"Bearer {self._access_token}"}

        # If a new refresh token is provided, update it (happens occasionally)
        if "refresh_token" in token_data:
            self.refresh_token = token_data["refresh_token"]

    def get_access_token(self) -> str:
        """
        Get a valid access token, refreshing if necessary.

        Checks if the current token is expired and refreshes it if needed.
        Always returns a valid token.

        Parameters
        ----------
        None

        Returns
        -------
        str
            A valid Spotify access token

        Raises
        ------
        Exception
            If token refresh fails (forwarded from _refresh_access_token)
        """
        # Check if token is expired and refresh if needed
        if time.time() >= self._token_expiry:
            self._refresh_access_token()
        return self._access_token

    def get_headers(self) -> dict[str, str]:
        """
        Get authorization headers with a valid access token.

        Returns the cached authorization headers. If the access token
        has expired, it will first refresh the token and update the headers.

        Parameters
        ----------
        None

        Returns
        -------
        Dict[str, str]
            Headers dictionary with Authorization bearer token

        Raises
        ------
        Exception
            If token refresh fails (forwarded from _refresh_access_token)

        Examples
        --------
        >>> headers = spotify_resource.get_headers()
        >>> response = requests.get("https://api.spotify.com/v1/me", headers=headers)
        """
        # Check if token is expired and refresh if needed
        if time.time() >= self._token_expiry:
            self._refresh_access_token()
        return self._headers

    def call_api(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """
        Make a request to the Spotify API.

        Constructs the full URL with the Spotify API base URL and makes a GET request
        with the provided parameters and proper authorization.

        Parameters
        ----------
        endpoint : str
            The API endpoint path, without the base URL
            (e.g., "me/playlists" or "tracks/{id}")
        params : Dict[str, Any], optional
            Query parameters to include in the request, by default None

        Returns
        -------
        Dict[str, Any]
            The JSON response from the API, parsed as a dictionary

        Raises
        ------
        Exception
            If token refresh fails (forwarded from get_headers)
        requests.exceptions.RequestException
            If the HTTP request fails
        ValueError
            If the response is not valid JSON
        KeyError, TypeError
            If the response JSON structure is unexpected

        Examples
        --------
        >>> # Get currently playing track
        >>> current_track = spotify_resource.call_api("me/player/currently-playing")
        >>>
        >>> # Get a user's playlists with pagination parameters
        >>> playlists = spotify_resource.call_api("me/playlists", {"limit": 50, "offset": 0})
        """
        # Get headers with a valid token
        headers = self.get_headers()

        # Handle both absolute URLs and relative endpoints
        if endpoint.startswith(("http://", "https://")):
            # If it's already a complete URL, use furl to parse it
            url = furl(endpoint)
        else:
            # Remove leading slash if present
            endpoint = endpoint.removeprefix("/")

            # Build URL using furl
            url = furl(self.SPOTIFY_API_BASE)
            url.path.add("v1")
            url.path.add(endpoint)

        # Add query parameters
        if params:
            url.args.update(params)

        response = requests.get(url.url, headers=headers, timeout=30)

        # Check for successful response before parsing JSON
        response.raise_for_status()

        return response.json()
