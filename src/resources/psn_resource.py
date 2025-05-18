"""PlayStation Network Resource for interacting with PSN API via PSNAWP."""

from dataclasses import asdict
from typing import Any

import dagster as dg
from loguru import logger
from psnawp_api import PSNAWP
from psnawp_api.models import Client
from pydantic import PrivateAttr


class PSNResource(dg.ConfigurableResource):
    """
    A Dagster resource for interacting with the PlayStation Network API.

    This resource handles authentication and API calls to PSN using the PSNAWP library.
    It maintains a single client instance to reduce redundant API calls within a job.

    Parameters
    ----------
    refresh_token : str
        The PSN refresh token used for authentication

    Attributes
    ----------
    _client : PSNAWP
        The PSNAWP client instance (private)
    _user : PSNAWPUser
        The current user instance from PSNAWP (private)
    """

    # Configuration params
    refresh_token: str

    # Private attributes to track client state
    _client: PSNAWP = PrivateAttr(default=None)
    _user: Client = PrivateAttr(default=None)

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """
        Initialize the resource during Dagster execution.

        This method is called by Dagster when the resource is initialized.
        It creates the PSNAWP client instance using the refresh token.

        Parameters
        ----------
        context : dg.InitResourceContext
            The Dagster initialization context

        Raises
        ------
        Exception
            If client initialization fails
        """
        self._initialize_client()
        context.log.info("PlayStation Network client initialized")

    def _initialize_client(self) -> None:
        """
        Initialize the PSNAWP client with the refresh token.

        Creates a new PSNAWP instance and sets up the user reference.

        Parameters
        ----------
        None

        Raises
        ------
        ValueError
            If client initialization fails
        """
        try:
            if self._client is None:
                logger.info("Initiating PSN client")
                self._client = PSNAWP(self.refresh_token)
            if self._user is None:
                logger.info("Initiating PSN user info")
                self._user = self._client.me()
        except Exception as e:
            raise ValueError(f"Failed to initialize PSN client: {e}") from e

    def get_client(self) -> PSNAWP:
        """
        Get the PSNAWP client instance.

        Returns the cached PSNAWP client for making API calls.

        Parameters
        ----------
        None

        Returns
        -------
        PSNAWP
            The initialized PSNAWP client instance

        Examples
        --------
        >>> client = psn_resource.get_client()
        >>> game = client.game_title(title_id="CUSA00572_00")
        """
        if self._client is None:
            self._initialize_client()
        return self._client

    def get_user(self) -> Client:
        """
        Get the current user instance.

        Returns the cached user instance for accessing user-specific data.

        Parameters
        ----------
        None

        Returns
        -------
        PSNAWPUser
            The current user instance

        Examples
        --------
        >>> user = psn_resource.get_user()
        >>> profile = user.get_profile_legacy()
        """
        if self._user is None:
            self._initialize_client()
        return self._user

    def get_profile(self) -> dict[str, Any]:
        """
        Get the user's profile information.

        Retrieves the complete profile data from PSN including trophy information.

        Parameters
        ----------
        None

        Returns
        -------
        Dict[str, Any]
            The user profile information as a dictionary

        Examples
        --------
        >>> profile = psn_resource.get_profile()
        >>> trophy_level = profile["profile"]["trophySummary"]["level"]
        """
        user = self.get_user()
        return user.get_profile_legacy()

    def get_account_devices(self) -> list[Any]:
        """
        Get the user's registered PlayStation devices.

        Retrieves a list of devices associated with the user's account.

        Parameters
        ----------
        None

        Returns
        -------
        List[Any]
            List of device objects associated with the account

        Examples
        --------
        >>> devices = psn_resource.get_account_devices()
        >>> for device in devices:
        >>>     print(device)
        """
        user = self.get_user()
        return user.get_account_devices()

    def get_title_stats(self) -> list[dict[str, Any]]:
        """
        Get the user's game title statistics.

        Retrieves statistics for all games played by the user,
        converted to dictionaries for easier access.

        Parameters
        ----------
        None

        Returns
        -------
        List[Dict[str, Any]]
            List of game title statistics as dictionaries

        Examples
        --------
        >>> stats = psn_resource.get_title_stats()
        >>> for game in stats:
        >>>     print(f"Game: {game['name']}, Play time: {game['play_duration']}")
        """
        user = self.get_user()
        return [asdict(title) for title in user.title_stats()]

    def get_game_details(self, title_id: str) -> dict[str, Any]:
        """
        Get detailed information about a specific game.

        Retrieves game information including name, genres, and other metadata.

        Parameters
        ----------
        title_id : str
            The PSN title ID of the game

        Returns
        -------
        dict[str, Any]
            Game details as a list of dictionaries

        Examples
        --------
        >>> game_info = psn_resource.get_game_details("CUSA00572_00")
        >>> print(f"Game: {game_info[0]['name']}, Genres: {game_info[0]['genres']}")
        """
        client = self.get_client()
        return client.game_title(title_id=title_id).get_details()
