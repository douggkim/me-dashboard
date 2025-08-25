"""
GPS Caching Resource - Comprehensive geospatial caching with geohash indexing.

This module provides a Dagster resource for GPS geocoding with intelligent
geospatial caching using geohash-based spatial indexing for ~500m radius matching.
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from urllib.parse import urlparse

import dagster as dg
import fsspec
import googlemaps
import polars as pl
import pygeohash as pgh
from loguru import logger
from pydantic import PrivateAttr

from src.utils.aws import AWSCredentialFormat, get_aws_storage_options


@dataclass
class LocationInfo:
    """
    Represents a cached geocoding result with spatial and temporal metadata.

    Attributes
    ----------
    geohash : str
        Geohash string representing the spatial location
    coordinates : Dict[str, float]
        Dictionary containing 'lat' and 'lng' keys with coordinate values
    geocoding_data : Dict[str, Any]
        Complete geocoding response from Google API
    cached_at : str
        ISO timestamp when the location was cached
    expires_at : str
        ISO timestamp when the cache entry expires
    """

    geohash: str
    coordinates: dict[str, float]
    geocoding_data: dict[str, Any]
    cached_at: str
    expires_at: str


class GeoEncoderResource(dg.ConfigurableResource):
    """
    A Dagster resource for GPS geocoding with intelligent geospatial caching.

    This resource provides geohash-based spatial indexing for efficient proximity
    matching within ~500m radius, S3-backed persistent cache with 30-day TTL for
    Google API compliance, neighbor checking for comprehensive proximity matching,
    and seamless Polars DataFrame integration for batch processing.

    The resource uses geohash precision 6 (~1.2km x 609m cells) with 8-neighbor
    checking to handle edge cases at cell boundaries, ensuring reliable spatial
    proximity matching for geocoding cache hits.

    Parameters
    ----------
    api_key : str
        Google Maps API key for geocoding requests
    s3_bucket : str
        S3 bucket name for cache storage
    s3_cache_key : str, optional
        S3 object key for cache file, by default "gps_cache/geocoding_cache.json"
    cache_precision : int, optional
        Geohash precision level (1-12), by default 6 (~500m radius coverage)
    cache_ttl_days : int, optional
        Cache TTL in days for Google API compliance, by default 30
    batch_size : int, optional
        Batch size for API rate limiting, by default 50
    requests_per_second : int, optional
        Rate limit for API requests, by default 40 (below Google's 50/sec limit)

    Attributes
    ----------
    _google_api_client : googlemaps.Client
        Google Maps client instance (private)
    _filesystem : fsspec.AbstractFileSystem
        Filesystem instance for cache storage (private)
    _cache : Dict[str, LocationInfo]
        In-memory cache of geocoded locations (private)
    _cache_loaded : bool
        Flag indicating if cache has been loaded from storage (private)

    Examples
    --------
    >>> gps_resource = GPSCachingResource(
    ...     api_key="your_google_api_key",
    ...     s3_bucket="your-cache-bucket",
    ...     cache_precision=6
    ... )
    >>>
    >>> # Use in Dagster asset
    >>> @dg.asset
    ... def process_locations(gps_cache: GPSCachingResource) -> pl.DataFrame:
    ...     df = pl.DataFrame({"lat": [37.7749], "lng": [-122.4194]})
    ...     return gps_cache.enrich_dataframe(df)
    """

    # Configuration parameters
    google_maps_api_key: str
    s3_bucket: str
    s3_cache_location: str = "gps_cache/geocoding_cache.json"
    cache_precision: int = 7
    cache_ttl_days: int = 30
    batch_size: int = 50
    queries_per_second: int = 40

    _google_api_client: googlemaps.Client = PrivateAttr(default=None)
    _filesystem: Any = PrivateAttr(default=None)
    _cache: dict[str, LocationInfo] = PrivateAttr(default_factory=dict)
    _cache_loaded: bool = PrivateAttr(default=False)

    def setup_for_execution(self, context: dg.InitResourceContext) -> None:
        """
        Initialize the resource during Dagster execution.

        This method is called automatically by Dagster when the resource is
        initialized. It sets up the Google Maps and S3 clients, then loads
        the existing cache from S3.

        Parameters
        ----------
        context : dg.InitResourceContext
            Dagster initialization context for logging

        Raises
        ------
        ValueError
            If Google Maps client initialization fails
        Exception
            If S3 client initialization or cache loading fails
        """
        self._initialize_clients()
        self._load_cache()
        context.log.info(f"GPS Caching Resource initialized with {len(self._cache)} cached locations")

    def _initialize_clients(self) -> None:
        """
        Initialize Google Maps client and filesystem for cache storage.

        Creates singleton instances of the Google Maps client with rate limiting
        and the fsspec filesystem for cache persistence.

        Raises
        ------
        ValueError
            If Google Maps client initialization fails with invalid API key
        Exception
            If filesystem initialization fails
        """
        if self._google_api_client is None:
            try:
                logger.info("Initializing Google Maps client")
                self._google_api_client = googlemaps.Client(
                    key=self.google_maps_api_key, queries_per_second=self.queries_per_second
                )
            except Exception as e:
                raise ValueError(f"Failed to initialize Google Maps client: {e}") from e

        if self._filesystem is None:
            try:
                logger.info("Initializing filesystem for cache storage")
                # Get full cache path to determine filesystem type
                cache_path = f"s3://{self.s3_bucket}/{self.s3_cache_location}"
                parsed = urlparse(cache_path)
                storage_options = get_aws_storage_options(return_credential_type=AWSCredentialFormat.UTILIZE_ENV_VARS)

                self._filesystem = (
                    fsspec.filesystem("file")
                    if parsed.scheme in {"", "file"}
                    else fsspec.filesystem(parsed.scheme, **storage_options)
                )
            except Exception as e:
                raise ValueError(f"Failed to initialize filesystem: {e}") from e

    def _load_cache(self) -> None:
        """
        Load geocoding cache from storage.

        Retrieves the cache file from storage, filters out expired entries based on
        TTL, and populates the in-memory cache with valid LocationInfo objects.
        If no cache file exists, starts with an empty cache.

        Raises
        ------
        Exception
            If storage access fails or JSON parsing fails
        """
        if self._cache_loaded:
            return

        cache_path = f"s3://{self.s3_bucket}/{self.s3_cache_location}"

        try:
            # Check if cache file exists
            if not self._filesystem.exists(cache_path):
                logger.info(f"Cache file does not exist at {cache_path}, starting with empty cache")
                self._cache = {}
                self._cache_loaded = True
                return

            # Load cache file
            with self._filesystem.open(cache_path, "r") as f:
                cache_data = json.load(f)

            # Convert to LocationInfo objects and filter expired entries
            current_time = datetime.now(UTC)
            valid_cache = {}

            for geohash, data in cache_data.items():
                expires_at = datetime.fromisoformat(data["expires_at"])
                if expires_at > current_time:  # if time left until expiration
                    valid_cache[geohash] = LocationInfo(**data)

            self._cache = valid_cache
            logger.info(f"Loaded {len(self._cache)} valid cached locations from {cache_path}")

        except Exception:  # noqa: BLE001
            logger.exception(f"Error loading cache from {cache_path}")
            self._cache = {}

        self._cache_loaded = True

    def _save_cache(self) -> None:
        """
        Save the current cache to storage.

        Serializes all LocationInfo objects to JSON format and uploads
        to storage for persistence across resource instances.

        Raises
        ------
        Exception
            If storage upload fails or JSON serialization fails
        """
        cache_path = f"s3://{self.s3_bucket}/{self.s3_cache_location}"

        try:
            # Convert LocationInfo objects to dict for JSON serialization
            cache_data = {
                geohash: {
                    "geohash": location.geohash,
                    "coordinates": location.coordinates,
                    "geocoding_data": location.geocoding_data,
                    "cached_at": location.cached_at,
                    "expires_at": location.expires_at,
                }
                for geohash, location in self._cache.items()
            }

            # Save to storage using fsspec
            with self._filesystem.open(cache_path, "w") as f:
                json.dump(cache_data, f, indent=2)

            logger.info(f"Saved {len(self._cache)} cached locations to {cache_path}")

        except Exception as e:
            logger.error(f"Error saving cache to {cache_path}: {e}")
            raise

    def _get_geohash(self, lat: float, lng: float) -> str:
        """
        Convert latitude/longitude coordinates to geohash string.

        Parameters
        ----------
        lat : float
            Latitude coordinate in decimal degrees
        lng : float
            Longitude coordinate in decimal degrees

        Returns
        -------
        str
            Geohash string at the configured precision level

        Raises
        ------
        ValueError
            If coordinates are invalid (lat not in [-90,90] or lng not in [-180,180])
        """
        lat_max_abs_val = 90
        lng_max_abs_val = 180
        if not (-1 * lat_max_abs_val <= lat <= lat_max_abs_val):
            raise ValueError(f"Invalid latitude: {lat}. Must be between -90 and 90")
        if not (-1 * lng_max_abs_val <= lng <= lng_max_abs_val):
            raise ValueError(f"Invalid longitude: {lng}. Must be between -180 and 180")

        return pgh.encode(lat, lng, precision=self.cache_precision)

    def _get_neighbors(self, geohash: str) -> list[str]:
        """
        Get the 8 neighboring geohashes for proximity search.

        Retrieves all 8 adjacent geohash cells to handle edge cases where
        nearby locations might fall in different geohash cells.

        Parameters
        ----------
        geohash : str
            Center geohash for which to find neighbors

        Returns
        -------
        List[str]
            List containing the center geohash plus its 8 neighbors (9 total)

        Raises
        ------
        Exception
            If geohash neighbor calculation fails
        """
        try:
            neighbors = []

            # Get 4 cardinal direction neighbors
            cardinal_directions = ["top", "bottom", "right", "left"]
            cardinal_neighbors = {}

            for direction in cardinal_directions:
                try:
                    neighbor_hash = pgh.get_adjacent(geohash=geohash, direction=direction)
                    neighbors.append(neighbor_hash)
                    cardinal_neighbors[direction] = neighbor_hash
                except Exception:  # noqa: BLE001
                    logger.debug(f"Failed to get {direction} neighbor for geohash {geohash}")
                    continue

            # Get 4 diagonal neighbors by chaining cardinal directions
            diagonal_combinations = [
                ("top", "right"),  # top-right
                ("top", "left"),  # top-left
                ("bottom", "right"),  # bottom-right
                ("bottom", "left"),  # bottom-left
            ]

            for first_dir, second_dir in diagonal_combinations:
                try:
                    # Chain two cardinal moves to get diagonal neighbor
                    if first_dir in cardinal_neighbors:
                        diagonal_hash = pgh.get_adjacent(geohash=cardinal_neighbors[first_dir], direction=second_dir)
                        neighbors.append(diagonal_hash)
                except Exception:  # noqa: BLE001
                    logger.debug(f"Failed to get diagonal neighbor {first_dir}-{second_dir} for geohash {geohash}")
                    continue

            # Return center + all valid neighbors (up to 8 neighbors)
            return [geohash, *neighbors]
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to get neighbors for geohash {geohash}: {e}")
            return [geohash]

    def _check_cache(self, lat: float, lng: float) -> LocationInfo | None:
        """
        Check if coordinates are in cache using geohash proximity matching.

        Converts coordinates to geohash and checks both the center cell and
        its 8 neighbors for cached entries. This handles cases where nearby
        locations might fall in adjacent geohash cells.

        Parameters
        ----------
        lat : float
            Latitude coordinate to check
        lng : float
            Longitude coordinate to check

        Returns
        -------
        Optional[CachedLocation]
            Cached location if found and not expired, None otherwise

        Raises
        ------
        ValueError
            If coordinates are invalid
        """
        center_geohash = self._get_geohash(lat, lng)
        center_n_neighbor_geohashes = self._get_neighbors(center_geohash)

        # Check cache for any matching geohash
        for geohash in center_n_neighbor_geohashes:
            if geohash in self._cache:
                cached_location = self._cache[geohash]
                # Verify it's still valid
                if datetime.fromisoformat(cached_location.expires_at) > datetime.now(UTC):
                    return cached_location
                # Remove expired entry
                del self._cache[geohash]

        return None

    def _geocode_coordinates(self, lat: float, lng: float) -> dict[str, Any] | None:
        """
        Geocode coordinates using Google Maps API.

        Performs reverse geocoding to convert coordinates into address information
        using the Google Maps Geocoding API.

        Parameters
        ----------
        lat : float
            Latitude coordinate
        lng : float
            Longitude coordinate

        Returns
        -------
        Dict[str, Any] | None
            Geocoding response data if successful, None if no results found

        Raises
        ------
        Exception
            If API request fails or rate limits are exceeded
        """
        try:
            results = self._google_api_client.reverse_geocode((lat, lng))
            if results:
                return {
                    "coordinates": {"lat": lat, "lng": lng},
                    "reverse_geocoding": results[0],
                    "formatted_address": results[0].get("formatted_address"),
                    "address_components": results[0].get("address_components", []),
                    "place_id": results[0].get("place_id"),
                    "geometry": results[0].get("geometry", {}),
                }
            return None
        except Exception as e:
            logger.error(f"Error geocoding {lat}, {lng}: {e}")
            raise

    def _cache_location(self, lat: float, lng: float, geocoding_data: dict[str, Any]) -> None:
        """
        Cache a geocoded location with TTL expiration.

        Stores the geocoding result in the cache with automatic expiration
        based on the configured TTL to comply with Google API terms.

        Parameters
        ----------
        lat : float
            Latitude coordinate
        lng : float
            Longitude coordinate
        geocoding_data : Dict[str, Any]
            Complete geocoding response from API

        Raises
        ------
        ValueError
            If coordinates are invalid for geohash generation
        """
        geohash = self._get_geohash(lat, lng)
        current_time = datetime.now(UTC)
        expires_at = current_time + timedelta(days=self.cache_ttl_days)

        location_info = LocationInfo(
            geohash=geohash,
            coordinates={"lat": lat, "lng": lng},
            geocoding_data=geocoding_data,
            cached_at=current_time.isoformat(),
            expires_at=expires_at.isoformat(),
        )

        self._cache[geohash] = location_info

    def enrich_dataframe(
        self,
        df: pl.DataFrame,
        lat_col: str = "latitude",
        lng_col: str = "longitude",
        enrich_columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Enrich a Polars DataFrame with geocoding data using intelligent caching.

        This is the main method for batch geocoding. It extracts unique coordinate
        pairs, checks cache for existing data, makes API calls only for uncached
        locations, and joins enriched data back to the original DataFrame.

        Parameters
        ----------
        df : pl.DataFrame
            Input DataFrame containing latitude and longitude columns
        lat_col : str, optional
            Name of the latitude column, by default "latitude"
        lng_col : str, optional
            Name of the longitude column, by default "longitude"
        enrich_columns : Optional[List[str]], optional
            Specific geocoding fields to extract. If None, uses default set
            including formatted_address, country, state, city, postal_code, by default None

        Returns
        -------
        pl.DataFrame
            Original DataFrame enriched with geocoding data columns

        Raises
        ------
        ValueError
            If specified lat/lng columns don't exist in DataFrame
        Exception
            If geocoding API calls fail or S3 operations fail

        Examples
        --------
        >>> df = pl.DataFrame({
        ...     "id": [1, 2],
        ...     "latitude": [37.7749, 40.7589],
        ...     "longitude": [-122.4194, -73.9851]
        ... })
        >>> enriched_df = gps_cache.enrich_dataframe(df)
        >>> print(enriched_df.columns)
        ['id', 'latitude', 'longitude', 'formatted_address', 'country', ...]
        """
        if lat_col not in df.columns:
            raise ValueError(f"Latitude column '{lat_col}' not found in DataFrame")
        if lng_col not in df.columns:
            raise ValueError(f"Longitude column '{lng_col}' not found in DataFrame")

        logger.info(f"Enriching DataFrame with {len(df)} rows")

        # Extract unique lat/lng combinations
        unique_coords = (
            df.select([lat_col, lng_col])
            .unique()
            .filter((pl.col(lat_col).is_not_null()) & (pl.col(lng_col).is_not_null()))
        ).to_dicts()

        logger.info(f"Found {len(unique_coords)} unique coordinate pairs")

        # Separate cached vs uncached coordinates
        cached_data = []
        uncached_coords = []

        for coord in unique_coords:
            lat, lng = coord[lat_col], coord[lng_col]
            cached_location = self._check_cache(lat, lng)

            if cached_location:
                enriched_row = self._extract_enrichment_data(
                    lat=lat, lng=lng, geocoding_data=cached_location.geocoding_data, enrich_columns=enrich_columns
                )
                cached_data.append(enriched_row)
            else:
                uncached_coords.append((lat, lng))

        logger.info(f"Cache hits: {len(cached_data)}, Cache misses: {len(uncached_coords)}")

        # Process uncached coordinates
        newly_geocoded = []
        for lat, lng in uncached_coords:
            geocoding_data = self._geocode_coordinates(lat, lng)
            if geocoding_data:
                self._cache_location(lat, lng, geocoding_data)
                enriched_row = self._extract_enrichment_data(lat, lng, geocoding_data, enrich_columns)
                newly_geocoded.append(enriched_row)

        # Save updated cache
        if newly_geocoded:
            self._save_cache()

        # Combine all enrichment data
        all_enrichment_data = cached_data + newly_geocoded

        if not all_enrichment_data:
            logger.warning("No geocoding data available for any coordinates")
            return df

        # Create enrichment DataFrame
        enrichment_df = pl.DataFrame(all_enrichment_data)

        # Join with original DataFrame
        enriched_df = df.join(enrichment_df, on=[lat_col, lng_col], how="left", validate="m:1")

        logger.info(f"Successfully enriched {len(enriched_df.filter(pl.col('formatted_address').is_not_null()))} rows")

        return enriched_df

    def _extract_enrichment_data(
        self, lat: float, lng: float, geocoding_data: dict[str, Any], enrich_columns: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Extract specific columns from geocoding API response.

        Parses the Google Maps API response and extracts requested fields
        into a flat dictionary structure suitable for DataFrame operations.

        Parameters
        ----------
        lat : float
            Original latitude coordinate
        lng : float
            Original longitude coordinate
        geocoding_data : Dict[str, Any]
            Complete geocoding response from Google API
        enrich_columns : Optional[List[str]], optional
            Specific fields to extract. If None, uses default set, by default None

        Returns
        -------
        Dict[str, Any]
            Flattened dictionary with requested geocoding fields

        Examples
        --------
        >>> data = gps_cache._extract_enrichment_data(
        ...     37.7749, -122.4194, api_response,
        ...     ["formatted_address", "country"]
        ... )
        >>> print(data.keys())
        dict_keys(['latitude', 'longitude', 'formatted_address', 'country'])
        """
        default_columns = [
            "formatted_address",
            "country",
            "administrative_area_level_1",  # State/Province
            "administrative_area_level_2",  # County
            "locality",  # City
            "postal_code",
            "place_id",
        ]

        columns_to_extract = enrich_columns or default_columns

        # Base data
        enriched_geo_info = {
            "latitude": lat,
            "longitude": lng,
            "formatted_address": geocoding_data.get("formatted_address"),
            "place_id": geocoding_data.get("place_id"),
        }

        # Extract address components
        address_components = geocoding_data.get("address_components", [])
        component_dict = {}

        for component in address_components:
            for component_type in component.get("types", []):
                component_dict[component_type] = {
                    "long_name": component.get("long_name"),
                    "short_name": component.get("short_name"),
                }

        # Add requested columns
        for col in columns_to_extract:
            if col in component_dict:
                enriched_geo_info[col] = component_dict[col]["long_name"]
                enriched_geo_info[f"{col}_short"] = component_dict[col]["short_name"]
            elif col not in enriched_geo_info:
                enriched_geo_info[col] = None

        return enriched_geo_info

    def get_cache_stats(self) -> dict[str, Any]:
        """
        Get comprehensive cache statistics and metrics.

        Returns detailed information about cache performance, including
        entry counts, expiration status, and cache configuration.

        Returns
        -------
        Dict[str, Any]
            Dictionary containing cache statistics with keys:
            - total_entries: Total number of cached locations
            - valid_entries: Number of non-expired entries
            - expired_entries: Number of expired entries
            - cache_precision: Configured geohash precision level
            - ttl_days: Configured TTL in days

        Examples
        --------
        >>> stats = gps_cache.get_cache_stats()
        >>> print(f"Cache hit rate: {stats['valid_entries']}/{stats['total_entries']}")
        Cache hit rate: 1250/1300
        """
        if not self._cache_loaded:
            self._load_cache()

        total_entries = len(self._cache)
        current_time = datetime.now(UTC)

        expired_count = sum(
            1 for location in self._cache.values() if datetime.fromisoformat(location.expires_at) <= current_time
        )

        valid_count = total_entries - expired_count

        return {
            "total_entries": total_entries,
            "valid_entries": valid_count,
            "expired_entries": expired_count,
            "cache_precision": self.cache_precision,
            "ttl_days": self.cache_ttl_days,
        }

    def clear_expired_cache(self) -> int:
        """
        Remove expired entries from cache and update S3.

        Scans the cache for entries past their TTL and removes them,
        then saves the updated cache to S3 for persistence.

        Returns
        -------
        int
            Number of expired entries removed

        Raises
        ------
        Exception
            If S3 save operation fails after removing expired entries

        Examples
        --------
        >>> expired_count = gps_cache.clear_expired_cache()
        >>> print(f"Removed {expired_count} expired entries")
        Removed 50 expired entries
        """
        if not self._cache_loaded:
            self._load_cache()

        current_time = datetime.now(UTC)
        expired_geohashes = [
            geohash
            for geohash, location in self._cache.items()
            if datetime.fromisoformat(location.expires_at) <= current_time
        ]

        for geohash in expired_geohashes:
            del self._cache[geohash]

        if expired_geohashes:
            self._save_cache()
            logger.info(f"Removed {len(expired_geohashes)} expired cache entries")

        return len(expired_geohashes)

    def get_coordinates(self, address: str) -> tuple[float, float] | None:
        """
        Get latitude and longitude coordinates for an address.

        Performs forward geocoding to convert an address string into coordinates.
        This method bypasses the cache and makes direct API calls.

        Parameters
        ----------
        address : str
            Address string to geocode

        Returns
        -------
        Optional[Tuple[float, float]]
            Tuple of (latitude, longitude) if found, None if geocoding fails

        Raises
        ------
        Exception
            If Google Maps API request fails

        Examples
        --------
        >>> coords = gps_cache.get_coordinates("Empire State Building, NYC")
        >>> if coords:
        ...     lat, lng = coords
        ...     print(f"Coordinates: {lat}, {lng}")
        Coordinates: 40.748441, -73.985664
        """
        try:
            results = self._google_api_client.geocode(address)
            if results:
                location = results[0]["geometry"]["location"]
                return (location["lat"], location["lng"])
            return None
        except Exception as e:
            logger.error(f"Error geocoding address '{address}': {e}")
            raise

    def get_address(self, lat: float, lng: float) -> str | None:
        """
        Get formatted address for coordinates using cache when possible.

        Performs reverse geocoding with cache checking to convert coordinates
        into a human-readable address string.

        Parameters
        ----------
        lat : float
            Latitude coordinate
        lng : float
            Longitude coordinate

        Returns
        -------
        Optional[str]
            Formatted address string if found, None if geocoding fails

        Raises
        ------
        ValueError
            If coordinates are invalid
        Exception
            If Google Maps API request fails

        Examples
        --------
        >>> address = gps_cache.get_address(40.748441, -73.985664)
        >>> print(f"Address: {address}")
        Address: 350 5th Ave, New York, NY 10118, USA
        """
        # Check cache first
        cached_location = self._check_cache(lat, lng)
        if cached_location:
            return cached_location.geocoding_data.get("formatted_address")

        # Make API call and cache result
        geocoding_data = self._geocode_coordinates(lat, lng)
        if geocoding_data:
            self._cache_location(lat, lng, geocoding_data)
            self._save_cache()
            return geocoding_data.get("formatted_address")

        return None


# Pre-configured resource instances for different environments
gps_caching_resource = GeoEncoderResource(
    google_maps_api_key=dg.EnvVar("GOOGLE_MAPS_API_KEY"),
    s3_bucket=dg.EnvVar("GPS_CACHE_S3_BUCKET"),
    s3_cache_location="gps_cache/geocoding_cache.json",
    cache_precision=6,  # ~500m radius coverage
    cache_ttl_days=30,  # Google API compliance
    queries_per_second=40,  # Safe rate limiting
)
