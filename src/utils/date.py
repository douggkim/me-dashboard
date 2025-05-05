"""Date Utility functions."""

import datetime


def datetime_to_epoch_ms(dt_object: datetime) -> int:
    """
    Convert a datetime object to milliseconds since epoch (Unix timestamp).

    Parameters
    ----------
    dt_object : datetime
        A datetime object to convert.

    Returns
    -------
    int
        The datetime as milliseconds since epoch.
    """
    return int(dt_object.timestamp() * 1000)
