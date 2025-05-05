"""Helper functions that are hard to be categorized into certain utils function."""

import inspect
from typing import Any

from loguru import logger


def filter_kwargs(func: callable, kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Filter keyword arguments to only include those accepted by the target function.

    This function inspects the signature of the provided callable and returns a new
    dictionary containing only the keyword arguments that are valid parameters for
    the target function. Invalid arguments are logged as warnings.

    Parameters
    ----------
    func : callable
        The function whose signature will be used to filter the keyword arguments.
    kwargs : dict[str, Any]
        A dictionary of keyword arguments to be filtered.

    Returns
    -------
    dict[str, Any]
        A new dictionary containing only the keyword arguments that are valid
        parameters for the target function.

    Notes
    -----
    Invalid keyword arguments are logged as warnings using the logger object,
    but do not raise exceptions.

    Examples
    --------
    >>> def example_func(a, b, c=3):
    ...     return a + b + c
    >>> filter_kwargs(example_func, {'a': 1, 'b': 2, 'd': 4})
    {'a': 1, 'b': 2}
    """
    sig = inspect.signature(func)
    valid_keys = set(sig.parameters.keys())
    filtered = {}
    for k, v in kwargs.items():
        if k in valid_keys:
            filtered[k] = v
        else:
            logger.warning(
                f"Keyword argument '{k} is not valid for function '{func.__name__}'. This argument will be ignored"
            )
    return filtered
