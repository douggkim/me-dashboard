"""Custom Encoder for serializing dictionaries to JSON format.

This encoder extends the standard JSONEncoder to handle additional Python
types that are not natively supported by the JSON format, including datetime
objects, timedeltas, and Enum values.
"""

import datetime
import enum
import json
from typing import Any

from loguru import logger


class CustomerJSONEncoder(json.JSONEncoder):
    """
    JSON encoder that handles common non-serializable Python types.

    This encoder extends the standard JSONEncoder to handle datetime objects,
    timedeltas, and Enum values by converting them to JSON-serializable formats.
    Unrecognized types are logged and converted to string representations.

    Attributes
    ----------
    _date_data_types : Tuple(Any)
        Types of date-related objects to be serialized as ISO format strings

    Examples
    --------
    >>> data = {"timestamp": datetime.datetime.now(), "category": SomeEnum.VALUE}
    >>> json_str = json.dumps(data, cls=CustomerJSONEncoder)
    """

    _date_data_types = (datetime.datetime, datetime.date)

    def default(self, obj: Any) -> Any:
        """
        Convert Python objects to JSON-serializable types.

        This method is called by the JSONEncoder when it encounters a type
        it doesn't natively know how to serialize. It handles several common
        types and provides a fallback for unrecognized types.

        Parameters
        ----------
        obj : Any
            The Python object to convert to a JSON-serializable type

        Returns
        -------
        Any
            A JSON-serializable representation of the input object

        Notes
        -----
        Handled types:
        - datetime.datetime and datetime.date: Converted to ISO format strings
        - datetime.timedelta: Converted to total seconds (float)
        - enum.Enum: Converted to its value
        - Other types: Logged and converted to string representation
        """
        # Handle known types
        if isinstance(obj, self._date_data_types):
            return obj.isoformat()
        if isinstance(obj, datetime.timedelta):
            return obj.total_seconds()
        if isinstance(obj, enum.Enum):
            return obj.value

        # For unknown types, log and return string representation
        logger.debug(f"Encountered non-serializable type: {type(obj).__name__}")
        return f"<{type(obj).__name__}>"  # Or return None, or str(obj)
