# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any
import base64
import json


class BytesEncoder(json.JSONEncoder):
    """
    Custom JSON encoder to handle bytes objects.
    Encodes bytes as base64 strings to ensure they can be safely serialized.
    """

    def default(self, o):
        try:
            if isinstance(o, bytes):
                # Encode bytes using base64 and decode to ASCII for JSON serialization
                return base64.b64encode(o).decode('ascii')
            else:
                # For other types, use the default JSON encoding
                return super().default(o)
        except Exception as e:
            raise e


def dumps(obj: Any) -> str:
    """
    Serialize an object to a JSON-formatted string using the custom BytesEncoder.

    Args:
        obj (Any): The Python object to be serialized.

    Returns:
        str: The JSON-formatted string representation of the object.
    """
    return json.dumps(obj, cls=BytesEncoder)
