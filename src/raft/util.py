# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from typing import Any
import os
import threading
import msgpack
import json

from .logging import logger
from .json import BytesEncoder


class SingletonMeta(type):
    """
    A thread-safe implementation of the Singleton pattern using metaclasses.

    Ensures that only one instance of a class is created, even when accessed from
    multiple threads. The instance is stored in the `_instances` dictionary and 
    protected by the `_lock` to avoid race conditions.
    """
    _instances = {}
    # Ensures thread-safe access to the Singleton instance.
    _lock = threading.RLock()

    def __call__(cls, *args, **kwargs):
        """
        Controls the instantiation process of the Singleton class.
        If an instance already exists, return it. Otherwise, create a new one.
        """
        with cls._lock:
            if cls not in cls._instances:
                # If instance doesn't exist, create it and store it in _instances.
                instance = super().__call__(*args, **kwargs)
                cls._instances[cls] = instance
            # Return the existing or newly created instance.
            return cls._instances[cls]


def ensure_dir(dir: str):
    """
    Ensure that the specified directory exists. If it doesn't, create it.

    Args:
        dir (str): The path of the directory to check or create.

    Returns:
        None
    """
    if not os.path.exists(dir):
        # Create the directory if it does not exist.
        os.makedirs(dir)
        logger.info(f'Created directory: {dir}')


def write_msgpack(data: Any, filename: str) -> None:
    '''Serialize data to MessagePack format and write it to a file.

    Args:
        data (Any): The data to be serialized and stored.
        filename (str): The path and name of the file where the data should be
                        stored.
    '''
    data_bytes = msgpack.packb(data)
    with open(filename, 'wb') as file:
        file.write(data_bytes)


def read_msgpack(filename: str) -> Any:
    '''Read and deserialize data from a MessagePack file.

    Args:
        filename (str): The path and name of the file to read.

    Returns:
        Any: The deserialized data from the file, or None if the file is missing
             or the data is corrupted.
    '''
    try:
        ensure_dir(os.path.dirname(filename))
        with open(filename, 'r+b') as file:
            data_bytes = file.read()
            if type(data_bytes) is str:
                data_bytes = data_bytes.encode()
            return msgpack.unpackb(data_bytes)
    except FileNotFoundError:
        logger.warning(f'File not found: {filename}')
    except (msgpack.UnpackException, ValueError) as e:
        logger.error(f'Error unpacking data: {e}')
    return None


class MsgpackFileStorage:
    '''A base class for file-based storage implementations.'''

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._file_lock = threading.RLock()

    def _persist_to_file(self, data: Any) -> None:
        '''Persist the given data to the file.'''
        with self._file_lock:
            write_msgpack(data, self._filename)

    def _load_from_file(self) -> Any:
        '''Load data from the file.'''
        with self._file_lock:
            return read_msgpack(self._filename)


def write_json(data: Any, filename: str) -> None:
    '''Serialize data to JSON format and write it to a file.

    Args:
        data (Any): The data to be serialized and stored.
        filename (str): The path and name of the file where the data should be stored.
    '''
    ensure_dir(os.path.dirname(filename))
    with open(filename, 'w') as file:
        json.dump(data, file, cls=BytesEncoder)


def read_json(filename: str) -> Any:
    '''Read and deserialize data from a JSON file.

    Args:
        filename (str): The path and name of the file to read.

    Returns:
        Any: The deserialized data from the file, or None if the file is missing
             or the data is corrupted.
    '''
    try:
        ensure_dir(os.path.dirname(filename))
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.warning(f'File not found: {filename}')
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f'Error decoding JSON data: {e}')
    return None


class JSONFileStorage:
    '''A base class for file-based storage implementations using JSON.'''

    def __init__(self, filename: str) -> None:
        self._filename = filename
        self._file_lock = threading.RLock()

    def _persist_to_file(self, data: Any) -> None:
        '''Persist the given data to the file.'''
        with self._file_lock:
            write_json(data, self._filename)

    def _load_from_file(self) -> Any:
        '''Load data from the file.'''
        with self._file_lock:
            return read_json(self._filename)
