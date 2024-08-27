# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import ABC, abstractmethod
from typing import Optional
from threading import RLock

from . import util


class StableStorage(ABC):
    '''Abstract base class for stable storage.'''

    @abstractmethod
    def get(self, key: str, default: Optional[bytes] = None) -> Optional[bytes]:
        '''Retrieves the value associated with the given key, or returns default 
        if the key is not found.

        Args:
            key (str): The key to retrieve the value.
            default (Optional[bytes]): The default value to return if the key is 
                                       not found.

        Returns:
            Optional[bytes]: The value associated with the given key, or the 
                             default value.
        '''
        pass

    @abstractmethod
    def set(self, key: str, value: bytes) -> None:
        '''Sets the value for the given key.

        Args:
            key (str): The key to set the value for.
            val (bytes): The value to set for the key.
        '''
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        '''Deletes a kv by give key.

        Args:
            key (str): The key to delete.
        '''
        pass

    @abstractmethod
    def get_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        '''Retrieves the integer value associated with the given key, or returns
        default if the key is not found.

        Args:
            key (str): The key to retrieve the value.
            default (Optional[int]): The default value to return if the key is 
                                     not found.

        Returns:
            Optional[int]: The integer value associated with the given key, or 
                           the default value.
        '''
        pass

    @abstractmethod
    def set_int(self, key: str, value: int) -> None:
        '''Sets the integer value for the given key.

        Args:
            key (str): The key to set the value for.
            val (bytes): The value to set for the key.
        '''
        pass


class SimpleFileStableStorage(StableStorage, util.MsgpackFileStorage):
    '''A simple implementation of StableStorage'''

    def __init__(self, filename: str) -> None:
        self._data: dict[str, bytes] = {}
        self._data_lock = RLock()

        util.MsgpackFileStorage.__init__(self, filename)
        self._load_data()

    def get(self, key: str, default: Optional[bytes] = None) -> Optional[bytes]:
        '''Retrieves the value associated with the given key, or returns default 
        if the key is not found.

        Args:
            key (str): The key to retrieve the value.
            default (Optional[bytes]): The default value to return if the key is 
                                       not found.

        Returns:
            Optional[bytes]: The value associated with the given key, or the 
                             default value.
        '''
        with self._data_lock:
            return self._data.get(key, default)

    def set(self, key: str, value: bytes) -> None:
        '''Sets the value for the given key.

        Args:
            key (str): The key to set the value for.
            value (bytes): The value to set for the key.
        '''
        with self._data_lock:
            self._data[key] = value

        self._persist_data()

    def delete(self, key: str) -> bool:
        '''Deletes a key-value pair by the given key.

        Args:
            key (str): The key to delete.

        Returns:
            bool: True if the key was found and deleted, False otherwise.
        '''
        found = False
        with self._data_lock:
            found = key in self._data
            if found:
                del self._data[key]

        if found:
            self._persist_data()

        return found

    def get_int(self, key: str, default: Optional[int] = None) -> Optional[int]:
        '''Retrieves the integer value associated with the given key, or returns
        default if the key is not found.

        Args:
            key (str): The key to retrieve the value.
            default (Optional[int]): The default value to return if the key is 
                                     not found.

        Returns:
            Optional[int]: The integer value associated with the given key, or 
                           the default value.
        '''
        value_bytes = self.get(key=key)
        if value_bytes is None:
            return default

        return int.from_bytes(bytes=value_bytes, byteorder='big')

    def set_int(self, key: str, value: int) -> None:
        '''Sets the integer value for the given key.

        Args:
            key (str): The key to set the value for.
            value (int): The integer value to set for the key.
        '''
        self.set(key=key, value=value.to_bytes(length=8, byteorder='big'))

    def _persist_data(self) -> None:
        '''Persist the current in-memory data to the file.'''
        self._persist_to_file(self._data)

    def _load_data(self) -> None:
        '''Load the data from the file into memory.'''
        data = self._load_from_file()
        self._data = data if data else {}
