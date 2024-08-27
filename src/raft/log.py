# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import IntEnum
from threading import RLock
from typing import Any, Optional
import base64
import queue

from . import util


class ExceptionEntryIndexMismatch(Exception):
    '''
    Exception raised when there is a mismatch between the expected and actual
    log entry index.
    '''

    def __init__(self, expected_index: int, actual_index: int) -> None:
        self.expected_index = expected_index
        self.actual_index = actual_index
        super().__init__(
            f"Entry index mismatch: expected {expected_index}, got {actual_index}")


class LogType(IntEnum):
    '''Defines the types of log entries used in the Raft consensus algorithm.'''

    COMMAND = 1
    '''A log entry that contains a command to be applied to the state machine.'''

    NOOP = 2
    '''A log entry used to assert the leader's status without changing the state.'''

    CONFIGURATION = 3
    '''
    A log entry related to cluster membership changes, such as adding or 
    removing nodes.
    '''


@dataclass
class Log:
    '''Represents a log entry in the Raft consensus system.'''

    # The type of log entry (e.g., COMMAND, CONFIGURATION).
    type: LogType = field(default=LogType.COMMAND)
    # The binary data associated with the log entry.
    data: bytes = None
    # The term during which the log was created.
    term: int = field(default=-1)
    # The index of the log entry, defaults to -1 as a placeholder.
    index: int = field(default=-1)

    # The result of applying of this log, set by the leader.
    _result: queue.Queue = None

    def to_dict(self) -> dict[str, Any]:
        '''Converts the Log instance to a dictionary.'''
        return {
            'index': self.index,
            'type': int(self.type),
            'data': self.data,
            'term': self.term
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'Log':
        '''Creates a Log instance from a dictionary.'''
        decoded_data: bytes = base64.b64decode(data['data']) if isinstance(
            data['data'], str) else data['data']
        return Log(
            type=LogType(data['type']),
            data=decoded_data,
            term=data['term'],
            index=data['index']
        )
    
    def _get_result(self) -> queue.Queue:
        if self._result is None:
            self._result = queue.Queue(maxsize=1)

        return self._result


class LogStorage(ABC):
    '''Abstract base class for log storage. Defines the interface for log operations.'''

    @abstractmethod
    def append(self, *entries: Log) -> None:
        '''Appends a log entry to storage.'''
        pass

    @abstractmethod
    def get(self, index: int, default: Optional[Log] = None) -> Optional[Log]:
        '''Retrieves the log entry at the specified index. If no log entries 
        exist, returns the provided default value. If no default is provided, 
        returns None.'''
        pass

    @abstractmethod
    def get_range(self, start: int, end: int) -> list[Log]:
        '''Retrieves a range of log entries from start (inclusive) to end (exclusive).'''
        pass

    @abstractmethod
    def last_index(self) -> int:
        '''Returns the index of the last log entry, or -1 if no logs exist.'''
        pass

    @abstractmethod
    def first_index(self) -> int:
        '''Returns the index of the last log entry, or -1 if no logs exist.'''
        pass

    @abstractmethod
    def last(self, default: Optional[Log] = None) -> Optional[Log]:
        '''
        Returns the last log entry. If no log entries exist, returns the 
        provided default value. If no default is provided, returns None.
        '''
        pass

    @abstractmethod
    def first(self) -> Optional[Log]:
        '''Returns the first log entry.'''
        pass

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    @abstractmethod
    def trim(self, start: int) -> None:
        '''
        Trims the log entries starting from the specified index (inclusive).
        Supports negative indices for trimming logs from the end.
        '''
        pass


class SimpleFileLogStorage(LogStorage, util.MsgpackFileStorage):
    '''A simple file-based implementation of LogStorage.'''

    def __init__(self, filename: str) -> None:
        # Initialize an empty list to store logs in memory.
        self._logs: list[Log] = []
        # Thread locks to manage concurrent access.
        self._data_lock = RLock()

        util.MsgpackFileStorage.__init__(self, filename)
        self._load_logs()

    def append(self, *entries: Log) -> None:
        '''Appends a log entry to the storage and persists it to disk.'''
        with self._data_lock:
            first_index = 0 if not self._logs else self._logs[0].index
            for entry in entries:
                self._logs.append(entry)
                entry_index = first_index + len(self._logs) - 1
                if entry.index == -1:
                    entry.index = entry_index
                elif entry.index != entry_index:
                    raise ExceptionEntryIndexMismatch(entry_index, entry.index)
            self._persist_logs()

    def get(self, index: int, default: Optional[Log] = None) -> Optional[Log]:
        '''Retrieves the log entry at the specified index. If no log entries 
        exist, returns the provided default value. If no default is provided, 
        returns None.'''
        with self._data_lock:
            if 0 <= index < len(self._logs):
                return self._logs[index]
            return default

    def get_range(self, start: int, end: int) -> list[Log]:
        '''Retrieves a range of log entries from start (inclusive) to end (exclusive).'''
        with self._data_lock:
            return self._logs[start:end]

    def last_index(self) -> int:
        '''Returns the index of the last log entry, or -1 if no logs exist.'''
        with self._data_lock:
            if self._logs:
                return self._logs[-1].index
            return -1

    def first_index(self) -> int:
        '''Returns the index of the first log entry, or -1 if no logs exist.'''
        with self._data_lock:
            if self._logs:
                return self._logs[0].index
            return -1

    def last(self, default: Optional[Log] = None) -> Optional[Log]:
        '''
        Returns the last log entry. If no log entries exist, returns the 
        provided default value. If no default is provided, returns None.
        '''
        with self._data_lock:
            if self._logs:
                return self._logs[-1]
            return default

    def first(self) -> Optional[Log]:
        '''Returns the first log entry, or None if no logs exist.'''
        with self._data_lock:
            if self._logs:
                return self._logs[0]
            return None

    def is_empty(self) -> bool:
        return False if self._logs else True

    def trim(self, start: int) -> None:
        '''Trims the log entries starting from the specified index (inclusive).'''
        with self._data_lock:
            start = start if start >= 0 else len(self._logs) + start
            start = max(0, min(start, len(self._logs)))
            self._logs = self._logs[:start]
            self._persist_logs()

    def _persist_logs(self) -> None:
        '''Serializes and writes logs to disk in a persistent format.'''
        self._persist_to_file([log.to_dict() for log in self._logs])

    def _load_logs(self) -> None:
        '''Loads logs from disk into memory.'''
        raw_logs = self._load_from_file()
        self._logs = [Log.from_dict(log_dict)
                      for log_dict in raw_logs] if raw_logs else []
