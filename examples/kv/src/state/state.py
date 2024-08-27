from dataclasses import dataclass, field
from typing import Any, Optional
from rocksdict import Rdict
from enum import IntEnum
import json


import raft


class CommandType(IntEnum):
    """
    Enum representing different types of commands that can be applied by the FSM.
    """
    UNKNOWN = 0
    '''Represents an unknown or invalid command type.'''
    GET = 1
    '''Command to retrieve the value associated with a specific key.'''
    SET = 2
    '''Command to associate a value with a specific key.'''
    DEL = 3
    '''Command to delete a key-value pair.'''


@dataclass
class Command:
    # The type of command, represented by the CommandType enum.
    type: CommandType
    # The key associated with the command, typically a string.
    key: str
    # The value associated with the command, which is optional and only relevant
    #  for SET commands.
    value: Optional[str] = field(default=None)

    def to_dict(self) -> dict[str, Any]:
        """Converts the Command instance to a dictionary."""
        return {
            'type': self.type,
            'key': self.key,
            'value': self.value,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'Command':
        """Creates a Command instance from a dictionary."""
        return Command(
            type=CommandType(data['type']),
            key=data['key'],
            value=data.get('value')
        )

    def to_bytes(self) -> bytes:
        """Serializes the command to bytes using JSON."""
        return json.dumps(self.to_dict()).encode('utf-8')

    @staticmethod
    def from_bytes(data: bytes) -> 'Command':
        """Deserializes bytes into a Command object."""
        if not data:
            return Command(CommandType.UNKNOWN, '')
        command_data = json.loads(data.decode('utf-8'))
        return Command.from_dict(command_data)


@dataclass
class CommandResult:
    """
    Data class representing the result of a command executed by the FSM.
    """
    # A string indicating the outcome of the command execution (e.g., 'OK',
    # 'NOT_FOUND')
    result: str = ''
    # The value associated with the result, which is relevant for commands like
    # GET where the result may include a retrieved value.
    value: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        """Converts the CommandResult instance to a dictionary."""
        return {
            'result': self.result,
            'value': self.value,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'CommandResult':
        """Creates a CommandResult instance from a dictionary."""
        return CommandResult(
            result=data['result'],
            value=data.get('value')
        )

    def to_bytes(self) -> bytes:
        """Serializes the CommandResult to bytes using JSON."""
        return json.dumps(self.to_dict()).encode('utf-8')

    @staticmethod
    def from_bytes(data: bytes) -> 'CommandResult':
        """Deserializes bytes into a CommandResult object."""
        if not data:
            return CommandResult(result='', value=None)
        command_result_data = json.loads(data.decode('utf-8'))
        return CommandResult.from_dict(command_result_data)


class FSM(raft.FSM):
    _KEY_LAST_APPLIED_INDEX = 'last_applied_index'

    def __init__(self, db: Rdict) -> None:
        """
        Initializes the FSM with a database instance and sets the last applied 
        index if it doesn't already exist.
        """
        self._db = db
        if self._KEY_LAST_APPLIED_INDEX not in self._db:
            self._db[self._KEY_LAST_APPLIED_INDEX] = -1

    def apply(self, entry: raft.Log) -> Optional[bytes]:
        """
        Applies a log entry to the FSM.
        Processes GET, SET, and DEL commands based on the log entry.
        """
        if entry.type != raft.LogType.COMMAND:
            return None

        if entry.index <= self._db[self._KEY_LAST_APPLIED_INDEX]:
            return None

        command = Command.from_bytes(entry.data)
        command_result = CommandResult()

        if command.type == CommandType.GET:
            command_result = self._apply_get(command)
        elif command.type == CommandType.SET:
            command_result = self._apply_set(command)
        elif command.type == CommandType.DEL:
            command_result = self._apply_del(command)

        self._db[self._KEY_LAST_APPLIED_INDEX] = entry.index
        return command_result.to_bytes()

    def _apply_get(self, command: Command) -> CommandResult:
        """Handles the GET command."""
        if command.key not in self._db:
            return CommandResult(result='NOT_FOUND', value=None)
        return CommandResult(result='OK', value=self._db[command.key])

    def _apply_set(self, command: Command) -> CommandResult:
        """Handles the SET command."""
        self._db[command.key] = command.value
        return CommandResult(result='OK')

    def _apply_del(self, command: Command) -> CommandResult:
        """Handles the DEL command."""
        if command.key not in self._db:
            return CommandResult(result='NOT_FOUND', value=None)
        del self._db[command.key]
        return CommandResult(result='OK')

    def last_applied_index(self) -> int:
        """
        Returns the last applied index.
        This method can be enhanced further depending on your use case.
        """
        return self._db[self._KEY_LAST_APPLIED_INDEX]
