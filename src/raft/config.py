# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import abstractmethod
from dataclasses import dataclass
from enum import IntEnum
from typing import Optional
import msgpack
import math

from . import fsm, util, log


class ServerSuffrage(IntEnum):
    '''Determines whether a Server in a Configuration gets a vote.'''

    VOTER = 1
    '''A server whose vote is counted in elections and whose match index is used
    in advancing the leader's commit index.'''

    NON_VOTER = 2
    '''A server that receives log entries but is not considered for elections or 
    commitment purposes.'''


@dataclass
class Server:
    '''Server holds the information about a single server in a configuration.'''

    id: int
    suffrage: ServerSuffrage
    config: bytes

    def to_dict(self) -> dict:
        '''Converts the configuration to a dictionary format for serialization.'''
        return {
            'id': self.id,
            'suffrage': self.suffrage,
            'config': self.config
        }

    @staticmethod
    def from_dict(data: dict) -> 'Server':
        '''Creates a Server object from a dictionary.'''
        return Server(
            id=data['id'],
            suffrage=data['suffrage'],
            config=data['config']
        )

    def is_voter(self) -> bool:
        return self.suffrage == ServerSuffrage.VOTER


class ServerAlreadyExistsException(Exception):
    '''Exception raised when attempting to add a server with an Id that already exists in the configuration.'''

    def __init__(self, server_id: int):
        # Initialize the exception with a message indicating the duplicate server Id
        super().__init__(f'Server with id: {server_id} already exists')
        self.server_id = server_id


class Configuration:
    '''Manages the configuration of servers in a Raft cluster.'''

    def __init__(self, servers: dict[int, Server] = None) -> None:
        # Stores the servers as a dictionary with server Id as the key and Server object as the value
        self._servers = servers if servers is not None else {}

    def get(self, server_id: int) -> Optional[Server]:
        '''Retrieves a server by its Id.'''
        return self._servers.get(server_id)

    def contains(self, server_id: int) -> bool:
        '''Checks if a server with the given Id exists in the configuration.'''
        return server_id in self._servers

    def add(self, server: Server) -> 'Configuration':
        '''Returns a new Configuration with an added server.

        Raises:
            ServerAlreadyExistsException: If a server with the same Id already 
            exists.
        '''
        if self.contains(server.id):
            raise ServerAlreadyExistsException(server.id)
        new_servers = dict(self._servers)  # Create a copy to modify
        new_servers[server.id] = server
        return Configuration(new_servers)

    def remove(self, server_id: int) -> 'Configuration':
        '''Returns a new Configuration with the specified server removed.

        Args:
            server_id (int): The Id of the server to remove.

        Returns:
            Configuration: The new configuration without the specified server.
        '''
        if not self.is_exist(server_id):
            raise KeyError(f'Server with id: {server_id} does not exist')
        new_servers = dict(self._servers)  # Create a copy to modify
        del new_servers[server_id]
        return Configuration(new_servers)

    def clone(self) -> 'Configuration':
        '''Creates a new Configuration with the same servers.'''
        return Configuration(dict(self._servers))

    def has_non_voter(self) -> bool:
        '''Checks if there are any non-voter servers in the configuration.'''
        return any(server.suffrage == ServerSuffrage.NON_VOTER for server in self._servers.values())

    def upgrade_all_to_voter(self) -> 'Configuration':
        '''Returns a new Configuration with all servers upgraded to voters.'''
        new_servers = {
            id: Server(server.id, ServerSuffrage.VOTER, server.config)
            for id, server in self._servers.items()
        }
        return Configuration(new_servers)

    def quorum(self, excluding_non_voter: bool = False) -> int:
        '''Calculates the quorum size for the current configuration.

        Quorum is the minimum number of servers required to make decisions. 
        Optionally excludes non-voter servers.

        Args:
            excluding_non_voter (bool): Whether to exclude non-voter servers 
            from quorum calculation.

        Returns:
            int: The calculated quorum size.
        '''
        if excluding_non_voter:
            voters = [s for s in self._servers.values() if s.suffrage == ServerSuffrage.VOTER]
            return math.ceil((len(voters) + 1) / 2)
        
        return math.ceil((len(self._servers) + 1) / 2)

    @property
    def servers(self) -> dict[int, Server]:
        return self._servers

    @property
    def voters(self) -> dict[int, Server]:
        voters: dict[int, Server] = {}
        for server in self._servers.values():
            if server.is_voter():
                voters[server.id] = server

        return voters

    def to_dict(self) -> dict:
        '''Converts the configuration to a dictionary format for serialization.'''
        return {
            'servers': {id: server.to_dict() for id, server in self._servers.items()}
        }

    @staticmethod
    def from_dict(data: dict) -> 'Configuration':
        '''Creates a Configuration object from a dictionary.'''
        servers = {id: Server.from_dict(server_data)
                   for id, server_data in data['servers'].items()}
        return Configuration(servers)

    def to_bytes(self) -> bytes:
        '''Serializes the configuration to bytes using msgpack.'''
        return msgpack.packb(self.to_dict())

    @staticmethod
    def from_bytes(data: bytes) -> 'Configuration':
        '''Deserializes bytes into a Configuration object.'''
        if not data:
            return Configuration()
        config_data = msgpack.unpackb(data, strict_map_key=False)
        return Configuration.from_dict(config_data)


class ConfigurationStorage(fsm.FSM):
    @abstractmethod
    def last_configuration(self) -> Optional[Configuration]:
        '''Retrieves the last configuration that is applied to FSM.

        Returns:
            Optional[Configuration]: The last configuration.
        '''
        pass


class SimpleFileConfigurationStorage(ConfigurationStorage, util.MsgpackFileStorage):
    _KEY_LAST_APPLIED_INDEX = 'last_applied_index'
    _KEY_CONFIG = 'config'

    def __init__(self, filename: str, bootstrap_config: Optional[Configuration]) -> None:
        self._config: Optional[Configuration] = bootstrap_config
        self._last_applied_index: int = -1

        util.MsgpackFileStorage.__init__(self, filename)
        self._load_data()

    def apply(self, entry: log.Log) -> Optional[bytes]:
        '''Applies an log entry to the system's state or processing logic.

        Args:
            entry (log.Log): The log entry to be applied, which holds a 
                             command or data to be processed.
        '''
        if self._last_applied_index and entry.index <= self._last_applied_index:
            return

        if entry.type == log.LogType.CONFIGURATION:
            self._config = Configuration.from_bytes(entry.data)
        self._last_applied_index = entry.index

        self._persist_data()
        return None

    def last_applied_index(self) -> int:
        '''Retrieves the index of the most recently applied log entry to the 
        system's state or processing logic.

        Returns:
            int: The index of the last applied log entry, or -1 if no log entry 
                 has been applied yet.
        '''
        return self._last_applied_index

    def last_configuration(self) -> Optional[Configuration]:
        '''Retrieves the last configuration that is applied to FSM.

        Returns:
            Optional[Configuration]: The last configuration.
        '''
        return self._config

    def _persist_data(self) -> None:
        '''Persist the current in-memory data to the file.'''
        self._persist_to_file({
            self._KEY_CONFIG: self._config.to_bytes(),
            self._KEY_LAST_APPLIED_INDEX: self._last_applied_index,
        })

    def _load_data(self) -> None:
        '''Load the data from the file into memory.'''
        data = self._load_from_file()
        if not data:
            self._persist_data()
            return

        if self._KEY_CONFIG in data:
            self._config = Configuration.from_bytes(data[self._KEY_CONFIG])
        if self._KEY_LAST_APPLIED_INDEX in data and data[self._KEY_LAST_APPLIED_INDEX]:
            self._last_applied_index = int(data[self._KEY_LAST_APPLIED_INDEX])
