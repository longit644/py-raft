# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import ABC, abstractmethod
from threading import Thread
from typing import Any, Optional
from dataclasses import dataclass, field

from .log import Log


@dataclass
class RequestVoteRequest:
    '''Represents a request message for the RequestVote RPC in the Raft 
       consensus protocol.
    '''

    candidate_id: int  # The Id of the candidate requesting the vote.
    candidate_term: int  # The term number of the candidate.
    last_log_index: int  # The index of the candidate's last log entry.
    last_log_term: int  # The term number of the candidate's last log entry.

    def to_dict(self) -> dict[str, Any]:
        '''Converts the RequestVoteRequest instance to a dictionary.'''
        return {
            'candidate_id': self.candidate_id,
            'candidate_term': self.candidate_term,
            'last_log_index': self.last_log_index,
            'last_log_term': self.last_log_term
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'RequestVoteRequest':
        '''Creates a RequestVoteRequest instance from a dictionary.'''
        return RequestVoteRequest(
            candidate_id=data['candidate_id'],
            candidate_term=data['candidate_term'],
            last_log_index=data['last_log_index'],
            last_log_term=data['last_log_term']
        )


@dataclass
class RequestVoteResponse:
    '''Represents a response message for the RequestVote RPC in the Raft 
       consensus protocol.
    '''

    # The id of the responding server.
    server_id: int
    # The current term of the responding server.
    term: int
    # Indicates whether the vote was granted to the candidate.
    vote_granted: bool

    def to_dict(self) -> dict[str, Any]:
        '''Converts the RequestVoteResponse instance to a dictionary.'''
        return {
            'server_id': self.server_id,
            'term': self.term,
            'vote_granted': self.vote_granted
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'RequestVoteResponse':
        '''Creates a RequestVoteResponse instance from a dictionary.'''
        return RequestVoteResponse(
            server_id=data['server_id'],
            term=data['term'],
            vote_granted=data['vote_granted']
        )


@dataclass
class AppendEntriesRequest:
    '''Represents a request message for the AppendEntries RPC in the Raft 
    consensus protocol.
    '''

    # The Id of the leader sending the request.
    leader_id: int
    # The current term of the leader.
    leader_term: int
    # The index of the log entry immediately preceding new ones.
    prev_log_index: int
    # The term of the prev_log_index entry.
    prev_log_term: int
    # A list of Log entries to store.
    entries: list[Log]
    # The leader's commit index.
    leader_commit: int

    def to_dict(self) -> dict[str, Any]:
        '''Converts the AppendEntriesRequest instance to a dictionary.'''
        return {
            'leader_id': self.leader_id,
            'leader_term': self.leader_term,
            'prev_log_index': self.prev_log_index,
            'prev_log_term': self.prev_log_term,
            'entries': [entry.to_dict() for entry in self.entries],
            'leader_commit': self.leader_commit
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'AppendEntriesRequest':
        '''Creates an AppendEntriesRequest instance from a dictionary.'''
        return AppendEntriesRequest(
            leader_id=data['leader_id'],
            leader_term=data['leader_term'],
            prev_log_index=data['prev_log_index'],
            prev_log_term=data['prev_log_term'],
            entries=[Log.from_dict(entry) for entry in data['entries']],
            leader_commit=data['leader_commit']
        )


@dataclass
class AppendEntriesResponse:
    '''Represents a response message for the AppendEntries RPC in the Raft 
    consensus protocol.
    '''

    # The id of the responding server.
    server_id: int
    # The current term of the responding server.
    term: int
    # Indicates whether the log entries were successfully appended.
    success: bool

    match_index: int

    def to_dict(self) -> dict[str, Any]:
        '''Converts the AppendEntriesResponse instance to a dictionary.'''
        return {
            'server_id': self.server_id,
            'term': self.term,
            'success': self.success,
            'match_index': self.match_index,
        }

    @staticmethod
    def from_dict(data: dict[str, Any]) -> 'AppendEntriesResponse':
        '''Creates an AppendEntriesResponse instance from a dictionary.'''
        return AppendEntriesResponse(
            server_id=data['server_id'],
            term=data['term'],
            success=data['success'],
            match_index=data['match_index']
        )


class Client(ABC):
    '''Abstract Base Class for a Raft client responsible for communication 
    between nodes.

    This class provides the interface for a client that sends RPC requests to 
    other nodes in the Raft cluster.
    '''

    @abstractmethod
    def request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        '''Send a RequestVote RPC to another node and return its response.

        Args:
            req (RequestVoteRequest): The request payload containing candidate 
                                      information.

        Returns:
            RequestVoteResponse: The response indicating whether the vote was 
                                 granted.
        '''
        pass

    @abstractmethod
    def append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        '''Send an AppendEntries RPC to another node and return its response.

        Args:
            req (AppendEntriesRequest): The request payload containing log 
                                        entries and leader information.

        Returns:
            AppendEntriesResponse: The response indicating success or failure.
        '''
        pass


class Handler(ABC):
    '''Abstract Base Class for handling incoming RPC requests in the Raft 
    protocol.

    This class defines the interface for processing RPC requests received by a 
    Raft node.
    '''

    @abstractmethod
    def handle_request_vote(self, req: RequestVoteRequest) -> RequestVoteResponse:
        '''Handle an incoming RequestVote RPC request.

        Args:
            req (RequestVoteRequest): The incoming request payload from another 
                                      node.

        Returns:
            RequestVoteResponse: The response indicating whether the vote was   
                                 granted.
        '''
        pass

    @abstractmethod
    def handle_append_entries(self, req: AppendEntriesRequest) -> AppendEntriesResponse:
        '''Handle an incoming AppendEntries RPC request.

        Args:
            req (AppendEntriesRequest): The incoming request payload from 
                                        another node.

        Returns:
            AppendEntriesResponse: The response indicating success or failure.
        '''
        pass


class Server(ABC):
    '''Abstract Base Class for a Raft server that manages node behavior and 
    communication.

    This class provides the interface for a Raft server that can be started, 
    stopped, and configured with a request handler. It abstracts the behavior 
    of a node in the Raft cluster.
    '''

    @abstractmethod
    def register_handler(self, handler: Handler) -> None:
        '''Register a handler to process incoming RPC requests.

        Args:
            handler (Handler): The request handler that processes incoming Raft RPCs.
        '''
        pass

    @abstractmethod
    def run_in_thread(self) -> Thread:
        '''Run the server in a separate thread.

        Returns:
            Thread: The thread in which the server is running.
        '''
        pass

    @abstractmethod
    def stop(self) -> None:
        '''Stop the server and clean up resources.'''
        pass


class Factory(ABC):
    '''Abstract Base Class for a factory that creates Raft clients and servers.

    This class defines the interface for creating Raft clients and servers,
    abstracting the underlying communication mechanism (e.g., JSON-RPC, gRPC).
    '''

    @abstractmethod
    def create_client(self, config_bytes: bytes) -> Client:
        '''Create and configure a Raft client.

        Args:
            config_bytes (bytes): Configuration data for the client.

        Returns:
            Client: A configured client instance.
        '''
        pass

    @abstractmethod
    def create_server(self, config_bytes: bytes) -> Server:
        '''Create and configure a Raft server.

        Args:
            config_bytes (bytes): Configuration data for the server.

        Returns:
            Server: A configured server instance.
        '''
        pass
