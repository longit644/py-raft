# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.


from enum import IntEnum
import queue
from typing import Optional
from threading import Event, Thread
import concurrent.futures
import random
import time

from . import rpc
from .config import Configuration, ServerSuffrage, ConfigurationStorage, Server as ServerConfig
from .log import LogStorage, Log, LogType
from .logging import logger
from .stable import StableStorage
from .fsm import FSM


class ExceptionNotLeader(Exception):
    '''Exception raised when a request is made to a node that is not the leader.'''

    def __init__(self, message='This node is not the leader.'):
        super().__init__(message)


class ShuttingDownException(Exception):
    '''Raised when the worker is shutting down.'''
    pass


class Role(IntEnum):
    '''Defines roles servers in the Raft system.'''

    FOLLOWER = 1
    '''A passive role where the server replicates log entries and responds to 
    requests from the leader or candidate.'''

    CANDIDATE = 2
    '''A transitional role where the server attempts to become the leader by votes
    from other servers.'''

    LEADER = 3
    '''An active role where the server is responsible for managing log replication,
    handling client requests, and maintaining the consistency of the system.'''


class Member:
    '''Represents a member in the Raft consensus system.'''

    def __init__(self, id: int, rpc_client: rpc.Client) -> None:
        # Unique identifier for this server in the Raft cluster.
        self._id = id

        # RPC client.
        self._rpc_client = rpc_client

        # ----------------------------------------------------------------------
        # Volatile state on leaders, reinitialized after election.

        # For each server, index of the next log entry to send to that server
        # initialized to leader last log index + 1, increases monotonically.
        self._next_index = 0

        # For each server, index of highest log entry known to be replicated on
        # server, initialized to 0, increases monotonically.
        self._match_index = 0

    @property
    def id(self) -> int:
        return self._id

    @property
    def next_index(self) -> int:
        return self._next_index

    @next_index.setter
    def next_index(self, value: int) -> None:
        self._next_index = value

    @property
    def match_index(self) -> int:
        return self._match_index

    @match_index.setter
    def match_index(self, value: int) -> None:
        self._match_index = value

    @property
    def rpc_client(self) -> rpc.Client:
        return self._rpc_client

    def reinitialize(self, leader_last_log_index: int) -> None:
        '''Reinitialize the volatile state on leaders after election.'''
        self._next_index = leader_last_log_index + 1
        self._match_index = 0


class _RaftState:
    # Keys used for storing persistent state in stable storage.
    _KEY_CURRENT_TERM = 'current_term'
    _KEY_VOTE_FOR = 'vote_for'
    _KEY_COMMIT_INDEX = 'commit_index'

    def __init__(self, id: int, stable_storage: StableStorage, log: LogStorage, config_storage: ConfigurationStorage) -> None:
        # Unique identifier for this server in the Raft cluster.
        self._id = id

        # ----------------------------------------------------------------------
        # Persistent state on all servers.
        # These fields must be persisted to stable storage before responding to
        # RPCs.

        # Stable storage for maintaining persistent state.
        self._stable_storage = stable_storage

        # The latest term this server has seen.
        self._current_term: int = self._stable_storage.get_int(
            self._KEY_CURRENT_TERM, 0)

        # The candidate that received this server's vote in the current term.
        self._voted_for: Optional[int] = self._stable_storage.get_int(
            self._KEY_VOTE_FOR)

        # Log storage object to manage log entries.
        self._log = log

        # Configuration storage to manage cluster config
        self._config_storage = config_storage

        # The highest index of a log entry known to be committed.
        self._commit_index: int = self._stable_storage.get_int(
            self._KEY_CURRENT_TERM, -1)

        # ----------------------------------------------------------------------
        # Volatile state on all servers.
        # This state is not persisted and may be lost if the server restarts.

        # The server Id of the current leader in the cluster.
        self._current_leader: Optional[int] = None

        # The current role of this server (Follower, Candidate, Leader).
        self._role = Role.FOLLOWER

        # ----------------------------------------------------------------------
        # Volatile state on candidates.
        self._votes_received = set[int]()

    @property
    def id(self) -> int:
        return self._id

    @property
    def role(self) -> Role:
        return self._role

    @role.setter
    def role(self, value: Role) -> None:
        self._role = value

    @property
    def voted_for(self) -> int:
        return self._voted_for

    @voted_for.setter
    def voted_for(self, value: int) -> None:
        self._voted_for = value

    @property
    def current_leader(self) -> int:
        return self._current_leader

    @current_leader.setter
    def current_leader(self, value: int) -> None:
        self._current_leader = value

    @property
    def commit_index(self) -> int:
        return self._commit_index

    @commit_index.setter
    def commit_index(self, value: int) -> None:
        if self._commit_index and value <= self._commit_index:
            return

        self._commit_index = value
        self._stable_storage.set_int(
            self._KEY_COMMIT_INDEX, self._commit_index)

    @property
    def current_term(self) -> int:
        return self._current_term

    @current_term.setter
    def current_term(self, value: int) -> None:
        if value <= self._current_term:
            return

        self._current_term = value
        self._stable_storage.set_int(
            self._KEY_CURRENT_TERM, self._current_term)

    @property
    def config_storage(self) -> ConfigurationStorage:
        return self._config_storage

    @property
    def cluster_config(self) -> Configuration:
        return self._config_storage.last_configuration()

    @property
    def server_config(self) -> ServerConfig:
        return self.cluster_config.get(self.id)

    @property
    def log(self) -> LogStorage:
        return self._log

    @property
    def votes_received(self) -> set[int]:
        return self._votes_received

    @votes_received.setter
    def votes_received(self, value: set[int]) -> None:
        self._votes_received = value

    def upgrade_to_voter(self):
        '''Upgrades the server's role to VOTER.

        This method changes the server's role to VOTER if it is not already set 
        as VOTER. The updated configuration is then persisted.

        Returns:
            None
        '''
        if self.server_config.is_voter():
            return

        self.server_config.suffrage = ServerSuffrage.VOTER
        # Set cluster config to itself to persist.
        self.cluster_config = self.cluster_config


class Raft(rpc.Handler):
    '''Represents a Raft consensus algorithm implementation.

    This class manages the state of the Raft protocol, handles log entries, 
    and coordinates with other Raft servers to ensure consensus in a distributed system. 
    It is responsible for maintaining the integrity and consistency of data 
    across multiple servers in the cluster.
    '''

    # The base interval for sending heartbeat messages in seconds.
    _HEARTBEAT_INTERVAL_SECONDS = 1
    _BROADCAST_TIMEOUT_SECONDS = 0.2

    def __init__(self, id: int, stable_storage: StableStorage, log: LogStorage, config_storage: ConfigurationStorage, fsm: FSM, rpc_factory: rpc.Factory) -> None:
        '''Initializes the Raft instance with the given configuration.'''

        # Event to signal server shutdown.
        self._shutdown_event = Event()

        # The interval between heartbeats (in seconds) for leader-to-follower
        # communication.
        self._heartbeat_interval_seconds: float = Raft._HEARTBEAT_INTERVAL_SECONDS

        # The next scheduled time (in seconds) to start a new election, if
        # needed.
        self._next_election_time: int = 0

        # ----------------------------------------------------------------------
        # Raft state.

        self._state = _RaftState(id, stable_storage, log, config_storage)

        # ----------------------------------------------------------------------
        # Application state machine.

        # The state machine that applies committed log entries.
        self._fsm = fsm

        # ----------------------------------------------------------------------
        # RPC.

        # Factory for creating RPC objects (e.g., for RPC communication).
        self._rpc_factory = rpc_factory

        # List of cluster servers (peers) for communication.
        self._servers: dict[int, Member] = {}
        self._init_servers()

        # RPC Server.
        self._rpc_server = self._rpc_factory.create_server(
            self._state.server_config.config)
        self._rpc_server.register_handler(self)

        # ----------------------------------------------------------------------
        # Task queue.
        self._task_queue = queue.Queue()

    @property
    def fsm(self) -> FSM:
        return self._fsm

    def _init_servers(self):
        '''Initializes the servers for the Raft cluster.'''
        for server_config in self._state.cluster_config.servers.values():
            if server_config.id not in self._servers:
                self._servers[server_config.id] = Member(
                    server_config.id, self._rpc_factory.create_client(
                        server_config.config)
                )
        servers: dict[int, Member] = {}
        for server in self._servers.values():
            if server.id in self._state.cluster_config.servers:
                servers[server.id] = server

        self._servers = servers

    def _duration_until_next_election(self) -> float:
        '''
        Calculates the time remaining until the next election, in milliseconds. 
        If the calculated duration is negative, indicating the election should 
        already have occurred, it returns 0.

        This method is thread-safe as it uses a lock to ensure that the election
        time is read atomically.
        '''
        # Calculate the duration until the next election by subtracting the
        # current time from the next election time.
        return self._next_election_time - time.monotonic()

    def _reset_election_time(self) -> float:
        '''
        Resets the election timer to a random point in the future. The new 
        election time is calculated by adding a random jitter to twice the 
        heartbeat interval to prevent election conflicts in the Raft cluster.

        This method is thread-safe as it uses a lock to ensure that the election 
        time is updated atomically.
        '''

        duration_until_next_election = random.random() * self._heartbeat_interval_seconds * \
            2 + self._heartbeat_interval_seconds * 2
        self._next_election_time = time.monotonic() + duration_until_next_election
        return duration_until_next_election

    # --------------------------------------------------------------------------
    # RPC util methods.

    def _call_concurrent_rpc_to_all_members(self, task_function):
        '''
        Helper method to execute RPC calls to all cluster members concurrently 
        with timeout handling.

        :param task_function: The function to execute for each server.
        '''

        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures: list[concurrent.futures.Future] = []
            future_to_server: dict[concurrent.futures.Future, Member] = {}
            for server in self._servers.values():
                if server.id != self._state.id:
                    future = executor.submit(task_function, server)
                    futures.append(future)
                    future_to_server[future] = server

            try:
                for future in concurrent.futures.as_completed(futures, timeout=self._BROADCAST_TIMEOUT_SECONDS):
                    try:
                        logger.debug(
                            f'Make "{task_function.__name__}" RPC call to server: {future_to_server[future].id}, server: {self._state.id}, term: {self._state.current_term}')
                        future.result(timeout=self._BROADCAST_TIMEOUT_SECONDS)
                    except Exception as e:
                        logger.error(
                            f'Failed to make "{task_function.__name__}" RPC cal to server: {future_to_server[future].id}, excpetion: {e}')
            except concurrent.futures.TimeoutError:
                logger.info(
                    f'Make "{task_function.__name__}" RPC broadcast timed out.')

    # --------------------------------------------------------------------------
    # RPC util methods.

    def _check_and_transition_to_follower(self, term: int) -> bool:
        if term > self._state.current_term:
            self._state.current_term = term
            self._state.role = Role.FOLLOWER
            self._state.voted_for = None
            self._reset_election_time()
            return True

        return False

    # --------------------------------------------------------------------------
    # Task worker methods.

    def _run_worker(self) -> Thread:
        '''
        Runs a worker thread that continuously processes tasks from the queue.

        The worker retrieves tasks from `self.task_queue` and executes them.
        It runs until the `_shutdown_event` is set, signaling the thread to stop.
        '''
        def worker():
            while not self._shutdown_event.is_set() or not self._task_queue.empty():
                try:
                    # Retrieve a task from the queue with a timeout.
                    func_name, func, args, kwargs = self._task_queue.get(
                        timeout=1)
                    logger.debug(
                        f'Execute func "{func_name}", server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
                    try:
                        # Execute the task.
                        func(*args, **kwargs)
                    finally:
                        # Mark the task as done only if it was retrieved.
                        self._task_queue.task_done()
                except queue.Empty:
                    # Continue if no task is available within the timeout.
                    continue
                except Exception as e:
                    logger.error(f'Error in worker: {e}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
            logger.debug(f'End worker loop, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')

        # Start the worker thread.
        thread = Thread(target=worker)
        thread.start()
        return thread

    def _emit_task(self, func, *args, **kwargs) -> concurrent.futures.Future:
        '''
        Adds a task to the queue for the worker to process.

        A task is defined by a function `func` and its arguments `args` and 
        `kwargs`. This method also provides an optional timeout for waiting on 
        the task's completion.

        :param func: The function to execute as a task.
        :param timeout: Maximum time to wait for the task to complete.
        :param args: Positional arguments for the function.
        :param kwargs: Keyword arguments for the function.
        :return: A Future that can be used to wait for the task's result or exception.
        '''
        if self._shutdown_event.is_set():
            raise ShuttingDownException()

        future = concurrent.futures.Future()

        def task():
            if self._shutdown_event.is_set():
                future.set_exception(ShuttingDownException())
                return

            try:
                result = func(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                logger.error(
                    f'Failed to execute func "{func.__name__}", exception: {e}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
                future.set_exception(e)

        # Add the task to the queue.
        self._task_queue.put((func.__name__, task, (), {}))

        return future

    # --------------------------------------------------------------------------
    # Time ticker methods.

    def _run_timeout_ticker(self) -> Thread:
        '''
        Runs a ticker thread that triggers the timeout handler at random intervals.

        This method reduces the likelihood of simultaneous elections in a Raft
        cluster by adding a jitter to the timeout interval.
        '''
        def timeout_ticker():
            while not self._shutdown_event.is_set():
                # Calculate the sleep time in seconds with added jitter.
                duration_until_next_election = self._duration_until_next_election()
                if duration_until_next_election <= 0:
                    self._reset_election_time()
                    try:
                        self._emit_task(self._on_timeout)
                    except ShuttingDownException:
                        pass
                    continue

                time.sleep(duration_until_next_election)

        # Start the timeout ticker thread.
        thread = Thread(target=timeout_ticker)
        thread.start()
        return thread

    def _run_heartbeat_ticker(self) -> Thread:
        '''
        Runs a ticker thread that triggers the heartbeat handler at regular 
        intervals.

        This method is responsible for emitting heartbeats to maintain 
        leadership in Raft.
        '''
        def heartbeat_ticker():
            while not self._shutdown_event.is_set():
                time.sleep(self._heartbeat_interval_seconds)
                try:
                    # Emit a task to handle the heartbeat.
                    self._emit_task(self._on_heartbeat)
                except ShuttingDownException:
                    pass

        # Start the heartbeat ticker thread.
        thread = Thread(target=heartbeat_ticker)
        thread.start()
        return thread

    # --------------------------------------------------------------------------
    # Raft's lifecycle methods.

    def _run(self) -> None:
        '''
        Starts and manages the Raft server's core components.

        This includes the worker thread, timeout ticker, heartbeat ticker,
        and the RPC server. Each of these runs in its own thread.
        The method blocks until all threads have completed.
        '''
        random.seed(time.time())
        # Start all necessary threads for the Raft server.
        worker_thread = self._run_worker()
        timeout_ticker_thread = self._run_timeout_ticker()
        heartbeat_ticker_thread = self._run_heartbeat_ticker()
        rpc_server_thread = self._rpc_server.run_in_thread()

        # Wait for all threads to complete (blocking operation).
        if rpc_server_thread.is_alive():
            rpc_server_thread.join()

        if worker_thread.is_alive():
            worker_thread.join()

        if timeout_ticker_thread.is_alive():
            timeout_ticker_thread.join()

        if heartbeat_ticker_thread.is_alive():
            heartbeat_ticker_thread.join()

    def run_in_thread(self) -> Thread:
        '''
        Starts the Raft server in a separate thread.

        This allows the Raft server to run independently without blocking the main thread.
        :return: The thread in which the Raft server is running.
        '''
        thread = Thread(target=self._run)
        thread.start()
        return thread

    def stop(self) -> None:
        '''
        Signals the Raft server to shut down.

        This method sets the `_shutdown_event`, which will cause all threads to
        exit gracefully, stopping the server and cleaning up resources.
        '''
        self._rpc_server.stop()
        self._shutdown_event.set()

    # --------------------------------------------------------------------------
    # Election flow methods.

    def _on_timeout(self) -> None:
        '''Handle timeout event'''
        if self._state.role in [Role.FOLLOWER, Role.CANDIDATE]:
            self._start_election()

    def _start_election(self) -> None:
        '''Start a new election process when a timeout occurs.'''
        # This method should handle transitioning the server from a follower or
        # candidate to a leader, incrementing the term, and sending vote
        # requests to other servers.
        if not self._state.server_config.is_voter():
            return

        self._state.current_term += 1
        self._state.role = Role.CANDIDATE
        self._state._voted_for = self._state.id
        self._state.votes_received.add(self._state.id)

        logger.info(
            f'Start a new selection, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
        )

        # Automatically transition to leader if the qourum is 1.
        if self._state.cluster_config.quorum(True) == 1:
            self._state.role = Role.LEADER
            self._state.current_leader = self._state.id
            return

        self._call_concurrent_rpc_to_all_members(self._request_vote)

    def _request_vote(self, server: Member) -> None:
        if not self._state.cluster_config.servers.get(server.id).is_voter():
            return

        last_log = self._state.log.last(default=Log())
        resp = server.rpc_client.request_vote(rpc.RequestVoteRequest(
            candidate_id=self._state.id,
            candidate_term=self._state.current_term,
            last_log_index=last_log.index,
            last_log_term=last_log.term,
        ))
        self._emit_task(self._on_request_vote_response, resp)

    def _on_request_vote(self, req: rpc.RequestVoteRequest) -> rpc.RequestVoteResponse:
        '''Handle an incoming AppendEntries RPC request.

        1. Reply false if term < currentTerm (§5.1)
        2. If votedFor is null or candidateId, and candidate's log is at least 
        as up-to-date as receiver's log, grant vote.

        Args:
            req (AppendEntriesRequest): The incoming request payload from 
                                        another server.

        Returns:
            AppendEntriesResponse: The response indicating success or failure.
        '''

        self._check_and_transition_to_follower(req.candidate_term)

        last_log = self._state.log.last(default=Log())
        log_ok = req.last_log_term > last_log.term or (
            req.last_log_term == last_log.term and req.last_log_index >= last_log.index)
        if self._state.current_term == req.candidate_term and self._state.voted_for in [None, req.candidate_id] and log_ok:
            self._state.voted_for = req.candidate_id
            return rpc.RequestVoteResponse(self._state.id, self._state.current_term, True)
        else:
            return rpc.RequestVoteResponse(self._state.id, self._state.current_term, False)

    def _on_request_vote_response(self, resp: rpc.RequestVoteResponse) -> None:
        if self._check_and_transition_to_follower(resp.term):
            return

        if self._state.role == Role.CANDIDATE and resp.term == self._state.current_term and resp.vote_granted:
            self._state.votes_received.add(resp.server_id)
            if len(self._state.votes_received) >= self._state.cluster_config.quorum(True):
                self._state.role = Role.LEADER
                self._state.current_leader = self._state.id
                for server in self._servers.values():
                    server.reinitialize(self._state.log.last_index())
                future = self._emit_task(self._replicate_logs)
                future.result(timeout=self._BROADCAST_TIMEOUT_SECONDS)

    # --------------------------------------------------------------------------
    # Broadcast heartbeat flow methods.
    def _on_heartbeat(self) -> None:
        if len(self._state.cluster_config.servers) == 1:
            return

        if self._state.role == Role.LEADER:
            logger.debug(
                f'Broadcast heartbeat, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
            )
            self.apply_log(
                Log(LogType.NOOP, b'', self._state.current_term), False
            )

    # --------------------------------------------------------------------------
    # Append log entry methods.

    def apply_command(self, msg: bytes, wait: bool) -> queue.Queue:
        return self.apply_log(Log(LogType.COMMAND, msg, self._state.current_term), wait)

    def apply_log(self, log: Log, wait: bool) -> queue.Queue:
        future = self._emit_task(self._on_apply_log, log)
        if wait:
            future.result(self._BROADCAST_TIMEOUT_SECONDS)
            return log._get_result()
        return None

    def _on_apply_log(self, log: Log):
        if self._state.role == Role.LEADER:
            self._state.log.append(log)
            server = self._servers[self._state.id]
            server.match_index = self._state.log.last_index()
            server.next_index = self._state.log.last_index()+1

            # Automatically commit entries if the qourum is 1.
            if self._state.cluster_config.quorum() == 1:
                self._commit_entries()
            else:
                self._replicate_logs()
            return None

        elif self._state.role == Role.FOLLOWER:
            # TODO: Forward the request to currentLeader via a FIFO link
            raise ExceptionNotLeader()
        else:  # CANDIDATE:
            # TODO: Buffer the request for later processing or reject the request with
            # an error indicating no leader is currently available.
            raise ExceptionNotLeader()

    def _replicate_logs(self) -> None:
        logger.debug(
            f'Replicate logs to all members, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
        self._call_concurrent_rpc_to_all_members(
            self._append_entries)

    def _append_entries(self, server: Member) -> None:
        if self._state.log.is_empty():
            return

        prev_log = self._state.log.get(server.next_index-1, Log())
        resp = server.rpc_client.append_entries(rpc.AppendEntriesRequest(
            leader_id=self._state.id,
            leader_term=self._state.current_term,
            prev_log_index=prev_log.index,
            prev_log_term=prev_log.term,
            entries=self._state.log.get_range(server.next_index, -1),
            leader_commit=self._state._commit_index
        ))
        self._emit_task(self._on_append_entries_response, resp)

    def _on_append_entries(self, req: rpc.AppendEntriesRequest) -> rpc.AppendEntriesResponse:
        self._reset_election_time()

        self._check_and_transition_to_follower(req.leader_term)
        if self._state.current_term == req.leader_term:
            # TODO: Review this logic
            self._state.current_leader = req.leader_id

        last_log_index = self._state.log.last_index()
        prev_log = self._state.log.get(req.prev_log_index, Log())
        prev_log_index = prev_log.index
        prev_log_term = prev_log.term

        log_ok = (last_log_index >= req.prev_log_index) and (
            prev_log_index < 0 or prev_log_term == req.leader_term)

        if self._state.current_term == req.leader_term and log_ok:
            self._process_new_entries(
                req.entries, req.prev_log_index, req.leader_commit)
            return rpc.AppendEntriesResponse(
                self._state.id, self._state.current_term, True,
                req.prev_log_index + len(req.entries)
            )
        else:
            return rpc.AppendEntriesResponse(
                self._state.id, self._state.current_term, False, 0
            )

    def _on_append_entries_response(self, resp: rpc.AppendEntriesResponse) -> None:
        if self._check_and_transition_to_follower(resp.term):
            return

        if self._state.role == Role.LEADER and resp.term == self._state.current_term:
            server = self._servers.get(resp.server_id)
            if resp.success and resp.match_index >= server.match_index:
                server.match_index = resp.match_index
                server.next_index = resp.match_index + 1
                self._commit_entries()
            elif server.next_index > 0:
                server.next_index -= 1
                self._append_entries(server)

    def _commit_entries(self) -> None:
        if self._state.log.is_empty():
            return

        logger.debug(
            f'Commit entries, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')

        while self._state.commit_index < self._state.log.last_index():
            matches = 0
            for server in self._servers.values():
                matches += 1 if server.match_index > self._state.commit_index else 0
            if matches >= self._state.cluster_config.quorum():
                self._deliver_committed_entry(
                    self._state.log.get(self._state.commit_index + 1))
                self._state.commit_index += 1
            else:
                break

    def _process_new_entries(self, entries: list[Log], prev_log_index: int, leader_commit_index: int) -> None:
        logger.debug(
            f'Process new entries, prev_log_index: {prev_log_index}, leader_commit_index: {leader_commit_index}, entries_len: {len(entries)}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
        # Trim all invalid entries.
        last_log_index = self._state.log.last_index()
        if len(entries) > 0 and (last_log_index > prev_log_index):
            index = min(last_log_index, prev_log_index + len(entries))
            if self._state.log.get(index).term != entries[index-prev_log_index].term:
                self._state.log.trim(prev_log_index+1)

        # Skip duplicated entries and append new entries to log storage.
        last_log_index = self._state.log.last_index()
        if last_log_index is None:
            self._state.log.append(*entries)
        else:
            self._state.log.append(
                *[entry for entry in entries if entry.index > last_log_index])

        # Commit committed entries.
        if leader_commit_index > self._state.commit_index:
            committed_entries = self._state.log.get_range(
                self._state.commit_index+1, leader_commit_index+1)
            for entry in committed_entries:
                self._deliver_committed_entry(entry)
            self._state.commit_index = leader_commit_index

    def _deliver_committed_entry(self, entry: Log) -> None:
        if entry.type == LogType.COMMAND:
            logger.debug(
                f'Apply command entry to FSM, entry: {entry}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
            result = self._fsm.apply(entry)
            logger.debug(
                f'Result of applying command entry to FSM, entry: {entry}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
            result_queue = entry._get_result()
            if result_queue.empty():
                result_queue.put(result)
            logger.debug(
                f'Put result of applying command entry to FSM, entry: {entry}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
        elif entry.type == LogType.CONFIGURATION:
            self._deliver_committed_config_entry(entry)

    def _deliver_committed_config_entry(self, entry: Log) -> None:
        logger.debug(
            f'Deliver commited config entry, entry: {entry}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}')
        # Apply configuration entry log to config FSM.
        last_applied_index = self._state.config_storage.last_applied_index()
        if last_applied_index and entry.index <= last_applied_index:
            return

        is_voter = False if self._state.server_config is None else self._state.server_config.is_voter()
        cfg = Configuration.from_bytes(entry.data)
        if self._state.role == Role.LEADER and cfg.has_non_voter():
            new_cfg = cfg.upgrade_all_to_voter()
            new_cfg_log = Log(
                LogType.CONFIGURATION, new_cfg.to_bytes(), self._state.current_term
            )
            self._emit_task(self._on_apply_log, new_cfg_log)

        result = self._state.config_storage.apply(entry)
        result_queue = entry._get_result()
        if result_queue.empty():
            result_queue.put(result)

        self._init_servers()

        if is_voter and not cfg.contains(self._state.id):
            # Stop server.
            self.stop()

    # --------------------------------------------------------------------------
    # Admin methods.

    def add_server(self, id: int, server_config_bytes: bytes) -> None:
        new_config = self._state.cluster_config.add(
            ServerConfig(id, ServerSuffrage.NON_VOTER, server_config_bytes),
        )
        self.apply_log(
            Log(LogType.CONFIGURATION, new_config.to_bytes(),
                self._state.current_term), True
        )

    # --------------------------------------------------------------------------
    # RPC handler methods.

    def handle_request_vote(self, req: rpc.RequestVoteRequest) -> rpc.RequestVoteResponse:
        '''Handle an incoming RequestVote RPC request.

        Args:
            req (RequestVoteRequest): The incoming request payload from another 
                                      node.

        Returns:
            RequestVoteResponse: The response indicating whether the vote was   
                                 granted.
        '''
        logger.debug(
            f'Handles requestVote RPC, req: {req}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
        )

        future = self._emit_task(self._on_request_vote, req)
        result = future.result(timeout=self._BROADCAST_TIMEOUT_SECONDS)
        logger.debug(
            f'Result requestVote RPC, req: {req}, resp: {result}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
        )
        return result

    def handle_append_entries(self, req: rpc.AppendEntriesRequest) -> rpc.AppendEntriesResponse:
        '''Handle an incoming AppendEntries RPC request.

        Args:
            req (AppendEntriesRequest): The incoming request payload from 
                                        another node.

        Returns:
            AppendEntriesResponse: The response indicating success or failure.
        '''
        logger.debug(
            f'Handles appendEntries RPC, req: {req}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
        )

        future = self._emit_task(self._on_append_entries, req)
        result = future.result(timeout=self._BROADCAST_TIMEOUT_SECONDS)

        logger.debug(
            f'Result appendEntries RPC, req: {req}, resp: {result}, server: {self._state.id}, term: {self._state.current_term}, role: {self._state.role.name}',
        )
        return result
