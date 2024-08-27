# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import ABC
from functools import reduce
from jsonrpcserver import method, Result, Success, dispatch, Error
from typing import Any, Callable
from threading import Thread
from werkzeug.serving import run_simple
import requests
import jsonrpcclient
import flask
import msgpack

from . import json, rpc, util
from .logging import logger


def compose(*funcs: Callable[..., Any]) -> Callable[..., Any]:
    '''
    Compose two or more functions into a single composite function.
    The functions are applied from right to left.

    Args:
        *funcs: A sequence of functions to compose.

    Returns:
        A composite function that applies the provided functions from right to left.
    '''
    return reduce(
        lambda f, g: lambda *a, **kw: f(g(*a, **kw)), funcs
    )  # pylint: disable=invalid-name


# Compose the dumps function with jsonrpcclient.request to create a new request function.
# This function serializes the request object to JSON after building it with jsonrpcclient.
request = compose(json.dumps, jsonrpcclient.request)


class Client(rpc.Client):
    '''JSON-RPC client for communicating with other Raft nodes.'''

    def __init__(self, hostname: str, port: int):
        '''
        Initialize the Client with a hostname and port.

        Args:
            hostname (str): The hostname of the server.
            port (int): The port number of the server.
        '''
        self._url = 'http://' + hostname + ':' + str(port)

    def call_rpc_method(self, method_name: str, params: dict) -> dict:
        '''
        Call an RPC method on a remote server and return the response.

        Args:
            method_name (str): The name of the RPC method to call.
            params (dict): The parameters to pass to the RPC method.

        Returns:
            dict: The response from the server.

        Raises:
            RaftRPCException: If the server returns an error.
        '''
        resp = requests.post(self._url, data=request(method_name, params))
        parsed = jsonrpcclient.parse(resp.json())
        if isinstance(parsed, jsonrpcclient.Ok):
            return parsed.result
        elif isinstance(parsed, jsonrpcclient.Error):
            raise Exception(
                f'Failed to call method "{method_name}", code: {parsed.code}, message: {parsed.message}')

        return {}

    def request_vote(self, req: rpc.RequestVoteRequest) -> rpc.RequestVoteResponse:
        '''
        Implements the RequestVote RPC.

        Args:
            req (rpc.RequestVoteRequest): The RequestVote request.

        Returns:
            rpc.RequestVoteResponse: The response to the RequestVote request.
        '''
        resp = self.call_rpc_method('requestVote', req.to_dict())
        return rpc.RequestVoteResponse.from_dict(resp)

    def append_entries(self, req: rpc.AppendEntriesRequest) -> rpc.AppendEntriesResponse:
        '''
        Implements the AppendEntries RPC.

        Args:
            req (rpc.AppendEntriesRequest): The AppendEntries request.

        Returns:
            rpc.AppendEntriesResponse: The response to the AppendEntries request.
        '''
        resp = self.call_rpc_method('appendEntries', req.to_dict())
        return rpc.AppendEntriesResponse.from_dict(resp)


class Server(metaclass=util.SingletonMeta):
    '''Singleton implementation of a JSON-RPC server for handling Raft protocol requests.'''

    _ERROR_PARSE_ERROR = -32700
    _ERROR_INVALId_REQUEST = -32600
    _ERROR_METHOD_NOT_FOUND = -32601
    _ERROR_INVALId_PARAMS = -32602
    _ERROR_INTERNAL_ERROR = -32603
    _ERROR_SERVER_ERROR = -32000

    _instance = None
    _handler = None

    def __init__(self, hostname: str, port: int):
        '''
        Initialize the Server with a hostname and port.
        Enforce singleton pattern by checking if an instance already exists.

        Args:
            hostname (str): The hostname to bind the server to.
            port (int): The port to listen on.

        Raises:
            Exception: If an instance already exists.
        '''
        if Server._instance is None:
            Server._instance = self
            self._port = port
            self._hostname = hostname
        else:
            raise Exception(
                'Singleton instance already created. Use the existing instance.')

    @staticmethod
    @method(name='ping')
    def ping() -> Result:
        '''
        A simple ping method to check if the server is running.

        Returns:
            Result: A successful result with 'pong' message.
        '''
        return Success('pong')

    @staticmethod
    @method(name='requestVote')
    def request_vote(*args, **kwargs) -> Result:
        '''
        Handles the RequestVote RPC method.

        Args:
            *args: Positional arguments for the RPC method.
            **kwargs: Keyword arguments for the RPC method.

        Returns:
            Result: A successful result with the response or an error if the handler is not set.
        '''
        logger.debug(
            f'JSONRPC Server handles requestVote, request_args: {args}, request_kwargs: {kwargs}',
        )
        if Server._handler is None:
            return Error(code=Server._ERROR_SERVER_ERROR, message='Handler not set')
        resp = Server._handler.handle_request_vote(
            rpc.RequestVoteRequest.from_dict(kwargs))
        return Success(resp.to_dict())

    @staticmethod
    @method(name='appendEntries')
    def append_entries(*args, **kwargs) -> Result:
        '''
        Handles the AppendEntries RPC method.

        Args:
            *args: Positional arguments for the RPC method.
            **kwargs: Keyword arguments for the RPC method.

        Returns:
            Result: A successful result with the response or an error if the handler is not set.
        '''
        logger.debug(
            f'JSONRPC Server handles appendEntries, request_args: {args}, request_kwargs: {kwargs}',
        )
        if Server._handler is None:
            return Error(code=Server._ERROR_SERVER_ERROR, message='Handler not set')
        resp = Server._handler.handle_append_entries(
            rpc.AppendEntriesRequest.from_dict(kwargs))
        return Success(resp.to_dict())

    def register_handler(self, handler: rpc.Handler):
        '''
        Registers a handler for processing RPC requests.

        Args:
            handler (rpc.Handler): The handler to register.
        '''
        Server._handler = handler

    def run(self):
        '''
        Runs the Flask app to handle incoming RPC requests.

        Starts the server with Flask and Werkzeug.
        '''
        app = flask.Flask(__name__)

        @app.route('/', methods=['POST'])
        def index():
            '''
            Dispatches incoming requests to the appropriate handler.

            Returns:
                flask.Response: The response from the RPC method.
            '''
            return flask.Response(
                dispatch(flask.request.get_data().decode()), content_type='application/json'
            )

        run_simple(self._hostname, self._port, app, threaded=True)

    def stop(self):
        '''Stops the server.'''
        return

    def run_in_thread(self) -> Thread:
        '''
        Runs the server in a separate thread.

        Returns:
            Thread: The thread running the server.
        '''
        thread = Thread(target=self.run)
        thread.daemon = True
        thread.start()
        return thread


class Factory(rpc.Factory):
    '''Factory for creating JSON-RPC client and server instances.'''

    def create_server(self, config_bytes: bytes) -> rpc.Server:
        '''
        Creates a server instance from the given configuration bytes.

        Args:
            config_bytes (bytes): The configuration bytes in msgpack format.

        Returns:
            rpc.Server: The created server instance.
        '''
        config: dict[str, Any] = msgpack.unpackb(config_bytes)
        return Server(config['hostname'], config['port'])

    def create_client(self, config_bytes: bytes) -> rpc.Client:
        '''
        Creates a client instance from the given configuration bytes.

        Args:
            config_bytes (bytes): The configuration bytes in msgpack format.

        Returns:
            rpc.RPC: The created client instance.
        '''
        config: dict[str, Any] = msgpack.unpackb(config_bytes)
        return Client(config['hostname'], config['port'])
