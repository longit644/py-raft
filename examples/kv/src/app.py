# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from rocksdict import Rdict
import click
import signal
import raft
import os
import msgpack

import state
import api


def signal_handler(signum, frame):
    print('Graceful shutdown initiated...')
    raft_handler.stop()  # Trigger server shutdown


@click.command()
@click.option('--port', default=5000, type=int, help='Port for the API server.')
@click.option('--db-path', default='kv', type=str, help='Path to the database.')
@click.option('--server-id', default=1, type=int, help='The id of server.')
@click.option('--raft-port', default=6000, type=int, help='Port for the Raft server.')
@click.option('--raft-metadata-dir', default='./', type=str, help='Directory for Raft metadata.')
@click.option('--raft-is-voter', default=False, type=bool)
def start_servers(port: int, db_path: str, server_id: int, raft_port: int, raft_metadata_dir: str, raft_is_voter: bool):
    global raft_handler

    local_db = Rdict(db_path)
    # Start the Raft and Raft RPC server.
    raft_handler = raft.Raft(
        server_id,
        raft.SimpleFileStableStorage(os.path.join(
            raft_metadata_dir, 'METADATA')),
        raft.SimpleFileLogStorage(os.path.join(
            raft_metadata_dir, 'LOG')),
        raft.SimpleFileConfigurationStorage(os.path.join(
            raft_metadata_dir, 'CONFIG'), raft.Configuration(
                servers={server_id: raft.ServerConfig(
                    id=server_id,
                    suffrage=raft.ServerSuffrage.VOTER if raft_is_voter else raft.ServerSuffrage.NON_VOTER,
                    config=msgpack.packb(
                        {'hostname': '0.0.0.0', 'port': raft_port}),
                )}
        )),
        state.FSM(local_db),
        raft.jsonrpc.Factory(),
    )
    raft_handler_thread = raft_handler.run_in_thread()

    # Start the API server in a separate thread.
    api_server = api.Server(port, raft_handler, local_db)
    api_server_thread = api_server.run_in_thread()

    signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signals

    if api_server_thread.is_alive():
        api_server_thread.join()

    if raft_handler_thread.is_alive():
        raft_handler_thread.join()


if __name__ == '__main__':
    start_servers()
