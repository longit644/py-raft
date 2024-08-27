# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from flask import Flask
from threading import Event, Thread
from rocksdict import Rdict
from werkzeug.serving import make_server
from raft import Raft
import logging

from .kv import KVService
from .admin import AdminService

logger = logging.getLogger(__name__)


class Server:
    def __init__(self, port: int, raft: Raft, local_db: Rdict) -> None:
        app = Flask(__name__)
        self._port = port
        self._server = make_server('0.0.0.0', port, app, threaded=True)

        kv_service = KVService(raft, local_db)
        app.register_blueprint(
            kv_service.get_blueprint(), url_prefix='/api/kv')

        admin_service = AdminService(raft)
        app.register_blueprint(
            admin_service.get_blueprint(), url_prefix='/api/admin')

    def run(self) -> None:
        logger.info(f"Starting server on port {self._port}...")
        self._server.serve_forever()

    def run_in_thread(self) -> Thread:
        thread = Thread(target=self.run)
        thread.start()
        return thread
    
    def stop(self) -> None:
        self._server.shutdown()
