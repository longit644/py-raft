# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from flask import Flask
from threading import Thread
from rocksdict import Rdict
from werkzeug.serving import run_simple

from raft import Raft

from .kv import KVService
from .admin import AdminService


class Server:
    def __init__(self, port: int, raft: Raft, local_db: Rdict) -> None:
        self._app = Flask(__name__)
        self._port = port

        kv_service = KVService(raft, local_db)
        self._app.register_blueprint(
            kv_service.get_blueprint(), url_prefix='/api/kv')

        admin_service = AdminService(raft)
        self._app.register_blueprint(
            admin_service.get_blueprint(), url_prefix='/api/admin')

    def run(self) -> None:
        run_simple('0.0.0.0', self._port, self._app, threaded=True)

    def run_in_thread(self) -> Thread:
        thread = Thread(target=self.run)
        thread.start()
        return thread
