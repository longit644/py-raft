# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from flask import Blueprint, jsonify, request
import msgpack

from raft import Raft

class AdminService:
    def __init__(self, raft: Raft):
        self._blueprint = Blueprint('admin_service', __name__)
        self._raft = raft
        self._init_routes()

    def _init_routes(self):
        self._blueprint.add_url_rule('/addServer', view_func=self.add_server, methods=['POST'])

    def add_server(self):
        data = request.json
        self._raft.add_server(
            data['id'],
            msgpack.packb({'hostname': data['hostname'], 'port': data['port']}),
        )

        return jsonify(result='OK')

    def get_blueprint(self):
        return self._blueprint
