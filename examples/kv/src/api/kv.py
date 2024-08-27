# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from flask import Blueprint, jsonify, request
from rocksdict import Rdict
from raft import Raft
from state import CommandResult, Command, CommandType
from typing import Optional

class KVService:
    _WAIT_COMMAND_RESULT_TIMEOUT_SECONDS = 10
    
    def __init__(self, raft: Raft, local_db: Rdict):
        self._blueprint = Blueprint('kv_service', __name__)
        self._raft = raft
        self._local_db = local_db
        self._init_routes()

    def _init_routes(self):
        self._blueprint.add_url_rule('/get', view_func=self.get, methods=['POST'])
        self._blueprint.add_url_rule('/set', view_func=self.set, methods=['POST'])
        self._blueprint.add_url_rule('/delete', view_func=self.delete, methods=['POST'])

    def _process_command(self, command_type: CommandType, key: str, value: Optional[str] = None) -> dict:
        command = Command(type=command_type, key=key, value=value).to_bytes()
        result_queue = self._raft.apply_command(command, True)
        result_bytes = result_queue.get(self._WAIT_COMMAND_RESULT_TIMEOUT_SECONDS)
        result = CommandResult.from_bytes(result_bytes)
        return result.to_dict()

    def get(self):
        body = request.json
        if str.lower(body['read_concern']) == 'linearizable':
            result = self._process_command(CommandType.GET, key=body['key'])
            return jsonify(result)
        elif str.lower(body['read_concern']) == 'local':
            key = body['key']
            if key not in self._local_db:
                return jsonify(CommandResult(result='NOT_FOUND', value=None).to_dict())
            return jsonify(CommandResult(result='OK', value=self._local_db[key]).to_dict())

    def set(self):
        body = request.json
        result = self._process_command(CommandType.SET, key=body['key'], value=body['value'])
        return jsonify(result)

    def delete(self):
        body = request.json
        result = self._process_command(CommandType.DEL, key=body['key'])
        return jsonify(result)

    def get_blueprint(self):
        return self._blueprint
