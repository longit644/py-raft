# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import unittest
import msgpack

from unittest.mock import mock_open, patch

from raft import SimpleFileStableStorage 

class TestFileStableStorage(unittest.TestCase):

    @patch('builtins.open', new_callable=mock_open, read_data=msgpack.packb({b'key': b'value'}, use_bin_type=True))
    def test_load_data(self, mock_open):
        storage = SimpleFileStableStorage('raft-unittest.metadata.msgpack')
        self.assertEqual(storage.get(b'key'), b'value')

if __name__ == '__main__':
    unittest.main()
