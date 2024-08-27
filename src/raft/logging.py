# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

import logging

# Configure the logger.
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Config logging handler.
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s - %(name)s - %(message)s'))

# Add handler to logger.
logger.addHandler(handler)
