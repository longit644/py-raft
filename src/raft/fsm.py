# Copyright (c) 2024 Long Bui.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.

from abc import ABC, abstractmethod
from typing import Optional

from . import log


class FSM(ABC):
    '''Abstract base class for implementing a state machine in a Raft consensus 
    system.

    This class defines the interface for an application that processes entry log
    and produces results. Concrete subclasses should implement the `apply` 
    method to handle specific command execution logic.
    '''

    @abstractmethod
    def apply(self, entry: log.Log) -> Optional[bytes]:
        '''Applies an log entry to the system's state or processing logic.

        Args:
            entry (log.Log): The log entry to be applied, which holds a 
                             command or data to be processed.
        '''
        pass

    @abstractmethod
    def last_applied_index(self) -> int:
        '''Retrieves the index of the most recently applied log entry to the 
        system's state or processing logic.

        Returns:
           int: The index of the last applied log entry, or -1 if no log entry 
                has been applied yet.
        '''
        pass


class NoopFSM(FSM):
    '''A no-operation state machine that implements the FSM interface.

    This class provides a default, no-op implementation of the `apply` method. 
    It effectively does nothing with the input log entry and is useful for 
    testing  or placeholder purposes where no actual state processing is needed.
    '''

    def apply(self, entry: log.Log) -> Optional[bytes]:
        '''No-op implementation of the apply method.

        This method does not process the input log entry. It is a placeholder
        implementation that does nothing with the provided log entry.
        '''
        return None
    
    def last_applied_index(self) -> int:
        '''No-op implementation of the last_applied_index method.'''
        return -1
