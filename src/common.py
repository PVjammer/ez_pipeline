import time

from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Callable

class PipelineStop:
    pass


class PipelineCallable(ABC):

    @abstractmethod
    def _exec(self, input: Any):
        raise NotImplementedError

    def __call__(self, input):
        return self._exec(input)


class Worker(ABC):
    """"""
    def __init__(self, function: Callable, input_queue: Queue, output_queue: Queue , valid_inputs: list = None, *args, **kwargs):
        self._input_q = input_queue
        self._output_q = output_queue
        self._function = function
        self.valid_inputs = None if not valid_inputs or len(valid_inputs) < 1 else valid_inputs
            

    @abstractmethod
    def _exec(self, *args, **kwargds):
        raise NotImplementedError

    def _check_valid_inputs(self, input: Any):
        if not input:
            time.sleep(0.1)
            return False

        if not self.valid_inputs:
            return True

        for v in self.valid_inputs:
            if isinstance(input, v):
                return True

        return False

    def run(self):
        self._exec()