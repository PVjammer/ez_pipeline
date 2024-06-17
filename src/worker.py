import logging
import time
import multiprocessing as mp

from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Callable

# logging.basicConfig(filename='pipeline.debug.log', encoding='utf-8', level=logging.DEBUG)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class PipelineStop:
    pass


class Worker(ABC):

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


class DefaultPipelineWorker(Worker):

    def __init__(self, function: Callable, input_queue: Queue, output_queue: Queue, *args, **kwargs):
        super().__init__(function, input_queue, output_queue, *args, **kwargs)
        
    def _get_input(self):
        if self._input_q.empty():
            return None

        _input = self._input_q.get()
        logger.info(f"{self._function.name} got input {_input}")

        if not self._check_valid_inputs(_input):
            return None

        return _input

    def _process_input(self, input):
        
        if isinstance(input, list):
            for i in input:
                o = self._function(i)
                self._output_q.put(o)
            
        else:
            o = self._function(input)
            if isinstance(o, list):
                for _o in o:
                    self._output_q.put(_o)
            else:
                self._output_q.put(o)

    def _exec(self):     
        while True:
            input = self._get_input()
            if isinstance(input, PipelineStop):
                break
            if not input:
                time.sleep(0.1)
                continue
            self._process_input(input)
        self._output_q.put(PipelineStop())

        