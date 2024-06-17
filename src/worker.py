import logging
import time
import multiprocessing as mp

from abc import ABC, abstractmethod
from queue import Queue
from typing import Any, Callable

from .common import Worker, PipelineCallable, PipelineStop

logging.basicConfig(filename='pipeline.debug.log', encoding='utf-8', level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DefaultPipelineWorker(Worker):
    """
    Standard worker which reads data from an input queue, calls a function, and puts the output of that function into 
    an output queue. If the input is a list, each element of the list will be passed to the function sequentially. If the
    function output is a list, each element of the list will be pushed into the output queue sequentially.

    Args:
     function: PipelineFunction which takes as input a single variable `input` and produces an output. Use `functools.partial` 
               when creating a PipelineFunction to include other parameters or pass a dictionary as input.
     input_queue: Queue to use for generating input. Default Pipeline implementation uses a `multiprocessing.Queue`
     output_queue: Queue to use for pushing output. Default Pipeline implementation uses a `multiprocessing.Queue`
    """
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
                o = _wrap(self._function, i)
                logger.debug(f"{self._function.name} inserting {o} into output queue")
                self._output_q.put(o)
            
        else:
            o = _wrap(self._function, input)
            logger.debug(f"{self._function.name} inserting {o} into output queue")
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


def _wrap(function: Callable, input: Any):    
    # Case: Input is not a dictionary
    if not isinstance(input, dict):
        logger.debug("Not a dictionary.")
        _output = function(input)
        if not function.output_variable:    
            return _output
        return {function.output_variable: _output}

    # Input is a dictionary. 
    # Case: No input variable specified.
    if not function.input_variable:
        logger.debug(f"No input variable specifed. Passing {input}")
        _output = function(input)
        if not function.output_variable:    
            return _output
        return {function.output_variable: _output}

    # Case: Input variable is specified.
    _input =  input.get(function.input_variable, None)
    logger.debug(f"Got {_input} from input dictionary.")
    print(f"{_input=}")
    if  _input is None:
        logger.warn(f"Variable {function.input_variable} not present in input. Passing the input instead. {input}")
        _output =  function(input)
    else:
        _output =  function(_input)
    print(f"{_output=}")
    if function.output_variable is None:    
        return _output
    input.update({function.output_variable: _output})

    return input