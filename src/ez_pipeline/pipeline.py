import concurrent.futures
import logging
import multiprocessing as mp
import time

from abc import ABC, abstractmethod
# from queue import Queue
from typing import Any, Callable

from .common import PipelineCallable, PipelineStop, Worker
from .worker import DefaultPipelineWorker

"""
# TODO 
 - Store function neighbor names
 - Incorporate batch processing
 - Settle on a data structure for input?
"""

logger = logging.getLogger(__name__)




class PipelineFunction(PipelineCallable):

    def __init__(self, 
                 f: Callable, 
                 name: str = None, 
                 valid_inputs: list = None, 
                 input_variable: str = None,
                 output_variable: str = None,
                 num_workers: int = 1) -> None:
        
        self._func = f
        self._name = name
        self.valid_inputs = valid_inputs
        self.input_variable = input_variable
        self.output_variable = output_variable
        self._num_workers = num_workers

    @property
    def name(self):
        if not self._name:
            return str(self)
        return self._name

    def _exec(self, input: Any):
        return self._func(input)

    def __or__(self, f: Any):
        return Pipeline(self, *pipeify(f))
    
    def __ror__(self, f: Any):
        return Pipeline(*pipeify(f), self)


# class MultiThreadedPipelineFunction(PipelineCallable):

#     def __init__(self, 
#                  f: Callable, 
#                  name: str = None, 
#                  valid_inputs: list = None, 
#                  input_variable: str = None,
#                  output_variable: str = None,
#                  num_workers: int = 5) -> None:
        
#         self._func = f
#         self._name = name
#         self.valid_inputs = valid_inputs
#         self.input_variable = input_variable
#         self.output_variable = output_variable

#     @property
#     def name(self):
#         if not self._name:
#             return str(self)
#         return self._name

#     def _exec(self, input: Any):

#         return self._func(input)

#     def __or__(self, f: Any):
#         return Pipeline(self, *pipeify(f))
    
#     def __ror__(self, f: Any):
#         return Pipeline(*pipeify(f), self)



class Pipeline(PipelineCallable):
    
    def __init__(self, *args, **kwargs):
        self._functions: list[PipelineFunction] = [f for f in args if isinstance(f, PipelineCallable)] if args else []
        self._workers = []


    def get_functions(self):
        return self._functions

    def _exec(self, input: Any):
        for f in self._functions:
            input = f._exec(input)

        return input

    def _init_workers(self):
        self._queues = [mp.Queue()]
        for i in range(len(self._functions)):
            output_queue = mp.Queue()
            self._workers.append(DefaultPipelineWorker(
                function=self._functions[i],
                input_queue=self._queues[-1],
                output_queue=output_queue,
                valid_inputs= self._functions[i].valid_inputs,
                num_workers= self._functions[i]._num_workers,
            ))
            self._queues.append(output_queue)

    def _insert_input(self, input: Any):
        if not isinstance(input, list):
            self._queues[0].put(input)
        else:
            for i in input:
                self._queues[0].put(i)
                logger.info(f"Inserting {i} into queue {self._queues[0]}")
        logger.info(f"Inserting pipelineStop into queue {self._queues[0]}")
        self._queues[0].put(PipelineStop())

    def _run(self, input: Any):
        """
        """
        self._init_workers()
        processes = []
        for w in self._workers:
            p = mp.Process(target=w.run)
            p.start()
            processes.append(p)
        p = mp.Process(target=self._insert_input, args=(input,))
        
        p.start()
        processes.append(p)

        # Run as blocking process until output queue is empty
        output_list = []
        while True:
            if self._queues[-1].empty():
                time.sleep(0.5)
                continue

            _output = self._queues[-1].get()
            if isinstance(_output, PipelineStop):
                logger.info("Pipeline finished")
                for p in processes:
                    p.terminate()
                break
            if not _output:
                time.sleep(0.1)
                continue
            output_list.append(_output)

        return output_list


    def __call__(self, input: Any):
        return self._exec(input)

    def __or__(self, f: Any):
        return Pipeline(*flatten(self), *pipeify(f))
    
    def __ror__(self, f: Any):
        return Pipeline(*pipeify(f), *flatten(self))
            


def pipeify(f: Any) -> list[PipelineFunction]:
    if isinstance(f, PipelineFunction):
        return [f]
    elif isinstance(f, Callable):
        return [PipelineFunction(f)]
    elif isinstance(f, Pipeline):
        return flatten(f)
    else:
        raise ValueError(f"Type {type(f)} not supported for conversion to PipelineFunction")

def flatten(pipeline: Pipeline):
    return pipeline.get_functions()
