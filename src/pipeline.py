import logging
import multiprocessing as mp
import time

from abc import ABC, abstractmethod
# from queue import Queue
from typing import Any, Callable

from worker import PipelineStop, Worker, DefaultPipelineWorker

"""
# TODO 
 - Incorporate workers in separate threads for continuous processing
 - Incorporate batch processing
 - Settle on a data structure for input?
"""

logger = logging.getLogger(__name__)

class PipelineCallable(ABC):

    @abstractmethod
    def _exec(self, input: Any):
        raise NotImplementedError

    def __call__(self, input):
        return self._exec(input)


class PipelineFunction(PipelineCallable):

    def __init__(self, f: Callable, name: str = None, valid_inputs: list = None) -> None:
        # self._input_q = Queue()
        self._func = f
        self._name = name
        self.valid_inputs = valid_inputs

    @property
    def name(self):
        return self._name

    def _exec(self, input: Any):
        return self._func(input)

    def __or__(self, f: Any):
        return Pipeline(self, *pipeify(f))
    
    def __ror__(self, f: Any):
        return Pipeline(*pipeify(f), self)


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
            ))
            self._queues.append(output_queue)

    def _insert_input(self, input: Any):
        for i in input:
            self._queues[0].put(i)
            print(f"Inserting {i} into queue {self._queues[0]}")
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
        # p = mp.Process(target=self._insert_input, args=(input))
        self._insert_input(input)
        # print(self._queues[0].get())
        # raise
        # p.start()
        # processes.append(p)

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

def add_two(x):
    return x + 2

def double(x):
    return x*2

def div_3(x):
    return round(x/3.0, 2)


if __name__ == "__main__":
    logging.basicConfig(filename='pipeline.debug.log', encoding='utf-8', level=logging.DEBUG)
    pipeline: Pipeline = PipelineFunction(add_two, name="add_two") | PipelineFunction(double, name="double") | PipelineFunction(div_3, name="div3")
    input_list = range(10000)
    # output = pipeline._exec(input_list)
    output = pipeline._run(input_list)
    print(output)