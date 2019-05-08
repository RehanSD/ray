#!python
# cython: embedsignature=True, binding=True
# cython: profile=False
# distutils: language = c++
# cython: language_level = 3

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map
from libcpp.queue cimport queue as c_queue
from libcpp cimport float as c_float
from libcpp cimport int as c_int


from collections import defaultdict
from functools import total_ordering
from typing import Callable, Dict, List, Set, Tuple

import ray
from ray.experimental.serve.object_id import get_new_oid
from ray.experimental.serve.utils.priority_queue import PriorityQueue

import json

ACTOR_NOT_REGISTERED_MSG: Callable = (
    lambda name: ("Actor {} is not registered with this router. Please use "
                  "'router.register_actor.remote(...)' "
                  "to register it.").format(name))

RESOURCE_NOT_DEFINED_MSG = "A resource was not specified. There must be 'num_cpu', 'num_gpu' or 'resource' arguments " \
                           "with this action."

cdef extern from "../../../src/ray/experimental/serve/router/routers.h":
   int fib(int s)
   void register_arrival(c_string model, int query_id)


def c_fib(int n):
   return fib(n)

# Use @total_ordering so we can sort SingleQuery
@total_ordering
class SingleQuery:
    """A data container for a query.

    Attributes:
        data: The request data.
        result_object_id: The result object ID.
        deadline: The deadline in seconds.
    """

    def __init__(self, data, result_object_id: ray.ObjectID,
                 deadline_s: float):
        self.data = data
        self.result_object_id = result_object_id
        self.deadline = deadline_s

    def __lt__(self, other):
        return self.deadline < other.deadline

    def __eq__(self, other):
        return self.deadline == other.deadline

