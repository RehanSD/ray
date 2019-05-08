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
#from libcpp cimport float as c_float
#from libcpp cimport int as c_int


cdef extern from "../../../src/ray/experimental/serve/router/routers.h":
   int fib(int s)
   void register_arrival(c_string model, int query_id)
   void run_monitor_thread(unordered_map model_throughputs_, unordered_map last_model_scale_snapshot_,
    unordered_map model_num_replicas_, unordered_map model_max_loads_, unordered_map current_arrival_counts_,
    unordered_map arrival_curve_max_counts)

def c_fib(int n):
   return fib(n)


# init with
# arrival_max_counts_per_model
# num_replicas_per_model
# model_throughputs
# model_scale_factors
cpdef init(self, router_name, model_info):

   arrival_curve_max_counts: Dict[c_string, c_float] = {}
   replicas_per_model: Dict[c_string, c_int] = {}
   model_throughputs: Dict[c_string, c_float] = {}
   model_scale_factors: Dict[c_string, c_float] = {}

   for model_name in model_info:
        replicas_per_model[model_name] = model_info[model_name][0]
        model_scale_factors[model_name] = model_info[model_name][1]
        model_throughputs[model_name] = model_info[model_name][2]
        arrival_curve_max_counts[model_name] = model_info[model_name][3]

   #call init for routers.cc

# start - starts monitoring thread
cpdef void start(self):
   print("started")

# stop - stop monitoring thread
cpdef void stop(self):
   print("stopped")

# monitor thread - constantly checks if
#      arrival_curve_exceeded
cdef void arrival_curve_exceeded(self):
   print("checked curve")

#      need_to_add_replicas
cdef void check_need_to_add_replicas(self):
   print("checked need to add")

#      need_to_reduce_replicas
cdef void check_need_to_reduce_replicas(self):
   print("checked need to reduce")

# request for actor (get_replica) - allows for count of arrival times and serves model
cpdef void submit_arrival(self, model: str, query_id: int):
  register_arrival(model, query_id)
