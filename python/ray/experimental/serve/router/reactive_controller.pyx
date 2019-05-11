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


cdef extern from "../../../src/ray/experimental/serve/janus/reactive_controller.h":
   int fib(int s)
   void register_arrival(c_string model, int query_id)
   void run_monitor_thread(unordered_map model_throughputs_, unordered_map last_model_scale_snapshot_,
    unordered_map model_num_replicas_, unordered_map model_max_loads_, unordered_map current_arrival_counts_,
    unordered_map arrival_curve_max_counts)
   void reactive_controller_init (c_vector[int] replicas_per_model, c_vector[float] model_scale_factors,
    c_vector[float] model_throughputs, c_vector[float] arrival_curve_max_counts)

def c_fib(int n):
   return fib(n)

def init(self, router_name, dict model_info):

   cdef c_vector[int] replicas_per_model = []
   cdef c_vector[float] model_scale_factors = []
   cdef c_vector[float] model_throughputs = []
   cdef c_vector[float] arrival_curve_max_counts = []
   cdef c_vector[c_string] model_names = []
   cdef c_string c_model_name
   cdef int index = 0

   for model_name in model_info:
        c_model_name = model_name.encode('UTF-8')
        model_names[index] = c_model_name
        index = index + 1
        replicas_per_model[model_name].emplace(model_info[model_name][0])
        model_scale_factors[model_name].emplace(model_info[model_name][1])
        model_throughputs[model_name].emplace(model_info[model_name][2])
        arrival_curve_max_counts[model_name].emplace(model_info[model_name][3])

   reactive_controller_init(replicas_per_model, model_scale_factors, model_throughputs, arrival_curve_max_counts)

# start - starts monitoring thread
cpdef void start(self):
   print("started")

# stop - stop monitoring thread
cpdef void stop(self):
   print("stopped")

# request for actor (get_replica) - allows for count of arrival times and serves model
cpdef void submit_arrival(self, model: str, query_id: int):
  register_arrival(model, query_id)
