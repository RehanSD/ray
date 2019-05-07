#!python
# cython: embedsignature=True, binding=True
# cython: profile=False
# distutils: language = c++
# cython: language_level = 3

from libcpp.string cimport string as c_string
from libcpp.vector cimport vector as c_vector
from libcpp.unordered_map cimport unordered_map
from libcpp.queue cimport queue as c_queue

cdef extern from "../../../src/ray/experimental/janus/reactive_controller.h":
   int fib(int s)
   void run_monitor_thread()
   float check_arrival_curve_exceeded(unordered_map map)

def c_fib(int n):
   return fib(n)

cdef class ReactiveController:

   cdef unordered_map[float, float] arrival_curve_max_counts_
   cdef unordered_map[float, int] replicas_per_model_
   cdef unordered_map[float, float] model_throughput_
   cdef unordered_map[float, float] model_scale_factors_
   cdef unordered_map[float, int] current_arrival_count_

   # __cinit__ with
   # arrival_max_counts_per_model
   # num_replicas_per_model
   # model_throughputs
   # model_scale_factors
   def __cinit__(self, unordered_map[float, int] rpm_,
       unordered_map[float, float] acmc_,
       unordered_map[float, float] mt_,
       unordered_map[float, float] msf_):
       self.arrival_curve_max_counts_ = acmc_
       self.replicas_per_model_ = rpm_
       self.model_throughput_ = mt_
       self.model_scale_factors_ = msf_

   cpdef get_num(self, float n):
       return self.arrival_curve_max_counts_[n]




   # start - starts monitoring thread

   # monitor thread - constantly checks if
   #      arrival_curve_exceeded
   #      need_to_add_replicas
   #      need_to_reduce_replicas

   # stop -stop monitoring thread

   # request for actor (get_replica) - allows for count of arrival times and serves model
   #

