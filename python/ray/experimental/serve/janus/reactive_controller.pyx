#!python
# cython: embedsignature=True, binding=True
cdef extern from "../../../src/ray/experimental/janus/reactive_controller.h":
   int fib(int s)

def c_fib(int n):
   return fib(n)
#   cdef int i
#   cdef double a=0.0, b=1.0
#   for i in range(n):
#      a, b = a + b, a
#   return a

def start():
   return "start"

def stop():
   return "stop"

def get_replica():
   return "get_replica"

def complete_query():
   return "complete_query"