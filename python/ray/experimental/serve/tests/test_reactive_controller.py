import pyximport; pyximport.install()
from ray.experimental.serve.router import janus

# checks that janus.pyx is linking properly to the C++ code
def test_fib():

    value = janus.c_fib(6)

    assert value == 8
