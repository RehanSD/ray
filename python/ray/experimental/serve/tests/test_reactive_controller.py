import pyximport; pyximport.install()
from ray.experimental.serve.router import reactive_controller

# checks that reactive_controller.pyx is linking properly to the C++ code
def test_fib():

    value = reactive_controller.c_fib(6)

    assert value == 8
