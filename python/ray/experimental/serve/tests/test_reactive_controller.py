from ray.experimental.serve.router import routers

# checks that routers.pyx is linking properly to the C++ code
def test_fib():

    value = routers.c_fib(6)

    assert value == 8
