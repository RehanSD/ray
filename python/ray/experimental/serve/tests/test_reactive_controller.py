from ray.experimental.serve.janus import reactive_controller

def test_fib():

    value = reactive_controller.c_fib(6)

    assert value == 8

def test_start():

    value = reactive_controller.c_start()

    assert value.decode("utf-8")  == "start"

def test_stop():

    value = reactive_controller.c_stop()

    assert value.decode("utf-8")  == "stop"