from ray.experimental.serve.janus import _reactive_controller

def test_fib():

    value = _reactive_controller.fib(6)

    assert value == 8

def test_start():

    value = _reactive_controller.start()

    assert value == "start"

def test_stop():

    value = _reactive_controller.stop()

    assert value == "stop"

def test_get_replica():

    value = _reactive_controller.get_replica()

    assert value == "get_replica"

def test_complete_query():

    value = _reactive_controller.complete_query()

    assert value == "complete_query"