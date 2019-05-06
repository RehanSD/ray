from ray.experimental.serve.janus import reactive_controller

def test_fib():

    value = reactive_controller.c_fib(6)

    assert value == 8

def test_start():

    value = reactive_controller.start()

    assert value == "start"

def test_stop():

    value = reactive_controller.stop()

    assert value == "stop"

def test_get_replica():

    value = reactive_controller.get_replica()

    assert value == "get_replica"

def test_complete_query():

    value = reactive_controller.complete_query()

    assert value == "complete_query"