import ray
from ray.experimental.serve import DeadlineAwareRouter
from ray.experimental.serve.examples.adder import VectorizedAdder
from ray.experimental.serve.router import start_router
from ray.experimental.serve.frontend import ZMQFrontendActor

ROUTER_NAME = "DefaultRouter"
ray.init(num_cpus=4)
router = start_router(DeadlineAwareRouter, ROUTER_NAME)
zmq_fe = ZMQFrontendActor.remote(router=ROUTER_NAME, port=8082)
a = zmq_fe.start.remote()
router.register_actor.remote(
    "VAdder", VectorizedAdder, init_kwargs={"scaler_increment": 1})
ray.get(a)