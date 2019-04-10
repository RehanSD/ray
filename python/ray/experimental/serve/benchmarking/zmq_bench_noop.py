import ray
from ray.experimental.serve import DeadlineAwareRouter
from ray.experimental.serve.examples.noop import NoOp
from ray.experimental.serve.router import start_router
from ray.experimental.serve.frontend import ZMQFrontendActor

ROUTER_NAME = "DefaultRouter"
ray.init(num_cpus=4)
router = start_router(DeadlineAwareRouter, ROUTER_NAME)
zmq_fe = ZMQFrontendActor.remote(router=ROUTER_NAME, port=8080)
a = zmq_fe.start.remote()
router.register_actor.remote(
        "NoOp", NoOp)
ray.get(a)
