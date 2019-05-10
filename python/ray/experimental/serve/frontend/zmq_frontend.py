from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import zmq
from zmq import Context

import ray
"""
Replicate HTTP Frontend in ZMQ.
"""

def unwrap(future):
    """Unwrap the result from ray.experimental.server janus.
    Router returns a list of object ids when you call them.
    """
    return ray.get(future)[0]

@ray.remote
class ZMQFrontendActor:
    """ZMQ API for an Actor. This exposes a socket that serves as the
     endpoints of queries. Clients (ZMQ.REQ sockets) send new requests to the ObjectID socket (ZMQ.REP),
      which returns the result of calling ray.get on the future returned when the query is scheduled
      with the Router. In other words, it returns the result of the model actor as a response.

    ObjectID Socket
    Request:
        sock.send_json(
            {
                "actor": string,
                "slo_ms": float,
                "input": any
            })
    Response:
        sock.recv_json()
        {
            "success": bool,
            "actor": str,
            "result": Serialized ObjectID
        }
    """

    def __init__(self, ip="0.0.0.0", port=8080, router="DefaultRouter"):
        self.ip = ip
        self.port = port
        self.router = ray.experimental.named_actors.get_actor(router)
        self.ctx = Context.instance()

    def start(self):
        objid_sock = self.ctx.socket(zmq.REP)
        objid_sock.bind("tcp://{}:{}/".format(self.ip, self.port))
        while True:
            req = objid_sock.recv_json()
            actor_name = req["actor"]
            slo_seconds = req["slo_ms"] / 1000
            deadline = time.perf_counter() + slo_seconds
            inp = req["input"]

            try:
                result_future = unwrap(
                    self.router.call.remote(actor_name, inp, deadline)
                )
                result = ray.get(result_future)
                objid_sock.send_json({"success": True, "actor": actor_name, "result": result})
            except Exception as e:
                objid_sock.send_json({"success": False, "actor": actor_name, "result": str(e)})