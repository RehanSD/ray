import zmq
from zmq import Context

class ZMQClientException(Exception):
    pass

class ZMQClient:
    """Ray oblivious Class that manages communications with ray.serve.ZMQFrontendActor.

    If provided, the callback's only positional output should be
    the result from the ray.serve.model_actor. Any other arguments
    should be keyword arguments, and can be passed as such into the
    query method.
    """
    def __init__(self, ip="0.0.0.0", port=8080, default_actor=None):
        self.ip = ip
        self.port = port
        self.default_actor = default_actor
        self.ctx = Context()
        self.sock = self.ctx.socket(zmq.REQ)
        self.sock.connect("tcp://{}:{}".format(ip, port))

    def query(self, input, slo_ms, actor=None, callback=None, **kwargs):
        if actor is None and self.default_actor is None:
            raise ZMQClientException("A model must be specified, either in the constructor or the query method.")
        actor_name = actor if actor is not None else self.default_actor
        self.sock.send_json({"actor":actor_name, "input":input, "slo_ms":slo_ms})
        out = self.sock.recv_json()
        if not out["success"]:
            raise ZMQClientException("Actor %s raised the following Exception: %s" % (actor_name, out["result"]))
        if callback is None:
            return out["result"]
        output = callback(out["result"], **kwargs)
        return output