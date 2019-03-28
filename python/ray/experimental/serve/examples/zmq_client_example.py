from ray.experimental.serve.frontend import ZMQClient

def post(inp, inc=1.0):
    print("Output from VAdder Actor: %d\nKeyword Arg inc: %d" % (inp, inc))
    return inp + inc

zmq_client = ZMQClient(port=8082)

print(zmq_client.query(1.0, 1000, actor="VAdder"))
print(zmq_client.query(1.0, 1000, actor="VAdder", callback=post))
print(zmq_client.query(1.0, 1000, actor="VAdder", callback=post, inc=3.0))
try:
    print("Using wrong actor name")
    print(zmq_client.query(1.0, 1000, actor="Vadder", callback=post, inc=3.0))
except Exception as e:
    print(e)
try:
    print("Passing in no actor")
    print(zmq_client.query(1.0, 1000, callback=post, inc=3.0))
except Exception as e:
    print(e)

# Next line will and should crash.
print(zmq_client.query(1.0, 1000, callback=post, inc=3.0))
