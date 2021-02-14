import sys
import zmq
import time
from random import randrange
from zmq_api import register_pub
from zmq_api import register_pub_with_broker
from zmq_api import publish

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

broker_mode = sys.argv[2] if len(sys.argv) > 2 else 0

#context = zmq.Context()

# The difference here is that this is a publisher and its aim in life is
# to just publish some value. The binding is as before.
#socket = context.socket(zmq.PUB)
#socket.bind("tcp://*:5556") 

topic = "zipcode temperature relhumidity"

if broker_mode == 0:
	register_pub("*", topic)
else:
	register_pub_with_broker("10.0.0.3", topic)

# keep publishing 
while True:
    #zipcode = randrange(1, 100000)
    zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    data = "%i %i %i" %(int(zipcode), temperature, relhumidity)

    print("Sending data: %s, %i, %i" % (zipcode, temperature, relhumidity))

    #socket.send_string("%i %i %i" % (int(zipcode), temperature, relhumidity))

    print("Trying to publish")
    publish(topic, data)
    time.sleep(0.5)
