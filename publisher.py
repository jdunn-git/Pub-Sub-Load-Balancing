import sys
import zmq
from random import randrange
import zmq_api

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

context = zmq.Context()

# The difference here is that this is a publisher and its aim in life is
# to just publish some value. The binding is as before.
#socket = context.socket(zmq.PUB)
#socket.bind("tcp://*:5556") 

topic = "zipcode temperature relhumidity"

register_pub("*", topic)

# keep publishing 
while True:
    #zipcode = randrange(1, 100000)
    zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    data = "%i %i %i" %(int(zipcode), temperature, relhumidity)

    #print("Sending data: %s, %i, %i" % (zipcode, temperature, relhumidity))

    #socket.send_string("%i %i %i" % (int(zipcode), temperature, relhumidity))

    publish(topic, data)
    print("Sent data")
