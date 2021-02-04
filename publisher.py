# Sample code for CS6381
# Vanderbilt University
# Instructor: Aniruddha Gokhale
#
# Code taken from ZeroMQ examples with additional
# comments or extra statements added to make the code
# more self-explanatory  or tweak it for our purposes
#
# We are executing these samples on a Mininet-emulated environment
#
#

#
#   Weather update server
#   Binds PUB socket to tcp://*:5556
#   Publishes random weather updates
#

import sys
import zmq
from random import randrange

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

context = zmq.Context()

# The difference here is that this is a publisher and its aim in life is
# to just publish some value. The binding is as before.
socket = context.socket(zmq.PUB)
socket.bind("tcp://*:5556")

# keep publishing 
while True:
    #zipcode = randrange(1, 100000)
    zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"
    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    #print("Sending data: %s, %i, %i" % (zipcode, temperature, relhumidity))

    socket.send_string("%i %i %i" % (int(zipcode), temperature, relhumidity))
