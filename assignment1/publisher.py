import argparse
import sys
import zmq
import time

from zmq_api import (
    publish,
    publish_to_broker,
    register_pub,
    register_pub_with_broker,

)

print(f"Current libzmq version is {zmq.zmq_version()}")
print(f"Current  pyzmq version is {zmq.__version__}")

parser = argparse.ArgumentParser ()
parser.add_argument ("-t", "--topic", type=str, default="zipcode temperature relhumidity", help="Topic needed")
parser.add_argument ("-s", "--srv_addr", type=str, default="localhost", help="Server Address")
parser.add_argument ("-b", "--broker_mode", default=False, action="store_true")
parser.add_argument ("-z", "--zip_code", type=str, default="10001", help="Zip Code")
args = parser.parse_args ()

#srv_addr = sys.argv[2] if len(sys.argv) > 2 else "localhost"
srv_addr = args.srv_addr

#broker_mode = int(sys.argv[3]) if len(sys.argv) > 3 else 0
broker_mode = args.broker_mode

zip_code = int(args.zip_code)

#context = zmq.Context()

# The difference here is that this is a publisher and its aim in life is
# to just publish some value. The binding is as before.
#socket = context.socket(zmq.PUB)
#socket.bind("tcp://*:5556")

#topic = "zipcode temperature relhumidity"
topic = args.topic

if not broker_mode:
	register_pub("*", topic)
else:
	register_pub_with_broker(srv_addr, topic)

# keep publishing
while True:
    #zipcode = randrange(1, 100000)
    #zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"

    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    #data = "%i %i %i" %(int(zipcode), temperature, relhumidity)
    data = f"{zip_code} {temperature} {relhumidity}"

    #print("Sending data: %s, %i, %i" % (zipcode, temperature, relhumidity))
    print("Sending data: {zip_code}, {temperature}, {relhumidity}")

    #socket.send_string("%i %i %i" % (int(zipcode), temperature, relhumidity))

    print("Trying to publish")
    if not broker_mode:
	    publish(topic, data)
    else:
        publish_to_broker(topic, data)
    time.sleep(0.5)