import argparse
import datetime
import sys
import zmq
import time
import os

from random import randrange
from zmq_api import (
    discover_broker,
    publish,
    publish_to_broker,
    register_pub,
    register_pub_with_broker,
    register_zk_driver,
    disconnect,
    decrement_pub_sub,
)

print(f"Current libzmq version is {zmq.zmq_version()}")
print(f"Current  pyzmq version is {zmq.__version__}")

parser = argparse.ArgumentParser ()
parser.add_argument ("-t", "--topic", type=str, default="zipcode", help="Topic needed")
parser.add_argument ("-s", "--srv_addr", type=str, default="localhost", help="Zookeeper Server Address")
parser.add_argument ("-b", "--broker_mode", default=False, action="store_true")
parser.add_argument ("-zk", "--zookeeper_ip", type=str, default="10.0.0.1", help="Zookeeper IP Address")
parser.add_argument ("-zp", "--zookeeper_port", type=int, default=2181, help="Zookeeper Port")
parser.add_argument ("-z", "--zip_code", type=str, default="10001", help="Zip Code")
parser.add_argument ("-e", "--executions", type=int, default=20, help="Number of executions for the program")
parser.add_argument ("-c", "--history", type=int, default=10, help="Number of messages to store in history")
parser.add_argument ("-w", "--record_time", default=False, action="store_true")
parser.add_argument ("-d", "--record_dir", type=str, default="timing_data", help="Directory to store timing data")
args = parser.parse_args ()

#zk_ip = "10.0.0.7"
#zk_port = 2181
zk_ip = args.zookeeper_ip
zk_port = args.zookeeper_port
print(f"Connecting to zk at {zk_ip}")

register_zk_driver(zk_ip, zk_port)
broker_ip = discover_broker(args.topic, args.zip_code)
print(f"Broker found at {broker_ip}")

#srv_addr = sys.argv[2] if len(sys.argv) > 2 else "localhost"

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

ownership_strength = "0"

if not broker_mode:
    register_pub(broker_ip, topic, zip_code, args.history)
else:
    ownership_strength = register_pub_with_broker(broker_ip, topic, zip_code, args.history)

f = None
if args.record_time:
    if not os.path.isdir(args.record_dir):
        os.mkdir(args.record_dir)
    f = open(f"{args.record_dir}/pub_{zip_code}.dat","a")

# keep publishing
messages_published = 0
messages_to_publish = args.executions
while messages_to_publish > messages_published:
    #zipcode = randrange(1, 100000)
    #zipcode = sys.argv[1] if len(sys.argv) > 1 else "10001"

    temperature = randrange(-80, 135)
    relhumidity = randrange(10, 60)

    #data = "%i %i %i" %(int(zipcode), temperature, relhumidity)
    data = f"{zip_code} {messages_published+1} {temperature} {relhumidity}"

    #print("Sending data: %s, %i, %i" % (zipcode, temperature, relhumidity))
    print(f"Sending data {messages_published+1}: {zip_code}, {temperature}, {relhumidity}")

    #socket.send_string("%i %i %i" % (int(zipcode), temperature, relhumidity))

    print("Trying to publish")

    if f != None:
        timestamp = str(datetime.datetime.utcnow().timestamp())
        f.write(f"{data} {timestamp}\n")

    if not broker_mode:
        publish(topic, zip_code, data, messages_published+1, datetime.datetime.utcnow().timestamp())
    else:
        publish_to_broker(topic, zip_code, data, messages_published+1, ownership_strength, datetime.datetime.utcnow().timestamp())
    time.sleep(0.5)
    messages_published += 1

if f != None:
    f.close()

decrement_pub_sub()
disconnect()
