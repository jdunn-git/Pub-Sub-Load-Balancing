import argparse
import sys
import zmq
import os
import datetime
import time
import _thread

from zmq_api import (
    discover_broker,
    discover_publishers,
    listen,
    register_sub,
    register_sub_with_broker,
    synchronized_listen,
    register_zk_driver,
    disconnect,
)

f = None
total_temp = 0

def process_response(string, update_nbr):
    global f
    global total_temp
    print(string)
    zipcode, temperature, relhumidity = string.split()
    total_temp += int(temperature)
    #print("Average temperature for zipcode '%s' was %dF" % (
      #zip_filter, total_temp / (update_nbr+1))
    #)
    print(f"Average temperature for zipcode {zip_filter} was {total_temp/ (update_nbr+1)}")

    if f != None:
        data = f"{zipcode} {temperature} {relhumidity}"
        timestamp = str(datetime.datetime.utcnow().timestamp())
        f.write(f"{data} {timestamp}\n")


parser = argparse.ArgumentParser ()
parser.add_argument ("-t", "--topic", type=str, default="zipcode temperature relhumidity", help="Topic needed")
parser.add_argument ("-s", "--srv_addr", type=str, default="localhost", help="Zookeeper Server Address")
parser.add_argument ("-b", "--broker_mode", default=False, action="store_true")
parser.add_argument ("-z", "--zip_code", type=str, default="10001", help="Zip Code")
parser.add_argument ("-e", "--executions", type=int, default=20, help="Number of executions for the program")
parser.add_argument ("-i", "--sub_id", type=int, default=0, help="id of this subscriber")
parser.add_argument ("-w", "--record_time", default=False, action="store_true")
parser.add_argument ("-d", "--record_dir", type=str, default="timing_data", help="Directory to store timing data")
args = parser.parse_args ()
print(f"args: {args}")

#zk_ip = "10.0.0.7"
zk_port = 2181
zk_ip = args.srv_addr
print(f"Connecting to zk at {zk_ip}")
register_zk_driver(zk_ip, zk_port)
broker_ip = discover_broker()
print(f"Broker found at {broker_ip}")

#  Socket to talk to server
#context = zmq.Context()
#socket = context.socket(zmq.SUB)

# Here we assume publisher runs locally unless we
# send a command line arg like 10.0.0.1
#srv_addr = sys.argv[1] if len(sys.argv) > 1 else "localhost"
#srv_addr = args.srv_addr

#connect_str = "tcp://" + srv_addr + ":5556"
connect_str = f"tcp://{broker_ip}:5556"

print("Collecting updates from weather server...")
#socket.connect(connect_str)

# Subscribe to zipcode, default is NYC, 10001
#zip_filter = sys.argv[2] if len(sys.argv) > 2 else "10001"
zip_filter = args.zip_code

# Python 2 - ascii bytes to unicode str
if isinstance(zip_filter, bytes):
    zip_filter = zip_filter.decode('ascii')

#print("Subscribing to %s" % zip_filter)
print(f"Subscribing to {zip_filter}")

#broker_mode = int(sys.argv[3]) if len(sys.argv) > 3 else 0
broker_mode = args.broker_mode
if not broker_mode:
    pub_ips = []
    #while len(pub_ips) == 0:
    pub_ips = discover_publishers(broker_ip, "zipcode")
    #    if len(pub_ips) == 0:
    #        time.sleep(1)

    register_sub(broker_ip, pub_ips, "zipcode", zip_filter, process_response, 10)
else:
    register_sub_with_broker(broker_ip, zip_filter)

if args.record_time:
    if not os.path.isdir(args.record_dir):
        os.mkdir(args.record_dir)
    f = open(f"{args.record_dir}/sub_{zip_filter}-{args.sub_id}.dat","a")

if not broker_mode:
    synchronized_listen(zip_filter, process_response, 10)
    print("Done with synchronized_listen")
else:
    # Process 10 updates
    for update_nbr in range(10):
        string = listen(zip_filter,0)
        process_response(string, update_nbr)

if f != None:
    f.close()

disconnect()
