import argparse
import sys
import zmq

from zmq_api import (
    listen,
    register_sub,
    register_sub_with_broker
)

parser = argparse.ArgumentParser ()
parser.add_argument ("-t", "--topic", type=str, default="zipcode temperature relhumidity", help="Topic needed")
parser.add_argument ("-s", "--srv_addr", type=str, default="localhost", help="Server Address")
parser.add_argument ("-b", "--broker_mode", default=False, action="store_true")
parser.add_argument ("-z", "--zip_code", type=str, default="10001", help="Zip Code")
parser.add_argument("-e", "--executions", type=int, default=20, help="Number of executions for the program")
args = parser.parse_args ()

#  Socket to talk to server
#context = zmq.Context()
#socket = context.socket(zmq.SUB)

# Here we assume publisher runs locally unless we
# send a command line arg like 10.0.0.1
#srv_addr = sys.argv[1] if len(sys.argv) > 1 else "localhost"
srv_addr = args.srv_addr
#connect_str = "tcp://" + srv_addr + ":5556"
connect_str = f"tcp://{srv_addr}:5556"

print("Collecting updates from weather server...")
#socket.connect(connect_str)

# Subscribe to zipcode, default is NYC, 10001
#zip_filter = sys.argv[2] if len(sys.argv) > 2 else "10001"
zip_filter = args.zip_code

# Python 2 - ascii bytes to unicode str
if isinstance(zip_filter, bytes):
    zip_filter = zip_filter.decode('ascii')

#print("Subscribing to %s" % zip_filter)
print("Subscribing to {zip_filter}")

#broker_mode = int(sys.argv[3]) if len(sys.argv) > 3 else 0
broker_mode = args.broker_mode
if not broker_mode:
	register_sub(srv_addr, zip_filter)
else:
	register_sub_with_broker(srv_addr, zip_filter)

# Process 10 updates
total_temp = 0
for update_nbr in range(10):
    string = listen(zip_filter)
    zipcode, temperature, relhumidity = string.split()
    total_temp += int(temperature)
    #print("Average temperature for zipcode '%s' was %dF" % (
      #zip_filter, total_temp / (update_nbr+1))
    #)
    print(f"Average temperature for zipcode {zip_filter} was {total_temp/ (update_nbr+1)}")
