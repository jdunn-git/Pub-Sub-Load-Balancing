import argparse
import zmq
import sys
from threading import Thread

from zmq_api import (
	listen_for_pub_data,
	listen_for_pub_registration,
	listen_for_sub_registration,
	listen_for_pub_discovery_req,
	publish_to_sub,
	register_broker,
	disconnect,
)

print(f"Current libzmq version is {zmq.zmq_version()}")
print(f"Current  pyzmq version is {zmq.__version__}")

parser = argparse.ArgumentParser ()
parser.add_argument ("-s", "--srv_addr", type=str, default="localhost", help="Zookeeper Server Address")
parser.add_argument ("-m", "--max_pub_count", type=int, default=-1, help="Maximum number of data propagations through broker.")
parser.add_argument ("-a", "--auto_mode", default=False, action="store_true")
args = parser.parse_args ()

threads = []
terminating = False


def register_subs():
	global terminating
	while not terminating:
		# Listen for new subs to come onto the system
		listen_for_sub_registration()


def register_pubs():
	global terminating
	while not terminating:
		# Listen for new subs to come onto the system
		listen_for_pub_registration()

def process_discovery():
	global terminating
	while not terminating:
		# Listen for new subs to come onto the system
		listen_for_pub_discovery_req()

def pub_data_processor():
	global terminating
	max_pub_count = args.max_pub_count
	pub_count = 0
	while not terminating:
		# Break if we have exceeded the maximuim count
		if (max_pub_count != -1 and pub_count >= max_pub_count):
			print("max pub count hit")
			terminating = True
			disconnect()
			sys.exit(0)

			break
		#else:
		#	print(f"Max: {max_pub_count}, current: {pub_count}")

		receive_pub_data()
		pub_count += 1

def receive_pub_data():
	# Get the pub message
	string = listen_for_pub_data()

	if string != None:
		# Forward published data to the appropriate subs
		publish_to_sub(string)


# Register broker
#zk_ip = "10.0.0.7"
zk_port = 2181
zk_ip = args.srv_addr
register_broker(zk_ip,zk_port)

# Start new listener for subs
t = Thread(target=register_subs, args=())
t.start()
threads.append(t)

t = Thread(target=register_pubs, args=())
t.start()
threads.append(t)

# Start new listener for discovery requests
t = Thread(target=process_discovery, args=())
t.start()
threads.append(t)

# Start pub data listener
t = Thread(target=pub_data_processor, args=())
t.start()
threads.append(t)

if not args.auto_mode:

	# Wait for input to kill the broker and terminate connections
	input ("Disconnect from the server -- Press any key to continue")
	print("Disconnected from the server")
	terminating = True
	disconnect()
	#for t in threads:
	#	t.join()
	sys.exit(0)