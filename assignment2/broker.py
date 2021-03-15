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
	while not terminating:
		receive_pub_data()

def receive_pub_data():
	# Get the pub message
	string = listen_for_pub_data()

	if string != None:
		# Forward published data to the appropriate subs
		publish_to_sub(string)


# Register broker
zk_ip = "10.0.0.7"
zk_port = 2181
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


# Wait for input to kill the broker and terminate connections
input ("Disconnect from the server -- Press any key to continue")
terminating = True
disconnect()
#for t in threads:
#	t.join()
sys.exit(0)