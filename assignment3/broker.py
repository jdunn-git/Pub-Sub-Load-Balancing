import argparse
import zmq
import sys
import time
from signal import signal, SIGINT
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
parser.add_argument ("-zk", "--zookeeper_ip", type=str, default="10.0.0.1", help="Zookeeper IP Address")
parser.add_argument ("-zp", "--zookeeper_port", type=int, default=2181, help="Zookeeper Port")
parser.add_argument ("-m", "--max_pub_count", type=int, default=-1, help="Maximum number of data propagations through broker.")
parser.add_argument ("-k", "--keep_alive", type=int, default=-1, help="Time to keep the broker alive.")
parser.add_argument ("-a", "--auto_mode", default=False, action="store_true")
#parser.add_argument ("-h", "--history", type=int, default=5, help="Number of publication to store in a history.")
args = parser.parse_args ()

threads = []
terminating = False

def handler(signal_received, frame):
	global terminating
	# Handle any cleanup here
	print('SIGINT or CTRL-C detected. Exiting gracefully')
	terminating = True
	disconnect()
	exit(0)

signal(SIGINT, handler)

def register_subs():
	global terminating
	try:
		while not terminating:
			# Listen for new subs to come onto the system
			listen_for_sub_registration()
	except:
		print("Sub registration listener ended")


def register_pubs():
	global terminating
	try:
		while not terminating:
			# Listen for new subs to come onto the system
			listen_for_pub_registration()
	except Exception as ex:
		print(f"Pub registration listener ended: {ex}")

def process_discovery():
	global terminating
	try:
		while not terminating:
			# Listen for new subs to come onto the system
			listen_for_pub_discovery_req()
	except:
			print("Pub discovery listener ended")

def pub_data_processor():
	global terminating
	max_pub_count = args.max_pub_count
	pub_count = 0
	#try:
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
	#except Exception as ex:
	#	print(f"Data propagator ended: {ex}")			

def receive_pub_data():
	# Get the pub message
	ownership_strength, history_count, filter_key, br_id, data = listen_for_pub_data()

	if data != None:
		# Forward published data to the appropriate subs
		publish_to_sub(ownership_strength, history_count, filter_key, br_id, data)


# Register broker
#zk_ip = "10.0.0.7"
zk_ip = args.zookeeper_ip
zk_port = args.zookeeper_port
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

#if not args.auto_mode:

if args.keep_alive == -1:
	#while True:
	#	pass
	# Wait for input to kill the broker and terminate connections
	input ("Disconnect from the server -- Press any key to continue")
	print("Disconnected from the server")
	terminating = True
	disconnect()
	sys.exit(0)
else:
	time.sleep(args.keep_alive)
	print("Disconnected from the server")
	terminating = True
	disconnect()
	sys.exit(0)
