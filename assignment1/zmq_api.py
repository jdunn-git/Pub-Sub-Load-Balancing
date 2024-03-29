import sys
import zmq

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
sub_dict = dict()
sub_port_dict = dict()

pub_socket = context.socket(zmq.PUB)
sub_socket = context.socket(zmq.SUB)

broker_receive_socket = context.socket(zmq.SUB)
broker_send_socket = context.socket(zmq.PUB)

pub_listener_socket = context.socket(zmq.REP)
sub_listener_socket = context.socket(zmq.REP)

pub_broker_socket = context.socket(zmq.REQ)

# Starting value for the ports used by subs int this api
sub_port = 5556

# TODO: Test and make sure that all subs will register with all pubs for that topic, and all pubs will send to all pubs
#			- This may just require defaulting to using "*" for subs, but it could be as complicated as adding new connects for the sub dynamically
#			- Or, it could just require re-connecting
# TODO: Handle dynamic arrivals and departures



## Functions for publisher communication ##

# Registers publisher
def register_pub(topic):
	pub_socket.bind("tcp://*:5556")
	pub_dict[topic] = pub_socket
	print("Registered pub on tcp://*:5556")

# Publishes data for the publisher based on the registered topic
def publish(topic, value, timestamp):
	if pub_dict.get(topic) != None:
		pub_dict.get(topic).send_string(value)
		print(f"Sending data to subscriber at {timestamp}")



## Functions for subscriber communication ##

# Registers subscriber
def register_sub(ip, topic_filter):
	sub_socket.connect("tcp://%s:5556" % ip)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	sub_dict[topic_filter] = sub_socket
	print("Listening to publisher at %s for %s" % (ip, topic_filter))

# Receives data for the subscriber based on the registered topic
def listen(topic):
	print("In listen")
	if sub_dict.get(topic) != None:
		print("Have socket for topic_filter %s, waiting for message" % (topic))
		string = sub_socket.recv_string()
		return string



## Functions for broker communication ##

# Registers the broker send and receive socks: 1. to get notified of all active pubs and subs,
# 2. to receive published messages, and 3. to send published messages to the subscribers
def register_broker():
	pub_listener_socket.bind("tcp://*:5554")

	sub_listener_socket.bind("tcp://*:5555")

def register_pub_with_broker(ip, topic):
	print("Registering to broker at tcp://%s:5554 with topic: %s" % (ip, topic))
	pub_broker_socket.connect("tcp://%s:5554" % ip)
	pub_dict[topic] = pub_broker_socket

#	tmp_socket = context.socket(zmq.REQ)
#	tmp_socket.connect("tcp://%s:5554" % ip)
#	tmp_socket.send("Registering ip %s topic: %s" % (pub_ip, topic))
#	resp = tmp_socket.recv()
#	if resp == "OK":
#		print("Registered publisher with broker")

def register_sub_with_broker(ip, topic_filter):
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5555" % ip)
	tmp_socket.send_string("Registering topic_filter %s" % (topic_filter))
	resp = tmp_socket.recv()
	#if resp == "OK":
	#	print( "Registered subscriber with broker")
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	sub_socket.connect("tcp://%s:%d" % (ip, int(resp)))
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	sub_dict[topic_filter] = sub_socket



def listen_for_pub_registration():
	resp = pub_listener_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")
	print("Got a publisher registration message: %s" % resp)
	_, _, ip = resp.split(' ', 2)

	print("Connecting to sub at: %s" % ip)

	sock = context.socket(zmq.SUB)
	sock.connect("tcp://%s:5556" % ip)

	# If it already exists, then the same pub will be trying to re-register, which should be fine
	pub_dict[ip] = sock

	ret = "ip %s topic %s" % (ip, topic)

	return ret

def listen_for_sub_registration():
	string = sub_listener_socket.recv()
	if isinstance(string, bytes):
		string = string.decode("ascii")
	_, _, topic_filter = string.split(' ')

	global sub_port

	if sub_dict.get(topic_filter) != None:
		print("Appending another sub listener for topic filter: %s" % topic_filter)
		#sock = context.socket(zmq.PUB)
		#sock.bind("tcp://*:%d" % sub_port)
		#sub_dict.get(topic_filter).update(sock)
	else:
		sock = context.socket(zmq.PUB)
		sock.bind("tcp://*:%d" % sub_port)
		sub_port_dict[topic_filter] = sub_port
		sub_port += 1
		print("Adding sub listener for topic filter: %s" % topic_filter)
		sub_dict[topic_filter] = {sock}

	resp = sub_port_dict.get(topic_filter)
	sub_listener_socket.send_string(str(resp))


def publish_to_broker(topic, value, message_number, timestamp):
	pub_dict.get(topic).send_string(value)
	print(f"Sending Data number {message_number} at {timestamp}")
	resp = pub_dict.get(topic).recv()
	if resp == "OK":
		print(f"Published data to the broker at {timestamp}")


def listen_for_pub_data():
	# May need to expand this to send back the port with the register message, rebuild the socket, and then listen on that port here
	print("Listening for pub data")
	string = pub_listener_socket.recv()
	if isinstance(string, bytes):
		string = string.decode("ascii")
	print("Received Data from pub: %s" % string)

	resp = "OK"
	pub_listener_socket.send_string(resp)

	return string

def publish_to_sub(data):
	for topic_filter, socks in sub_dict.items():
		if topic_filter in data:
			for sock in socks:
				sock.send_string(data)
				print("Sending Data")



#def notify(topic, value):
#	if sub_dict.get(topic) != None:






### Beavior Breakdown: ###
#
#
# Publishers will:
#	Register an ip/topic to get a socket
#	Publish data on that ip based on that topic
#	Don't care about broker or no broker - only difference will be the broker ip or the flood ip ("*")
#
#
# Subscribers will:
#   Register a socket for receiving
#   Register with a filter so they'll only get data that matches
#   Listen for data
#   Don't care about broker or no broker - only different will be if they send the broker ip or the flood ip ("*") (assuming the flood works for brokerless receiving)
#
#
# Broker will:
#	Register a sub socket to be informed about incoming publishers - pub ip and topic
#	Register a sub socket to be informed about incoming subscribers - sub ip and topic
#	Register an sub socket to get data from each publisher
#   Register an pbv socket to send data to each subscriber
#	Keep track of all pubs + topics
#	When forwarding to subs, it should use the topic it gets from the pub, and look up subs that filter on that
#	Then send to each of those subs
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
