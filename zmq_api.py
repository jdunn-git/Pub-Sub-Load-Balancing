import sys
import zmq
import socket

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
sub_dict = dict()

pub_socket = context.socket(zmq.PUB)
sub_socket = context.socket(zmq.SUB)

broker_receive_socket = context.socket(zmq.SUB)
broker_send_socket = context.socket(zmq.PUB)

pub_listener_socket = context.socket(zmq.REP)
sub_listener_socket = context.socket(zmq.REP)

# TODO: Test and make sure that all subs will register with all pubs for that topic, and all pubs will send to all pubs
#			- This may just require defaulting to using "*" for subs, but it could be as complicated as adding new connects for the sub dynamically
#			- Or, it could just require re-connecting
# TODO: Handle dynamic arrivals and departures



## Functions for publisher communication ##

# Registers publisher
def register_pub(ip, topic):
	pub_socket.bind("tcp://%s:5556" % ip)
	pub_dict[topic] = pub_socket

# Publishes data for the publisher based on the registered topic
def publish(topic, value):
	if pub_dict.get(topic) != None:
		pub_dict.get(topic).send_string(value)
		print("Sending Data")



## Functions for subscriber communication ##

# Registers subscriber
def register_sub(ip, topic_filter):
	sub_socket.connect("tcp://%s:5556" % ip)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	sub_dict[topic_filter] = sub_socket

# Receives data for the subscriber based on the registered topic
def listen(topic):
	if sub_dict.get(topic) != None:
		string = sub_dict.get(topic).recv_string()
		return string



## Functions for broker communication ##

# Registers the broker send and receive socks: 1. to get notified of all active pubs and subs, 
# 2. to receive published messages, and 3. to send published messages to the subscribers 
def register_broker():
	pub_listener_socket.bind("tcp://*:5554")

	sub_listener_socket.bind("tcp://*:5555")

def register_pub_with_broker(ip, topic):
	pub_socket.bind("tcp://%s:5556" % ip)
	pub_dict[topic] = pub_socket

	pub_ip = socket.gethostbyname(socket.gethostname())

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	tmp_socket.send_string("Registering ip %s topic: %s" % (pub_ip, topic))
	resp = tmp_socket.recv()
	if resp == "OK":
		print("Registered publisher with broker")

def register_sub_with_broker(ip, topic):
	sub_socket.connect("tcp://%s:5556" % ip)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	sub_dict[topic_filter] = sub_socket

	sub_ip = socket.gethostbyname(socket.gethostname())

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5555" % ip)
	tmp_socket.send_string("Registering ip %s topic %s" % (sub_ip, topic))
	resp = tmp_socket.recv()
	if resp == "OK":
		print("Registered subscriber with broker")


def listen_for_pub_registration():
	resp = pub_listener_socket.recv()
	_, _, ip, _, topic = resp.split()

	sock = context.socket(zmq.SUB)
	sock.bind("tcp://%s:5556" % ip)

	# If it already exists, then the same pub will be trying to re-register, which should be fine
	pub_dict.[ip] = sock

	ret = "ip %s topic %s" % (ip, topic)

	return ret

def listen_for_sub_registration():
	string = sub_listener_socket.recv()
	_, _, ip, _, topic = string.split()

	sock = context.socket(zmq.PUB)
	sock.bind("tcp://%s:5556" % ip)

	if sub_dict.get(topic) != None:
		sub_dict.get(topic).update(sock)
	else:
		sub_dict[topic] = {sock}


def listen_for_pub_data(ip):
	if pub_dict.get(ip) != None
	string = pub_dict.get(ip).recv_string()

	return string

def publish_to_sub(topic):
	if sub_dict.get(topic) != None:
		for sock in sub_dict.get(topic):
			sock.send_string(value)
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


