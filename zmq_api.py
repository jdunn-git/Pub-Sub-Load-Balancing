import sys
import zmq

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
sub_dict = dict()

pub_socket = context.socket(zmq.PUB)
sub_socket = context.socket(zmq.SUB)


def register_pub(ip, topic):
	pub_socket.bind("tcp://%s:5556" % ip)
	pub_dict[topic] = pub_socket


def register_sub(ip, topic_filter):
	sub_socket.connect("tcp://%i:5556" % ip)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	sub_dict[topic_filter] = sub_socket


def publish(topic, value):
	if pub_dict.get(topic) != None:
		pub_socket.send_string(value)
		print("Sending Data")


def listen(topic):
	if sub_dict.get(topic) != None:
		sub_socket = sub_dict.get(topic)
		string = sub_dict.get(topic).recv_string()
		return string


#def notify(topic, value):
#	if sub_dict.get(topic) != None:

