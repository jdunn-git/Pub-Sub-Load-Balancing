import sys
import zmq
import _thread

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
pub_topic_filter_dict = dict()
sub_dict = dict()
sub_port_dict = dict()

pub_socket = context.socket(zmq.PUB)
sub_socket = context.socket(zmq.SUB)
pub_broker_socket = context.socket(zmq.REQ)

# Used by broker for pub/sub data transfer
broker_receive_socket = context.socket(zmq.SUB)
broker_send_socket = context.socket(zmq.PUB)

# Used by broker to listen for pub and sub connections
pub_listener_socket = context.socket(zmq.REP)
pub_discovery_socket = context.socket(zmq.REP)
sub_registration_socket = context.socket(zmq.REP)
pub_registration_socket = context.socket(zmq.REP)

# Used to heartbeat check the publishers from the broker
pub_heartbeat_socket = context.socket(zmq.REP)
heartbeat_sock_dict = dict()

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
def register_sub(ips, topic_filter):
	if ip in ips:
		tmp_socket.connect("tcp://%s:5556" % ip)
		tmp_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
		if sub_dict.get(topic_filter) == None:
			sub_dict[topic_filter] = {tmp_socket}
		else:
			sub_dict[topic_filter].append(tmp_socket)
		print("Listening to publisher at %s for %s" % (ip, topic_filter))

# Receives data for the subscriber based on the registered topic
# If there are multiple publishers, it will listen for data in a round-robin format
def listen(topic):
	print("In listen")
	if sub_dict.get(topic)[listen_count%len(sub_dict.get(topic))] != None:
		list_count += 1
		print("Have socket for topic_filter %s, waiting for message" % (topic))
		string = sub_socket.recv_string()
		return string



## Functions for broker communication ##

# Registers the broker send and receive socks: 1. to get notified of all active pubs and subs,
# 2. to receive published messages, and 3. to send published messages to the subscribers
def register_broker():
	pub_discovery_socket.bind("tcp://*:5552")

	pub_listener_socket.bind("tcp://*:5553")

	pub_registration_socket.bind("tcp://*:5554")
	sub_registration_socket.bind("tcp://*:5555")

def register_pub_with_broker(ip, topic):
	print("Registering to broker at tcp://%s:5554 with topic: %s" % (ip, topic))

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	hostname = socket.gethostname()
	local_ip = socket.gethostbyname(hostname)
	tmp_socket.send_string("Registering topic_filter %s ip %s" % (topic, local_ip))
	resp = tmp_socket.recv()

	if resp == "OK":
	#	print( "Registered subscriber with broker")
		pub_broker_socket.connect("tcp://%s:5553" % ip)
		pub_dict[topic] = pub_broker_socket
		pub_heartbeat_socket.bind("tcp://*.5550")
		_thread.start_new_thread(heartbeat_response, ())


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

def discover_publishers(ip, topic_filter):
	print(f"Trying to discover a publisher with topic filter {topic_filter} through broker at tcp://{ip}:5554")
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5552" % ip)
	tmp_socket.send_string("Registering topic_filter %s" % (topic_filter))
	resp = tmp_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	# resp should be a string containing all pub IPs publishing this topic
	pub_ips = resp.split()
	return pub_ips

def listen_for_pub_discovery_req():
	req = pub_discovery_socket.recv()
	if isinstance(req, bytes):
		req = req.decode("ascii")
	print("Got a publisher discovery request: %s" % req)
	_,  _, topic_filter = req.split(' ')

	if pub_topic_filter_dict.get(topic_filter) != None:
		# TODO: Enable this to message to send multiple pubs 
		for ip in pub_topic_filter_dict.get(topic_filter):
			# This will double check that all pubs are still active before sending to the sub
			perform_heartbeat_check(ip)

		# Note: There is a small race condition where a pub goes down between the heartbeat check and here, but
		# it is miniscule and likely not worth the time to solve
		pub_discovery_socket.send_string(tmp.join(pub_topic_filter_dict.get(topic_filter)))
		pub_discovery_socket.recv()
	else:
		# TODO: Enable subs to be waiting for pubs, so this wont return a 404
		# Alternatively, I could solve this on the sub side.
		pub_discovery_socket.send_string("404")
		pub_discovery_socket.recv()


def listen_for_pub_registration():
	resp = pub_registration_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")
	print("Got a publisher registration message: %s" % resp)
	_, _, topic_filter, _, ip = resp.split(' ', 2)

	# If it already exists, then the same pub will be trying to re-register, which should be fine
	pub_dict[ip] = sock
	if pub_topic_filter_dict.get(topic_filter) == None:
		pub_topic_filter_dict[topic_filter] = {ip}
	else:
		pub_topic_filter_dict[topic_filter].append(ip)

	# Start heartbeat socket for this pub
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
	tmp_socket.setsockopt(zmq.LINGER, 0)
	tmp_socket.connect("tcp://%s:5550" % ip)
	heartbeat_sock_dict[ip] = tmp_socket

	pub_registration_socket.send("OK")

	ret = "ip %s topic %s" % (ip, topic)

	return ret

def listen_for_sub_registration():
	string = sub_registration_socket.recv()
	if isinstance(string, bytes):
		string = string.decode("ascii")
	_, _, topic_filter = string.split(' ')

	global sub_port

	if sub_dict.get(topic_filter) != None:
		# Note that since this sock is a PUB/SUB, I don't need to do anything to 'append' a new sub
		# because the same socket will broadcast to all subs
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
	sub_registration_socket.send_string(str(resp))


def publish_to_broker(topic, data, message_number, timestamp):
	print(f"Sending Data number {message_number} at {timestamp}")
	pub_dict.get(topic).send_string(f"Publishing {data}")
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

def perform_heartbeat_check(ip):
	if heartbeat_sock_dict.get(ip) != None:
		try:
			heartbeat_sock_dict.get(ip).send_string("Heartbeat check")
			heartbeat_sock_dict.get(ip).recv()
		# This exception should be hit if the socket is closed on the other end
		except:
			heartbeat_sock_dict.remove(ip)
			# Get the data socket to the pub so that we can close it
			sock = pub_dict[ip]

			# Will cause undelivered messages to linger for only 500 milliseconds
			sock.close(linger=500)

			# Remove the ip from the current maps that reference it
			pub_dict.remove(ip)
			pub_topic_filter_dict.get(topic_filter).remove(ip)

def heartbeat_response():
	resp = pub_heartbeat_socket.recv()
	pub_heartbeat_socket.send_string("OK")




#
# TODO:
#
# 1. Make broker run always
# 2. Let broker have a normal and a "discovery" mode
#	> This will require req-rep sockets for pub and sub registration
#	> Also need to be aware of topics for sub registration now
#	> This may also require moving to an XSUB socket for subs, but I'm not sure 
# 3. Make "heartbeat" requests in "discovery" mode to verify if pubs are still active,
#	and remove them if they don't respond	
# 	> Better yet, let the sub connecting to the pub be the "heartbeat", so that if
#		that request fails, it comes back to the broker and tells it about that
# 4. Connect broker to zookeeper, and add leader selection
# 5. Connect pub and sub to zookeeper to find broker leader
# 6. Update automated scripts for broker to be always on
#