import sys
import zmq
import _thread
import netifaces
import ipaddress
import time
import json
from threading import Lock, Thread

import zmq_api_zkclient as zk

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
pub_topic_dict = dict()
sub_dict = dict()
sub_topic_dict = dict()
sub_port_dict = dict()


# Keeps track of published messages like a ring buffer.
# The pub_history_count will be used in modulo arithmetic
# for list updates
pub_history_count = 0
published_messages_count = 0
published_message_history = []

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
new_pub_sock = context.socket(zmq.PUB)

# Used to listen for addition pubs registering
sub_new_pub_socket = context.socket(zmq.SUB)

# Used to heartbeat check the publishers from the broker
pub_heartbeat_socket = context.socket(zmq.REP)
heartbeat_sock_dict = dict()

# Used for new broker information
new_broker_listener_socket = context.socket(zmq.SUB)
new_broker_publisher_socket = context.socket(zmq.PUB)

# Starting value for the ports used by subs int this api
sub_port = 5556
# Value to connect to broker from the sub
broker_port = 5556

# Values needed for sub listening coordination
sync_listen_count = 0
rr_listen_count = 0
listen_count_lock = Lock()
listening_lock = Lock()
listening_state = ""
listening_threads = []
new_listening_threads = [] # new threads for pubs added after initial discovery
pub_ips = []
broker_ip = ""
monitor_broker = True

sub_new_pub_listener_thread = Thread()
sub_thread_end = False

pub_lock = Lock()

# Zookeeper
driver = zk.ZK_Driver('127.0.0.1',2181)

terminate_threads = False

def close_context():
	context.destroy()

## Functions for publisher communication ##

# Registers publisher
def register_pub(ip, topic, topic_filter, history):
	print("Registering to broker at tcp://%s:5554 with topic/filter %s/%s" % (ip, topic, topic_filter))

	global pub_history_count
	pub_history_count = history
	global published_message_history
	published_message_history = [""] * history

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string(f"Registering flood {topic} {topic_filter} {history} {local_ip}")
	resp = tmp_socket.recv()

	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	pub_socket.bind("tcp://*:5556")
	pub_dict[topic] = pub_socket
	print("Registered pub on tcp://*:5556")
	pub_heartbeat_socket.bind("tcp://*:5550")
	_thread.start_new_thread(heartbeat_response, ())

	#print(f"listening for history requests on {topic}")
	#pub_history_listener_thread = Thread(target=listen_for_history_requests, args=(topic,))
	#pub_history_listener_thread.start()



# Publishes data for the publisher based on the registered topic
def publish(topic, topic_filter, value, message_number, timestamp):
	global pub_history_count
	global published_message_history
	global published_messages_count
	global pub_lock
	have_lock = False
	try:
		pub_lock.acquire()
		have_lock = True

		#published_messages_count = message_number
		published_messages_count += 1
		published_message_history[message_number % pub_history_count] = value

		if pub_dict.get(topic) != None:

			print(f"Sending data to subscriber at {timestamp} for topic {topic}_{topic_filter}")
			print(published_message_history)

			# Generate JSON object of messages
			tmp_data = dict()
			data = json.dumps
			index = message_number
			i = 0
			while i < pub_history_count:
				message = published_message_history[index % pub_history_count]
				if len(message) > 0:
					#pub_dict.get(topic).send_string(published_message_history[index % pub_history_count])
					# This will constuct the JSON object backwards
					tmp_data[i] = message
				index -= 1
				i += 1
			send_str = f"{topic_filter} " + json.dumps(tmp_data)
			print(f"Actually publishing: {send_str}")
			pub_dict.get(topic).send_string(send_str)


	finally:
		if have_lock:
			pub_lock.release()
			have_lock = False

def listen_for_history_requests(topic):
	global pub_lock

	pub_history_socket = context.socket(zmq.REP)
	pub_history_socket.setsockopt(zmq.RCVTIMEO, 1200)

	lock_held = False
	while not terminate_threads:
		print(f"Still listening for more history requests: terminate: {terminate_threads}")
		try:
			pub_history_socket.bind(f"tcp://*:5549")

			print("Checking for history request")
			#history_count = pub_history_socket.recv_string(flags=zmq.NOBLOCK)
			history_count = pub_history_socket.recv_string()
			print("Got a history request")
			pub_lock.acquire()
			lock_held = True
			time.sleep(0.5)

			#print(f"Done checking for new pubs {resp}")
			if isinstance(history_count, bytes):
				history_count = history_count.decode("ascii")
			print(history_count)
			print(f"Resending previous {history_count} messages")
			send_history(int(history_count), topic)

			print(f"Done resending previous {history_count} messages")
			pub_history_socket.send_string("OK")
		except Exception as ex:
			print(f"Exception: {ex}")
			if not terminate_threads:
				pub_history_socket = context.socket(zmq.REP)
				pub_history_socket.setsockopt(zmq.RCVTIMEO, 1200)
			time.sleep(0.5)
		finally:
			if lock_held:
				pub_lock.release()
				lock_held = False
	print(f"Done listening for more history requests: {terminate_threads}")


def send_history(count, topic):
	global pub_history_count
	global published_messages_count
	global published_message_history
	global pub_dict

	#print(f"Sending {count} messages for topic {topic}")

	index = published_messages_count - count + 1
	endIndex = index + pub_history_count
	if pub_dict.get(topic) != None:
		while index < endIndex:
			message = published_message_history[index % pub_history_count]
			if len(message) > 0:
				print(f"Resending message {index}")
				sock = pub_dict.get(topic)
				sock.send_string(message)
			else:
				print(f"Message {index} had a zero length: {message}.")
				#pub_dict.get(topic).send_string(message)
			#print(f"Sending data to subscriber at {timestamp}")
			index += 1
			time.sleep(0.05)

## Functions for subscriber communication ##

# Registers subscriber
def register_sub(broker, ips, topic, topic_filter, process_response, max_listens):
	global broker_ip

	print(f"Registering sub with topic filter {topic_filter} from {broker}")
	for ip in ips:
		tmp_socket = context.socket(zmq.SUB)
		print(f"filtering for {topic} from {ip}")
		tmp_socket.connect("tcp://%s:5556" % ip)
		tmp_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
		if sub_dict.get(topic_filter) == None:
			sub_dict[topic_filter] = [tmp_socket]
		else:
			sub_dict[topic_filter].append(tmp_socket)
		print("Listening to publisher at %s for %s" % (ip, topic_filter))

		# Request historic data to be sent
		#print("Requesting historic data from publisher")
		#tmp_socket = context.socket(zmq.REQ)
		#tmp_socket.connect("tcp://%s:5549" % ip)
		#tmp_socket.send_string(str(max_listens))
		#resp = tmp_socket.recv()
		#print(resp)

	broker_ip = broker
	# Not needed anymore for assignment 3
	#print(f"listening for new pubs on {topic} from {broker_ip}")
	#sub_new_pub_socket.connect(f"tcp://{broker_ip}:5551")
	#sub_new_pub_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
	#sub_new_pub_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
	#sub_new_pub_listener_thread = Thread(target=listen_for_new_pubs, args=(topic, topic_filter, process_response, max_listens))
	#sub_new_pub_listener_thread.start()

# Receives data for the subscriber based on the registered topic
def listen(topic_filter, index, broker_mode):
	global broker_port
	print("In listen")
	success = False
	while not success:
		try:
			print("Listening for data!!")
			sock = sub_dict.get(topic_filter)[index]
			if sock != None:
				print("Have socket for topic_filter %s, waiting for message" % (topic_filter))
				string = sock.recv_string()
				string = string[len(topic_filter)+1:]
				success = True
				return string
		except:
			print("Didn't see any data!!")
			# The broker may have gone down, wait a second and reconnect
			sock = sub_dict.get(topic_filter)[index]
			sock.close()
			sock = context.socket(zmq.SUB)
			sock.setsockopt(zmq.RCVTIMEO, 1000) # milliseconds
			sock.setsockopt(zmq.LINGER, 0)
			sock.setsockopt_string(zmq.SUBSCRIBE, "")
			sock.connect(f"tcp://{broker_ip}:{broker_port}")
			sub_dict.get(topic_filter)[index] = sock
			#sub_dict[topic_filter] = [sub_socket]

			print(f"reconnected to {broker_ip}:{broker_port} in case broker went down")
			time.sleep(0.4)


def synchronized_listen_helper(topic, topic_filter, sock, process_response, max_listens, index):
	print("In listen helper")
	global sync_listen_count
	global pub_ips
	if sock != None:
		while sync_listen_count < max_listens:
			try:
				print(f"Have socket for topic_filter {topic_filter}, waiting for message {sync_listen_count}")
				#sock.setsockopt(ZMQ_RCVTIMEO, 500);
				#string = sock.recv_string()
				string = sock.recv_string(flags=zmq.NOBLOCK)
				#print(f"{string}")
				listen_count_lock.acquire()
				# Double checking this here in case any race conditions I haven't planned for exist
				if sync_listen_count >= max_listens:
					listen_count_lock.release()
					break;
				process_response(string,sync_listen_count)
				sync_listen_count += 1
				listen_count_lock.release()
			except:
				sock.close()
				sock = context.socket(zmq.SUB)
				#print(pub_ips)
				ip = pub_ips[index]
				sock.connect(f"tcp://{ip}:5556")
				print(f"filtering for new published message from {ip} on {topic_filter}")
				sock.setsockopt_string(zmq.SUBSCRIBE, f"{topic_filter}")
				time.sleep(0.4)


def synchronized_listen(topic, topic_filter, process_response, max_listens):
	print("In synchronized listen")
	global listening_lock
	global new_listening_threads
	global sub_thread_end
	listening_lock.acquire()
	listening_state = "active"
	print("Active listening is happening")
	ips = sub_dict.get(topic_filter)
	listening_lock.release()

	if ips != None:
		for i in range(len(ips)):
			print(f"Starting new thread {i}")
			#_thread.start_new_thread(synchronized_listen_helper,(topic_filter, 0, process_response, max_listens))
			sock = sub_dict.get(topic_filter)[i]
			t = Thread(target=synchronized_listen_helper, args=(topic, topic_filter, sock, process_response, max_listens, i))
			t.start()
			listening_threads.append(t)

	count = 0
	while count < max_listens:
		listen_count_lock.acquire()
		count = sync_listen_count
		listen_count_lock.release()

		time.sleep(0.5) # Continuously sleep for 500 milliseconds to let new pubs join
		#print("Waiting for pub to join...")

	# Join all threads
	for t in listening_threads:
		t.join()

	print("Done with original listen threads, only need to finish joining new threads")

	listening_lock.acquire()
	for t in new_listening_threads:
		t.join()
	listening_state = "done"
	print("Done active listening")
	listening_lock.release()

	sub_thread_end = True

# If there are multiple publishers, it will listen for data in a round-robin format
def round_robin_listen(topic_filter):
	print("In listen")
	sock = sub_dict.get(topic_filter)[rr_listen_count%len(sub_dict.get(topic_filter))]
	if sock != None:
		rr_listen_count += 1
		print("Have socket for topic_filter %s, waiting for message" % (topic_filter))
		string = sock.recv_string()
		return string


# New publishers can be added, and this will make sure the pub sees this
def listen_for_new_pubs(pub_topic, topic_filter, process_response, max_listens):
	global listening_lock
	global new_listening_threads
	global sub_new_pub_socket
	global broker_ip
	global pub_ips
	while not sub_thread_end:
		topic = ""
		ip = ""
		try:
			print("Checking for new pubs")
			resp = sub_new_pub_socket.recv_string(flags=zmq.NOBLOCK)
			print(f"Done checking for new pubs {resp}")
			if isinstance(resp, bytes):
				resp = resp.decode("ascii")
			print(resp)
			topic, ip = resp.split(' ')
		except:
			sub_new_pub_socket = context.socket(zmq.SUB)
			sub_new_pub_socket.connect(f"tcp://{broker_ip}:5551")
			print(f"filtering for new pub on {pub_topic}_{topic_filter}")
			sub_new_pub_socket.setsockopt_string(zmq.SUBSCRIBE, f"{pub_topic}_{topic_filter}")
			time.sleep(0.5)
			continue

		if ip not in pub_ips:
			print(f"Adding a new sub for {ip}")
			pub_ips.append(ip)
			tmp_socket = context.socket(zmq.SUB)
			tmp_socket.connect("tcp://%s:5556" % ip)
			tmp_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
			if sub_dict.get(topic_filter) == None:
				sub_dict[topic_filter] = [tmp_socket]
			else:
				sub_dict[topic_filter].append(tmp_socket)
			print("Listening to new publisher at %s for %s" % (ip, topic_filter))

			listening_lock.acquire()
			if listening_state != "done":
				print("Active listening is happening, adding thread")
				t = Thread(target=synchronized_listen_helper, args=(pub_topic, topic_filter, tmp_socket, process_response, max_listens, len(pub_ips)-1))
				t.start()
				new_listening_threads.append(t)
			else:
				print("No active listening")

			listening_lock.release()

## Functions for broker communication ##

# Registers the broker send and receive socks: 1. to get notified of all active pubs and subs,
# 2. to receive published messages, and 3. to send published messages to the subscribers
def register_broker(zk_ip, zk_port):
	pub_discovery_socket.bind("tcp://*:5552")

	pub_listener_socket.bind("tcp://*:5553")

	pub_registration_socket.bind("tcp://*:5554")
	sub_registration_socket.bind("tcp://*:5555")

	new_pub_sock.bind("tcp://*:5551")

	add_broker(zk_ip, zk_port)


def register_pub_with_broker(ip, topic, topic_filter, history):
	print("Registering to broker at tcp://%s:5554 with topic/filter: %s/%s" % (ip, topic, topic_filter))

	global pub_history_count
	pub_history_count = history
	global published_message_history
	published_message_history = [""] * history

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string(f"Registering broker {topic} {topic_filter} {history} {local_ip}")
	resp = tmp_socket.recv()

	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	pub_broker_socket.connect("tcp://%s:5553" % ip)
	pub_broker_socket.setsockopt( zmq.RCVTIMEO, 500 ) # milliseconds
	pub_broker_socket.setsockopt(zmq.LINGER, 0)
	pub_dict[topic] = pub_broker_socket
	pub_heartbeat_socket.bind("tcp://*:5550")
	_thread.start_new_thread(heartbeat_response, ())
	return resp

def register_sub_with_broker(ip, topic, topic_filter, history):
	global broker_port
	print(f"Registering sub with broker {ip}")
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5555" % ip)
	tmp_socket.send_string(f"Registering {topic} {topic_filter} history {history}")
	resp = tmp_socket.recv()
	#if resp == "OK":
	#	print( "Registered subscriber with broker")
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	broker_port = resp
	print(f"Registering sub to {ip}:{broker_port}")
	sub_socket.connect("tcp://%s:%d" % (ip, int(broker_port)))
	sub_socket.setsockopt( zmq.RCVTIMEO, 1000 ) # milliseconds
	sub_socket.setsockopt(zmq.LINGER, 0)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
	sub_dict[topic_filter] = [sub_socket]

def discover_publishers(ip, topic, count):
	global pub_ips
	print(f"Trying to discover (a) publisher(s) with topic {topic} through broker at tcp://{ip}:5554")
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5552" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string(f"Registering messages {count} topic {topic} {local_ip}")
	resp = tmp_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	if resp == "404":
		print("No publishers for now")
		return []

	# resp should be a string containing all pub IPs publishing this topic
	#for ip in resp:
	#	pub_ips.append(ip)
	pub_ips = resp.split()
	print(f"Found broker(s): {pub_ips})")
	return pub_ips

def listen_for_pub_discovery_req():
	req = pub_discovery_socket.recv()
	if isinstance(req, bytes):
		req = req.decode("ascii")
	print("Got a publisher discovery request: %s" % req)
	_, _, messages, _, topic, ip = req.split(' ')

	#TODO: Do something with this history
	print(f"pub_topic_dict: {pub_topic_dict}")

	if pub_topic_dict.get(topic) != None:
		pub_ips = pub_topic_dict.get(topic).copy()
		print(pub_ips)
		#for ownership_history_ip in pub_ips:
		#	_, _, ip = ownership_history_ip.split('_')
			# This will double check that all pubs are still active before sending to the sub
		#	perform_heartbeat_check(ip, topic)
		#pub_ips = pub_topic_dict.get(topic).copy()

		# Note: There is a small race condition where a pub goes down between the heartbeat check and here, but
		# it is miniscule and likely not worth the time to solve
		#delim = ' '
		#print(pub_topic_dict.get(topic))
		#pubs = delim.join(pub_topic_dict.get(topic))
		#pubs = pubs.strip()
		#print(pubs)
		#pub_discovery_socket.send_string(pubs)

		# For Assignment 3, we need to get just one pub and send it

		sent = False
		print(f"Verifying {pub_ips}")
		for ownership_history_ip in pub_ips:
			ownership, history, ip = ownership_history_ip.split('_')
			# Since lists are ordered in python, we can go through this list in order
			# to check the strongest ownership first
			print(f"Checking {history} >= {messages}")
			if int(history) >= int(messages):
				pub_discovery_socket.send_string(ip)
				sent = True
				break

		if not sent:
			pub_discovery_socket.send_string("404")

	else:
		pub_discovery_socket.send_string("404")

	# Keep a dict of sub ips that have request this topic for future updates
	#if sub_topic_dict.get(topic) == None:
	#	sub_topic_dict[topic] = {ip}
	#else:
	#	sub_topic_dict.get(topic).add(ip)

	#update_zk("add",f"/sub_topic_dict/{topic}",f"{ip}")

def listen_for_pub_registration():
	global pub_ips
	#try:
	print("Listening for publisher registration message")
	#pub_registration_socket = context.socket(zmq.REP)
	#pub_registration_socket.bind("tcp://*:5554")
	#pub_registration_socket.setsockopt(zmq.RCVTIMEO, 500) # milliseconds

	resp = pub_registration_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")
	print("Got a publisher registration message: %s" % resp)
	resp = resp.split(' ')
	ip = resp[len(resp)-1]
	history = resp[len(resp)-2]

	history_ip = f"{history}_{ip}"
	ownership_strength = 1
	ownership_history_ip = f"0_{history_ip}"
	mode = resp[1]
	topics = resp[2:len(resp)-3]
	topic_filter = resp[len(resp)-3]
	topic_key = ""

	print(f"pub_topic_dict: '{pub_topic_dict}'")
	for topic in topics:
		topic = f"{topic}_{topic_filter}"
		topic_key = topic
		# Determine the ownership strength based on how many ips there currnelty are
		if pub_topic_dict.get(topic) == None:
			ownership_history_ip = f"1_{history_ip}"
			print(f"Assigning ownership_strength: {ownership_history_ip}")
			pub_topic_dict[topic] = [ownership_history_ip]
		else:
			if len(pub_topic_dict.get(topic)) == 0:
				ownership_strength = "1"
			else:
				last_ownership_string, _, _ = pub_topic_dict.get(topic)[len(pub_topic_dict.get(topic))-1].split('_')
				ownership_strength = str(int(last_ownership_string)+1)
			ownership_history_ip = f"{str(ownership_strength)}_{history_ip}"
			print(f"Assigning ownership_strength: {ownership_history_ip}")
			pub_topic_dict[topic].append(ownership_history_ip)

		update_zk("add",f"/pub_topic_dict/{topic}",f"{ownership_history_ip}")

		# Notify sub listener when in flood mode
		if mode == "flood":
			# TODO: Only send this if the pub history is high enough
			string = f"{topic} {ip}"
			print(f"publishing new pub info {string}")
			new_pub_sock.send_string(string)

	if ip not in pub_ips:
		pub_ips.append(ip)
		# Start heartbeat socket for this pub
		tmp_socket = context.socket(zmq.REQ)
		tmp_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
		tmp_socket.setsockopt(zmq.LINGER, 0)
		print(f"IP is: {ip}")
		tmp_socket.connect("tcp://%s:5550" % ip)
		heartbeat_sock_dict[ip] = tmp_socket

		pub_registration_socket.send_string(str(ownership_strength))

		# Start background heartbeat check
		start_background_perform_heartbeat_check(ip)

	ret = "ip %s topics %s" % (ip, topics)

	return ret

def listen_for_sub_registration():
	print("Listening for subscriber registration message")
	string = sub_registration_socket.recv()
	if isinstance(string, bytes):
		string = string.decode("ascii")
	_, topic, topic_filter, _, history = string.split(' ')

	global sub_port

	if sub_dict.get(topic_filter) != None:
		if sub_dict.get(topic_filter).get(history) != None:
			# Note that since this sock is a PUB/SUB, I don't need to do anything to 'append' a new sub
			# because the same socket will broadcast to all subs
			print("Appending another sub listener for topic filter: %s" % topic_filter)
		else:
			sock = context.socket(zmq.PUB)
			print(f"Adding sub listener for topic filter {topic_filter} at port {sub_port} for history {history}")
			sock.bind("tcp://*:%d" % sub_port)
			sub_port_dict[topic_filter][history] = sub_port
			sub_port += 1
			sub_dict[topic_filter] = dict()
			sub_dict.get(topic_filter)[history] = [sock]
			update_zk("add",f"/sub_dict/{history}/{topic_filter}",f"{sub_port}")

		#sock = context.socket(zmq.PUB)
		#sock.bind("tcp://*:%d" % sub_port)
		#sub_dict.get(topic_filter).update(sock)
	else:
		sock = context.socket(zmq.PUB)
		print(f"Adding sub listener for topic filter {topic_filter} at port {sub_port} for history {history}")
		sock.bind("tcp://*:%d" % sub_port)
		sub_port_dict[topic_filter] = dict()
		sub_port_dict[topic_filter][history] = sub_port
		sub_port += 1
		sub_dict[topic_filter] = dict()
		sub_dict.get(topic_filter)[history] = [sock]
		update_zk("add",f"/sub_dict/{history}/{topic_filter}",f"{sub_port}")

	resp = sub_port_dict.get(topic_filter).get(history)
	print(f"Telling sub to register to port {resp}")
	sub_registration_socket.send_string(str(resp))

	# Request historic data to be sent
	topic_key = f"{topic}_{topic_filter}"
	pub_ips = pub_topic_dict[topic_key]
	print(f"pub_topic_dict: {pub_topic_dict}, topic_key: {topic_key}, pub_ips: {pub_ips}")
	sent = False
	# Since lists in python are ordered, this will go through ownership strengths starting
	# from the strongest ownership strength to the weakest
	for val in pub_ips:
		ownership_strength, history_value, ip = val.split('_')
		print(f"Checking {history_value} >= {history}")
		if int(history_value) >= int(history):
	#		print(f"Requesting historic data from publisher to {ip}")
	#		tmp_socket = context.socket(zmq.REQ)
	#		tmp_socket.connect("tcp://%s:5549" % ip)
	#		print("Sending request")
	#		tmp_socket.send_string(history)
	#		print("Checking for response")
	#		resp = tmp_socket.recv()
	#		print(f"Got response: {resp}")
			sent = True
			break

	if not sent:
		print(f"No publisher found with history at least {history} for {topic_filter}")


def publish_to_broker(topic, topic_filter, data, message_number, ownership_strength, timestamp):
	global pub_history_count
	global published_message_history
	global published_messages_count
	global pub_lock
	if pub_dict.get(topic) != None:
		success = False
		print(f"Sending Data number {message_number} at {timestamp}")
		published_messages_count += 1
		published_message_history[message_number % pub_history_count] = data

		while not success:
			try:

				#published_messages_count = message_number
				print(f"Sending data to subscriber at {timestamp} for topic {topic}_{topic_filter}")
				print(published_message_history)

				# Generate JSON object of messages
				tmp_data = dict()
				data = json.dumps
				index = message_number
				i = 0
				while i < pub_history_count:
					message = published_message_history[index % pub_history_count]
					if len(message) > 0:
						#pub_dict.get(topic).send_string(published_message_history[index % pub_history_count])
						# This will constuct the JSON object backwards
						tmp_data[i] = message
					index -= 1
					i += 1
				send_str = f"{ownership_strength} {pub_history_count} {topic}_{topic_filter} {topic_filter} " + json.dumps(tmp_data)
				print(f"Actually publishing: {send_str}")
				pub_dict.get(topic).send_string(send_str)

				#pub_dict.get(topic).send_string(data)
				resp = pub_dict.get(topic).recv()


				if isinstance(resp, bytes):
					resp = resp.decode("ascii")
				if resp == "OK":
					success = True
					print(f"Published data to the broker at {timestamp}")
			except Exception as ex:
				print(f"** Exception!: {ex}")
				# The broker may have gone down, wait a second and rebroadcast
				pub_dict.get(topic).close()
				sock = context.socket(zmq.REQ)
				sock.setsockopt(zmq.RCVTIMEO, 1000) # milliseconds
				sock.setsockopt(zmq.LINGER, 0)
				sock.connect(f"tcp://{broker_ip}:5553")
				pub_dict[topic] = sock

				time.sleep(1)
				print(f"Broker may have gone down... rebroadcasting to {broker_ip}.")

def listen_for_pub_data():
	# May need to expand this to send back the port with the register message, rebuild the socket, and then listen on that port here
	print("Listening for pub data")
	string = pub_listener_socket.recv()
	if isinstance(string, bytes):
		string = string.decode("ascii")
	print("Received Data from pub: %s" % string)

	resp = "OK"
	pub_listener_socket.send_string(resp)

	ownership_strength, history_count, filter_key, data = string.split(' ', 3)

	return ownership_strength, history_count, filter_key, data

def publish_to_sub(ownership_strength, history_count, filter_key, data):
	for topic_filter, histories in sub_dict.items():
		if topic_filter in data:
			for history, socks in histories.items():
				if int(history_count) >= int(history):
					# If the required history is more than the sending pub, go to the next history set
					if int(history) > int(history_count):
						print(f"History value {history_count} not strong enough to send. Need at least {history}")
						continue
					sent = False
					print(f"*** All vals: {pub_topic_dict.get(filter_key)}")
					# Check if this pub has the largest ownership strength for at least this history
					for val in pub_topic_dict.get(filter_key):
	
						own_str, hist, ip = val.split('_')
						# Since this list is ordered, if we get a matching ownership strength before
						# we get a higher history value, then we know the sending pub has the highest 
						# strength at this history value for this topic 
						if own_str == ownership_strength:
							# SEND
							for sock in socks:
								sock.send_string(data)
								print(f"Sending Data to sub from {ownership_strength}")
								sent = True
						elif own_str < ownership_strength and int(hist) >= int(history):
							# If we have a stronger owner with a good enough history, then don't send
							print(f"There is a stronger owner than {ownership_strength} with a good enough history")
							break 

					if not sent:
						print(f"Not the strongest owner with a strong enough history value. Strength value is {ownership_strength}")

def start_background_perform_heartbeat_check(ip):
	thread = Thread(target=background_heartbeat_check, args=(ip,))
	thread.start()

def background_heartbeat_check(ip):
	global pub_ips
	#ownership_strength, history_value, ip = pub_topic_value.split('_')
	while not terminate_threads:
		if heartbeat_sock_dict.get(ip) != None:
			#print(f"performing heartbeat check on {ip}")
			try:
				heartbeat_sock_dict.get(ip).send_string("Heartbeat check")
				heartbeat_sock_dict.get(ip).recv()
				#print(f"heartbeat check passed {ip} for {topic}")
			# This exception should be hit if the socket is closed on the other end
			except:
				print(f"heartbeat check failed, removing {ip}")
				del heartbeat_sock_dict[ip]

				for topic in pub_topic_dict:
					for value in pub_topic_dict.get(topic):
						_, _, tmp_ip = value.split('_')
						if ip == tmp_ip:
							pub_topic_dict.get(topic).remove(value)
							update_zk("delete",f"/pub_topic_dict/{topic}",f"{value}")
				pub_ips.remove(ip)
				return

			# Sleep for a split second
			time.sleep(0.2)

		else:
			return


def perform_heartbeat_check(ip, topic):
	global pub_ips
	if heartbeat_sock_dict.get(ip) != None:
		print(f"performing heartbeat check on {ip}")
		try:
			heartbeat_sock_dict.get(ip).send_string("Heartbeat check")
			heartbeat_sock_dict.get(ip).recv()
			print(f"heartbeat check passed {ip} for {topic}")
		# This exception should be hit if the socket is closed on the other end
		except:
			print(f"heartbeat check failed, removing {ip} for {topic}")
			del heartbeat_sock_dict[ip]

			# Go through all topics and remove the pub that failed the heartbeat check
			for topic in pub_topic_dict:
				for value in pub_topic_dict.get(topic):
					_, _, pub_ip = value.split('_')
					if pub_ip == ip:
						pub_topic_dict.get(topic).remove(value)
						update_zk("delete",f"/pub_topic_dict/{topic}",f"{value}")
			pub_ips.remove(ip)


def heartbeat_response():
	while True:
		try:
			resp = pub_heartbeat_socket.recv()
			#print("responding to heartbeat check")
			pub_heartbeat_socket.send_string("OK")
		except:
			print("heartbeat ending silently")
			break


# Returns the local ip address of the node this func is being called on
def get_local_ip():
    for interface in netifaces.interfaces():
        # Not all interfaces have an IPv4 address:
        if netifaces.AF_INET in netifaces.ifaddresses(interface):
            # Some interfaces have multiple IPv4 addresses:
            for address_info in netifaces.ifaddresses(interface)[netifaces.AF_INET]:
                address_object = ipaddress.IPv4Address(address_info['addr'])
                if not address_object.is_loopback:
                    print(address_info)
                    return address_info['addr']


## Functions for zookeeper client ##

def add_broker(zk_ip, zk_port):
	global driver

	driver = zk.ZK_Driver(zk_ip,zk_port)
	driver.init_driver()

	ip = get_local_ip()
	ip = f'{ip}'.encode('utf-8')

	print(ip)

	driver.start_session()
	leader = False
	while not leader:
		try:
			driver.add_node('/broker',ip,False)
			leader = True
			print("Elected as leader, continuing")
		except:
			watch_lock = Lock()
			watch_lock.acquire()

			def watch_func(event):
				print("broker has come online")
				watch_lock.release()
			print("There is already a leader, waiting until leader leaves")

			print("Watching for broker znode to change")
			driver.watch_node('/broker', watch_func)

			watch_lock.acquire()
			watch_lock.release()

	recover_broker()

def recover_broker():
	global sub_port
	global pub_ips

	# Get each of the maps needed by zk

	# sub_topic_dict
	'''
	topics = driver.get_children("/sub_topic_dict")
	print(f"recovering sub topics: {topics}")
	if topics != None:
		for topic in topics:
			print(topic)
			ips = driver.get_node_if_exists(f"/sub_topic_dict/{topic}")
			sub_topic_dict[topic] = set()

			if isinstance(ips, bytes):
				ips = ips.decode("ascii")
			print(ips)

			for ip in ips.split(' '):
				sub_topic_dict.get(topic).add(ip)
	'''


	# pub_topic_dict
	topics = driver.get_children("/pub_topic_dict")
	print(f"recovering pub topics: {topics}")
	if topics != None:
		for topic in topics:
			print(topic)
			ips = driver.get_node_if_exists(f"/pub_topic_dict/{topic}")
			ips = ips.strip()
			pub_topic_dict[topic] = list()

			if isinstance(ips, bytes):
				ips = ips.decode("ascii")

			print(f"ips: '{ips}'")
			if len(ips) > 0:
				for val in ips.split(' '):
					pub_topic_dict.get(topic).append(val)
					_, _, ip = val.split('_')
					if ip not in pub_ips:
						pub_ips.append(ip)


	# generate heartbeat_sock_dict
	for pub_ip in pub_ips:
		tmp_socket = context.socket(zmq.REQ)
		tmp_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
		tmp_socket.setsockopt(zmq.LINGER, 0)
		print(f"IP is: {pub_ip}")
		tmp_socket.connect("tcp://%s:5550" % pub_ip)
		heartbeat_sock_dict[pub_ip] = tmp_socket
		# Start background heartbeat check
		start_background_perform_heartbeat_check(pub_ip)


	# generate sub socket and ports
	ports_in_use = []
	topics = driver.get_children("/sub_dict")
	print(f"recovering sub port topics: {topics}")
	if topics != None:
		for topic in topics:
			print(topic)
			history_counts = driver.get_children(f"/sub_dict/{topic}")
			for history in history_counts:
				port = driver.get_node_if_exists(f"/sub_dict/{topic}/{history}")
				if port != None:
					if isinstance(port, bytes):
						port = port.decode("ascii")

					ports_in_use.append(int(port))
					sub_port_dict[topic] = dict()
					sub_port_dict.get(topic)[history] = int(port)

					sock = context.socket(zmq.PUB)
					sock.bind("tcp://*:%d" % int(port))
					sub_dict[topic] = dict()
					sub_dict.get(topic)[history] = sock
		if len(ports_in_use) > 0:
			sub_port = max(ports_in_use) + 1


def update_zk(action,name,value,unique=True):
	exists = driver.check_for_node(name)
	if exists:
		current_value = driver.get_node(name)

		if isinstance(current_value, bytes):
			current_value = current_value.decode("ascii")

		if action == "add":
			if unique and value in current_value:
				return
			new_value = f"{current_value}{value} "
			new_value = f'{new_value}'.encode('utf-8')

			print(f"update value {current_value} to {new_value} to {name}")

			driver.update_value(name,new_value)

		else:
			ips = current_value.split()
			print(f"ips: {ips}")
			#ips.remove(value)
			new_value = ""
			for ip in ips:
				if ip != value:
					new_value += f"{ip} "
			#new_value = new_value.jin(ips)
			new_value = new_value.rstrip(' ')
			print(f"delete replacing '{current_value} with '{new_value}'")
			#new_value = current_value.replace(f"{value} ","")
			new_value = f'{new_value}'.encode('utf-8')

			#print(f"delete replacing '{current_value}' with '{new_value}'")
			#ips.remove("tmp")

			driver.update_value(name,new_value)
	else:
		if action == "add":
			value = f'{value} '.encode('utf-8')
			print(f"adding value {value} to {name}")
			driver.add_node(name,value,True)


def register_zk_driver(zk_ip, zk_port):
	global driver

	driver = zk.ZK_Driver(zk_ip,zk_port)
	driver.init_driver()
	driver.start_session()

def disconnect():
	global monitor_broker
	global terminate_threads
	monitor_broker = False
	terminate_threads = True
	print("stopping driver")
	driver.stop_session()
	print("closing context")
	close_context()

# Used by pub and sub to get the broker's ip address
def discover_broker():
	global broker_ip
	exists = driver.check_for_node('/broker')
	if not exists:
		watch_lock = Lock()
		watch_lock.acquire()

		def watch_func(event):
			print("broker has come online")
			watch_lock.release()

		print("Watching for broker to come online")
		driver.watch_node('/broker', watch_func)

		watch_lock.acquire()
		watch_lock.release()

		print("Finding broker address")
		value = driver.get_node('/broker')

		if isinstance(value, bytes):
			value = value.decode("ascii")

		broker_ip = value
		print(f"Found broker {value}, monitoring for new broker asynchronously")
		async_broker_monitor()

		print(value)
		return value
	else:
		value = driver.get_node('/broker')

		if isinstance(value, bytes):
			value = value.decode("ascii")

		broker_ip = value
		print(f"Found broker {value}, monitoring for new broker asynchronously")
		async_broker_monitor()

		print(value)
		return value

# Used by pub and sub to monitor broker ip address change
def async_broker_monitor():
	thread = Thread(target=monitor_broker_change, args=())
	thread.start()

def monitor_broker_change():
	global broker_ip
	global monitor_broker
	global driver
	while monitor_broker:
		try:
			watch_lock = Lock()
			watch_lock.acquire()

			def watch_func(event):
				print("new broker has come online")
				time.sleep(1)
				watch_lock.release()

			print("Watching for new broker to come online")
			driver.watch_node('/broker', watch_func)

			watch_lock.acquire()
			watch_lock.release()

			print("Finding new broker address")
			value = driver.get_node_if_exists('/broker')
			if value != None:
				if isinstance(value, bytes):
					value = value.decode("ascii")

				print(f"Got a new broker address: {value}")
				broker_ip = value

		except:
			print("Interrupted while watching for new broker")

		finally:
			time.sleep(0.5)


def monitor_broker_change_original():
	global broker_ip
	global monitor_broker
	while monitor_broker:
		try:
			broker_state = "Online"

			def watch_func_1(event):
				print("broker has gone offline")
				broker_state = "Offline"
				driver.watch_node('/broker', watch_func_2)

			def watch_func_2(event):
				print("broker has come back online")
				broker_state = "Recovering"
				broker_changed = True

			# Watching for broker to go offline
			driver.watch_node('/broker', watch_func_1)

			while broker_state != "Online":
				if not monitor_broker:
					break
				else:
					time.sleep(1)
					value = driver.get_node('/broker')
					if value != None:
						broker_state = "Online"
					else:
						broker_state = "Offline"

			if not monitor_broker:
				break

			print("Finding new broker address")
			value = driver.get_node('/broker')
			if value != None:
				if isinstance(value, bytes):
					value = value.decode("ascii")

				broker_ip = value

		except:
			print("Interrupted while watching for new broker")

		finally:
			time.sleep(0.5)

#
# TODO:
#
# [DONE] 1. Give the publisher a history - keep track using a list, index, and counter
#	> This should loop around to the from again. This way, when a request comes
#	  in for history items, just copy the counter, and iterate it, using modulo
#	  to get the index inside the list.
#
# [DONE] 2. Add the ability for pubs and broker to respond to history requests
#	> Also will need to implement history requests in subscriber
#	> Send a history request with a new request, so that you'll get the next value + the last X-1 history
#	- This ended up needing to be a JSON object that contains the full history with each publication
#
# [DONE] 3. Give the pubs an "Ownership" value. Store this in ZK, or even use ZK to assign it
#	> Can be similar to the broker leader election algorithm
#	- Not implemented like the leader election - the broker is the one that controls the propagation
#	based on ownership strength.
#
# [DONE] 4. Implement pub selection logic based on ownership + history
#	> If these get stored in ZK, it could be pretty easy to grab it all, then just
#	  iterate across, or evne to just grab one at a time if they have to be independent znodes
#
# [DONE] 5. Update the heartbeat request to add/remove pubs on a regular interval
#	instead of on a new sub coming online.
#	> Could even flip it so that pubs are pushing their heartbeat, and the sub checks it.
#	> Then, if a pub doesn't send a heartbeat for (2) seconds, then it will be remove from
#	  the dicts and from ZK.
#
# 6. Implement broker logic to store pubs in a round-about fashion for max_brokers * 2 (so 6) znodes.
#	> This will allow us to split and combine brokers without redistributing pub ips from the znodes.
#	> Basically each broker will have an id, and it'll do id % broker_count (also from ZK) to get the
#	  branches that it should be responsible for. 
#		>> So everything might be the same as you have it now, only with a key between root and pub_topic_dict,
#		   like /1/pub_topic dict, /2/pub_topic_dict, etc.
#
# 7. Fix Pub/Sub broker discovery logic, to be able to recognize when to shift brokers.
#	> Current logic has a bug where Subs don't terminate when they're done because of how it works
#	> Need to be able to register, and then receive a notification message.
#	> Can round-robin for node in zk for initial connection, then brokers can load balance correctly
#	> Notification coming back from broker can be a different broker ip to connect to instead
#
# 8. Make brokers tell the pub and sub when to connect to a new broker
#	> Either on recovery, or when load balancing - the same algorithm can apply
#
#
# Other TODOs:  - Sub needs to terminate correctly after receiving all messages
#				- Sub ports are not adding/removing corectly in zk
#
#