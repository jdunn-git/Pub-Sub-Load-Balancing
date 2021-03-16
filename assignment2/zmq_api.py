import sys
import zmq
import _thread
import netifaces
import ipaddress
import time
from threading import Lock, Thread

import zmq_api_zkclient as zk

#  Socket to talk to server
context = zmq.Context()

pub_dict = dict()
pub_topic_dict = dict()
sub_dict = dict()
sub_topic_dict = dict()
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
new_pub_sock = context.socket(zmq.PUB)

# Used to listen for addition pubs registering
sub_new_pub_socket = context.socket(zmq.SUB)

# Used to heartbeat check the publishers from the broker
pub_heartbeat_socket = context.socket(zmq.REP)
heartbeat_sock_dict = dict()

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
broker_ip = ""
monitor_broker = True

sub_new_pub_listener_thread = Thread()
sub_thread_end = False

# Zookeeper
driver = zk.ZK_Driver('127.0.0.1',2181)


def close_context():
	context.destroy()

## Functions for publisher communication ##

# Registers publisher
def register_pub(ip, topic):
	print("Registering to broker at tcp://%s:5554 with topic: %s" % (ip, topic))

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string("Registering flood %s %s" % (topic, local_ip))
	resp = tmp_socket.recv()

	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	if resp == "OK":
		pub_socket.bind("tcp://*:5556")
		pub_dict[topic] = pub_socket
		print("Registered pub on tcp://*:5556")
		pub_heartbeat_socket.bind("tcp://*:5550")
		_thread.start_new_thread(heartbeat_response, ())

# Publishes data for the publisher based on the registered topic
def publish(topic, value, timestamp):
	if pub_dict.get(topic) != None:
		pub_dict.get(topic).send_string(value)
		print(f"Sending data to subscriber at {timestamp}")



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

	broker_ip = broker
	print(f"listening for new pubs on {topic} from {broker_ip}")
	sub_new_pub_socket.connect(f"tcp://{broker_ip}:5551")
	sub_new_pub_socket.setsockopt_string(zmq.SUBSCRIBE, topic)
	#sub_new_pub_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
	sub_new_pub_listener_thread = Thread(target=listen_for_new_pubs, args=(topic, topic_filter, process_response, max_listens))
	sub_new_pub_listener_thread.start()

# Receives data for the subscriber based on the registered topic
def listen(topic_filter, index):
	print("In listen")
	success = False
	while not success:
		try:
			sock = sub_dict.get(topic_filter)[index]
			if sock != None:
				print("Have socket for topic_filter %s, waiting for message" % (topic_filter))
				string = sock.recv_string()
				success = True
				return string
		except:
			# The broker may have gone down, wait a second and reconnect
			sock = sub_dict.get(topic_filter)[index]
			sock.close()
			sock = context.socket(zmq.SUB)
			sock.setsockopt(zmq.RCVTIMEO, 500) # milliseconds
			sock.setsockopt(zmq.LINGER, 0)
			sock.setsockopt_string(zmq.SUBSCRIBE, "")
			sock.connect(f"tcp://{broker_ip}:{broker_port}")
			sub_dict.get(topic_filter)[index] = sock
			#sub_dict[topic_filter] = [sub_socket]

			print(f"reconnected to {broker_ip} in case broker went down")
			time.sleep(0.5)


def synchronized_listen_helper(topic_filter, sock, process_response, max_listens):
	print("In listen helper")
	global sync_listen_count
	if sock != None:
		while sync_listen_count < max_listens:
			print("Have socket for topic_filter %s, waiting for message" % (topic_filter))
			string = sock.recv_string()
			listen_count_lock.acquire()
			# Double checking this here in case any race conditions I haven't planned for exist
			if sync_listen_count >= max_listens:
				listen_count_lock.release()
				break;
			sync_listen_count += 1
			process_response(string,sync_listen_count)
			listen_count_lock.release()

def synchronized_listen(topic_filter, process_response, max_listens):
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
			t = Thread(target=synchronized_listen_helper, args=(topic_filter, sock, process_response, max_listens))
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
			print(f"filtering on {pub_topic}")
			sub_new_pub_socket.setsockopt_string(zmq.SUBSCRIBE, pub_topic)
			time.sleep(0.5)
			continue

		print(f"Adding a new sub for {ip}")
		tmp_socket = context.socket(zmq.SUB)
		tmp_socket.connect("tcp://%s:5556" % ip)
		tmp_socket.setsockopt_string(zmq.SUBSCRIBE, topic_filter)
		if sub_dict.get(topic_filter) == None:
			sub_dict[topic_filter] = [tmp_socket]
		else:
			sub_dict[topic_filter].append(tmp_socket)
		print("Listening to publisher at %s for %s" % (ip, topic_filter))

		listening_lock.acquire()
		if listening_state != "done":
			print("Active listening is happening, adding thread")
			t = Thread(target=synchronized_listen_helper, args=(topic_filter, tmp_socket, process_response, max_listens))
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


def register_pub_with_broker(ip, topic):
	print("Registering to broker at tcp://%s:5554 with topic: %s" % (ip, topic))

	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5554" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string("Registering broker %s %s" % (topic, local_ip))
	resp = tmp_socket.recv()

	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	if resp == "OK":
	#	print( "Registered subscriber with broker")
		pub_broker_socket.connect("tcp://%s:5553" % ip)
		pub_broker_socket.setsockopt( zmq.RCVTIMEO, 500 ) # milliseconds
		pub_broker_socket.setsockopt(zmq.LINGER, 0)
		pub_dict[topic] = pub_broker_socket
		pub_heartbeat_socket.bind("tcp://*:5550")
		_thread.start_new_thread(heartbeat_response, ())
	else:
		print(f"invalid response {resp}")

def register_sub_with_broker(ip, topic_filter):
	print(f"Registering suib with broker {ip}")
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5555" % ip)
	tmp_socket.send_string("Registering topic_filter %s" % (topic_filter))
	resp = tmp_socket.recv()
	#if resp == "OK":
	#	print( "Registered subscriber with broker")
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	broker_port = resp
	print(f"Registering sub to {ip}:{broker_port}")
	sub_socket.connect("tcp://%s:%d" % (ip, int(broker_port)))
	sub_socket.setsockopt( zmq.RCVTIMEO, 500 ) # milliseconds
	sub_socket.setsockopt(zmq.LINGER, 0)
	sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
	sub_dict[topic_filter] = [sub_socket]

def discover_publishers(ip, topic):
	print(f"Trying to discover (a) publisher(s) with topic {topic} through broker at tcp://{ip}:5554")
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.connect("tcp://%s:5552" % ip)
	local_ip = get_local_ip()
	tmp_socket.send_string("Registering topic %s %s" % (topic, local_ip))
	resp = tmp_socket.recv()
	if isinstance(resp, bytes):
		resp = resp.decode("ascii")

	if resp == "404":
		print("No publishers for now")
		return []

	# resp should be a string containing all pub IPs publishing this topic
	pub_ips = resp.split()
	return pub_ips

def listen_for_pub_discovery_req():
	req = pub_discovery_socket.recv()
	if isinstance(req, bytes):
		req = req.decode("ascii")
	print("Got a publisher discovery request: %s" % req)
	_,  _, topic, ip = req.split(' ')

	if pub_topic_dict.get(topic) != None:
		pub_ips = pub_topic_dict.get(topic).copy()
		for ip in pub_ips:
			# This will double check that all pubs are still active before sending to the sub
			perform_heartbeat_check(ip, topic)

		# Note: There is a small race condition where a pub goes down between the heartbeat check and here, but
		# it is miniscule and likely not worth the time to solve
		delim = ' '
		print(pub_topic_dict.get(topic))
		pubs = delim.join(pub_topic_dict.get(topic))
		print(pubs)
		pub_discovery_socket.send_string(pubs)
	else:
		pub_discovery_socket.send_string("404")

	# Keep a dict of sub ips that have request this topic for future updates
	if sub_topic_dict.get(topic) == None:
		sub_topic_dict[topic] = {ip}
	else:
		sub_topic_dict.get(topic).add(ip)

	update_zk("add",f"/sub_topic_dict/{topic}",f"{ip}")

def listen_for_pub_registration():
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
	mode = resp[1]
	topics = resp[2:len(resp)-1]
	for topic in topics:
		if pub_topic_dict.get(topic) == None:
			pub_topic_dict[topic] = {ip}
		else:
			pub_topic_dict[topic].add(ip)

		update_zk("add",f"/pub_topic_dict/{topic}",f"{ip}")

		# Notify sub listener when in flood mode
		if mode == "flood":
			string = f"{topic} {ip}"
			print(f"publishing new pub info {string}")
			new_pub_sock.send_string(string)

	# Start heartbeat socket for this pub
	tmp_socket = context.socket(zmq.REQ)
	tmp_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
	tmp_socket.setsockopt(zmq.LINGER, 0)
	print(f"IP is: {ip}")
	tmp_socket.connect("tcp://%s:5550" % ip)
	heartbeat_sock_dict[ip] = tmp_socket

	pub_registration_socket.send_string("OK")

	ret = "ip %s topics %s" % (ip, topics)

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
		sub_dict[topic_filter] = [sock]
		update_zk("add",f"/sub_dict/{topic_filter}",f"{sub_port}")

	print(f"Telling sub to register to port {sub_port-1}")
	resp = sub_port_dict.get(topic_filter)
	sub_registration_socket.send_string(str(resp))

def publish_to_broker(topic, data, message_number, timestamp):
	if pub_dict.get(topic) != None:
		success = False
		print(f"Sending Data number {message_number} at {timestamp}")
		while not success:
			try:
				pub_dict.get(topic).send_string(data)
				resp = pub_dict.get(topic).recv()
				if isinstance(resp, bytes):
					resp = resp.decode("ascii")
				if resp == "OK":
					success = True
					print(f"Published data to the broker at {timestamp}")
			except:
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

	return string

def publish_to_sub(data):
	for topic_filter, socks in sub_dict.items():
		if topic_filter in data:
			for sock in socks:
				sock.send_string(data)
				print("Sending Data to sub")

def perform_heartbeat_check(ip, topic):
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

			pub_topic_dict.get(topic).remove(ip)
			update_zk("delete",f"/pub_topic_dict/{topic}",f"{ip}")


def heartbeat_response():
	resp = pub_heartbeat_socket.recv()
	pub_heartbeat_socket.send_string("OK")


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

	# Get each of the maps needed by zk

	# sub_topic_dict
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


	pub_ips = set()

	# pub_topic_dict
	topics = driver.get_children("/pub_topic_dict")
	print(f"recovering pub topics: {topics}")
	if topics != None:
		for topic in topics:
			print(topic)
			ips = driver.get_node_if_exists(f"/pub_topic_dict/{topic}")
			pub_topic_dict[topic] = set()

			if isinstance(ips, bytes):
				ips = ips.decode("ascii")

			print(ips)

			for ip in ips.split(' '):
				pub_topic_dict.get(topic).add(ip)
				pub_ips.add(ip)


	# generate heartbeat_sock_dict
	for pub_ip in pub_ips:
		tmp_socket = context.socket(zmq.REQ)
		tmp_socket.setsockopt(zmq.RCVTIMEO, 500 ) # milliseconds
		tmp_socket.setsockopt(zmq.LINGER, 0)
		print(f"IP is: {pub_ip}")
		tmp_socket.connect("tcp://%s:5550" % pub_ip)
		heartbeat_sock_dict[pub_ip] = tmp_socket

	# generate sub socket and ports
	ports_in_use = []
	topics = driver.get_children("/sub_dict")
	print(f"recovering sub port topics: {topics}")
	if topics != None:
		for topic in topics:
			print(topic)
			port = driver.get_node_if_exists(f"/sub_dict/{topic}")
			if port != None:
				if isinstance(port, bytes):
					port = port.decode("ascii")

				ports_in_use.append(int(port))
				sub_port_dict[topic] = int(port)

			sock = context.socket(zmq.PUB)
			sock.bind("tcp://*:%d" % int(sub_port))
			sub_dict[topic] = [sock]
		if len(ports_in_use) > 0:
			sub_port = max(ports_in_use)


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
			new_value = current_value.replace(f"{value} ","")
			new_value = f'{new_value}'.encode('utf-8')

			print(f"delete replacing {current_value} with {new_value}")

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
	monitor_broker = False
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
# [DONE] 1. Make broker able to always run, both in flood mode (where it just handles discovery), and in normal broker mode
# UPDATE: Everything is configured to enable this, but need to change the automation script
#
#
# [DONE] 2. Let broker have a normal and a "discovery" mode
#	> This will require req-rep sockets for pub and sub registration
#	> Also need to be aware of topics for sub registration now
#	> This may also require moving to an XSUB socket for subs, but I'm not sure
# UPDATE: broker handles both paths fine, and doesn't need to be configured for any "mode", either.
#		> As long as the pubs and subs are configured correctly, it'll work.
#
# [DONE] 3. Make "heartbeat" requests in "discovery" mode to verify if pubs are still active,
#	and remove them if they don't respond
# 	> Better yet, let the sub connecting to the pub be the "heartbeat", so that if
#		that request fails, it comes back to the broker and tells it about that
# UPDATE: Heartbeat is working, and pubs can drop off the system now without it breaking the new publisher discovery paths
#
# [DONE] 4. Connect broker to zookeeper, and add leader selection
# UPDATE: Broker connects to zookeeper, leader election is being performed, and new leader recovers to state of previous leader
#
# [DONE] 5. Connect pub and sub to zookeeper to find broker leader
# UPDATE: Pubs and Subs connects to zookeeper, but params need to be adjusted to intake the
#		zookeeper node ip instead of the broker ip (or, we could do both and have different params)
#			> This is really close to being done, just needs these params added to the pub and sub.
#			> Right now, it's hardcoded to use now 10.0.0.7 as the zookeeper node
#
# [    ] 6. Update automated scripts for broker to be always on and for zookeeper to be started
#
# [    ] 7. Data generation and graphing
#
