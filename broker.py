import zmq
import _thread
from zmq_api import register_broker
#from zmq_api import register_listener_for_pubs
from zmq_api import listen_for_pub_registration
#from zmq_api import register_listener_for_pubs
from zmq_api import listen_for_sub_registration
from zmq_api import publish_to_sub
from zmq_api import listen_for_pub_data

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)




def register_subs():
	while True:
		# Listen for new subs to come onto the system
		listen_for_sub_registration()


def register_pubs():
	# Listen for new pubs entering the system
	string = listen_for_pub_registration()

	_, ip, _, topic = string.split()

	# Start new thread listening for data from this pub
	_thread.start_new_thread(receive_pub_data, (ip))


def receive_pub_data(ip):
	# Get the pub message
	string = listen_for_pub_data(ip)

	# Forward published data to the appropriate subs
	for val in string.split():
		publish_to_sub(val)


# Register broker
register_broker()

# Start new listener for subs
_thread.start_new_thread(register_subs, ())


while True:
	register_pubs()






