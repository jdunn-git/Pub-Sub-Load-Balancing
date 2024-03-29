import zmq
import _thread

from zmq_api import (
	listen_for_pub_data,
	listen_for_pub_registration,
	listen_for_sub_registration,
	publish_to_sub,
	register_broker,

)

print(f"Current libzmq version is {zmq.zmq_version()}")
print(f"Current  pyzmq version is {zmq.__version__}")




def register_subs():
	while True:
		# Listen for new subs to come onto the system
		listen_for_sub_registration()


def register_pubs():
	# Listen for new pubs entering the system
	#string = listen_for_pub_registration()

	#_, ip, _, topic = string.split()

	# Start new thread listening for data from this pub
	_thread.start_new_thread(receive_pub_data, ())


def receive_pub_data():
	# Get the pub message
	string = listen_for_pub_data()

	# Forward published data to the appropriate subs
	publish_to_sub(string)


# Register broker
register_broker()

# Start new listener for subs
_thread.start_new_thread(register_subs, ())


while True:
	receive_pub_data()
