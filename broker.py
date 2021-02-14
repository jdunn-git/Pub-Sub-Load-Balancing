import thread
from zmq_api import register_broker
#from zmq_api import register_listener_for_pubs
from zmq_api import listen_for_pubs
#from zmq_api import register_listener_for_pubs
from zmq_api import listen_for_subs
from zmq_api import publish_to_sub


# Register broker
register_broker()

# Start new listener for subs
thread.start_new_thread(register_subs, ())

while True:
	# Start listening for published data
	string = listen_for_pubs()

	# Forward published data to the appropriate subs
    for val in string.split():
	    publish_to_sub(val)


def register_subs:
	while True:
		listen_for_pubs()



