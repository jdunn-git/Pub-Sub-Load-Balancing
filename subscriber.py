import sys
import zmq
from zmq_api import register_sub
from zmq_api import listen

#  Socket to talk to server
#context = zmq.Context()
#socket = context.socket(zmq.SUB)

# Here we assume publisher runs locally unless we
# send a command line arg like 10.0.0.1
srv_addr = sys.argv[1] if len(sys.argv) > 1 else "localhost"
connect_str = "tcp://" + srv_addr + ":5556"

print("Collecting updates from weather server...")
#socket.connect(connect_str)

# Subscribe to zipcode, default is NYC, 10001
zip_filter = sys.argv[2] if len(sys.argv) > 2 else "10001"

# Python 2 - ascii bytes to unicode str
if isinstance(zip_filter, bytes):
    zip_filter = zip_filter.decode('ascii')

print("Subscribing to %s" % zip_filter)

register_sub(srv_addr, zip_filter)

# Process 10 updates
total_temp = 0
for update_nbr in range(10):
    string = listen(zip_filter)
    zipcode, temperature, relhumidity = string.split()
    total_temp += int(temperature)
    print("Average temperature for zipcode '%s' was %dF" % (
      zip_filter, total_temp / (update_nbr+1))
)
