import os
import sys
import argparse
import threading

from signal import SIGINT
import time

import subprocess


from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor


from mr_topology import MR_Topo

from mininet.node import OVSController


def parse_args():
    # parse the command line
    parser = argparse.ArgumentParser()

    # add optional arguments
    parser.add_argument("-t", "--topic", type=str, default="zipcode temperature relhumidity", help="Topic needed")
    parser.add_argument("-s", "--subscribers", type=int, default=10, help="Number of subscribers, default 10, minimum is number of topics")
    parser.add_argument("-p", "--publishers", type=int, default=2, help="Number of publishers, default 2, minimum is number of topics")
    parser.add_argument("-r", "--racks", type=int, default=1, help="Number of racks, choices 1, 2 or 3")
    parser.add_argument("-c", "--clean_state", default=False, action="store_true")

    parser.add_argument("-b", "--broker_mode", default=False, action="store_true")

    # parse the args
    args = parser.parse_args ()

    args.subs = max(args.subscribers, len(args.topics))
    args.pubs = max(args.publishers, len(args.topics))
    return args


def execute(hosts, publishers, subscribers, broker_mode = False):

    commands = []
    host_index = 0
    ip_holder = 1
    zip_holder = 1

    if broker_mode:
        # Allocate first host as broker
        commands.append(f"python3 broker.py")
        # Allocate commands for publishers and subscribers
        for i in range(len(publishers)):
            commands.append(f"python3 publisher.py -s 10.0.0.1 - z 1010{zip_holder} -b True &> {output_dir}/{hosts[host_index].name}.out")
            zip_holder += 1

        zip_holder = 1
        for i in range(len(subscribers)):
            commands.append(f"python3 subscriber.py -s 10.0.0.1 - z 1010{zip_holder} -b True &> {output_dir}/{hosts[host_index].name}.csv")
            zip_holder += 1

    else:
        for i in range(len(publishers)):
            commands.append(f"python3 publisher.py -s 10.0.0.1 - z 1010{zip_holder} -b True &> {output_dir}/{hosts[host_index].name}.out")
            zip_holder += 1

        zip_holder = 1
        for i in range(len(subscribers)):
            commands.append(f"python3 subscriber.py -s 10.0.0.1 - z 1010{zip_holder} -b True &> {output_dir}/{hosts[host_index].name}.csv")
            zip_holder += 1

    # Run threads on hosts
    host_threads = []
    for i in range(len(hosts)):
        print(f"Call command {coomand[i]} on {hosts[i]}")
        thread = threading.Thread(target=hosts[i].cmdPrint, args=(coomand[i],))
        thread.start()
        host_threads.append(thread)

    for i in range(len(hosts)):
        if i > 0 or not broker_mode:
            print(f"Wait for {hosts[i].name} to be done")
            host_threads[i].join()

    if broker_mode:
        print("Terminating Broker")
        hosts[0].terminate()



def main():
    parse_args = parse_args()

    # Create Topology
    print("Creating Topology")
    topology = MR_Topo(racks = parse_args.racks,
                       publishers = parse_args.publishers,
                       subscribers = parse_args.subscribers,
                       broker_mode = parse_args.broker_mode )

    # Create Network
    print("Creating network")
    network = Mininet(topology, link = TCLink, controller = OVSController)

    # activate the network
    print("Activating network")
    network.start()

    # debugging purposes
    print("Dumping host connections")
    dumpNodeConnections(network.hosts)

    output_dir = '/tmp/assignment_output/'

    if os.path.isdir(output_dir):
        if parse_args.broker_mode:
            execute(output_dir = output_dir,
                    hosts=network.hosts,
                    publishers = parse_args.publishers,
                    subscribers = parse_args.subscribers,
                    broker_mode = True)

        else:
            execute(output_dir = output_dir,
                    hosts=network.hosts,
                    publishers = parse_args.publishers,
                    subscribers = parse_args.subscribers)
    else:
        print(f"{output_dir} does not exist")

    print("Deactivating Network")
    network.stop()


if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
