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
    parser.add_argument("-t", "--topics", type=str, default="zipcode temperature relhumidity", help="Topic needed")
    parser.add_argument("-s", "--subscribers", type=int, default=10, help="Number of subscribers, default 10, minimum is number of topics")
    parser.add_argument("-p", "--publishers", type=int, default=2, help="Number of publishers, default 2, minimum is number of topics")
    parser.add_argument("-r", "--racks", type=int, default=1, help="Number of racks, choices 1, 2 or 3")
    parser.add_argument("-e", "--executions", type=int, default=20, help="Number of executions for the program")

    parser.add_argument("-b", "--broker_mode", default=False, action="store_true")

    # parse the args
    args = parser.parse_args()

    args.subs = max(args.subscribers, len(args.topics))
    args.pubs = max(args.publishers, len(args.topics))
    return args


def execute(output_dir, hosts, publishers, subscribers, broker_mode = False, executions=20):

    commands = []


    if broker_mode:
        host_index = 0
        ip_holder = 1
        zip_holder = 1
        # Allocate first host as broker
        commands.append(f"python3 ./broker.py")
        # Allocate commands for publishers and subscribers
        for i in range(publishers):
            commands.append(f"python3 ./publisher.py -s 10.0.0.1 -z 1010{zip_holder} -b -e {executions} &> {output_dir}{hosts[host_index].name}.out")
            zip_holder += 1
            host_index += 1

        zip_holder = 1
        for i in range(subscribers):
            commands.append(f"python3 ./subscriber.py -s 10.0.0.1 -z 1010{zip_holder} -b &> {output_dir}{hosts[host_index].name}.csv")
            zip_holder += 1
            host_index += 1

    else:
        host_index = 0
        ip_holder = 1
        zip_holder = 1
        for i in range(publishers):
            commands.append(f"python3 ./publisher.py -z 1010{zip_holder} -e {executions} &> {output_dir}{hosts[host_index].name}.out")
            zip_holder += 1
            host_index += 1

        zip_holder = 1
        for i in range(subscribers):
            commands.append(f"python3 ./subscriber.py -s 10.0.0.{ip_holder} -z 1010{zip_holder} &> {output_dir}{hosts[host_index].name}.csv")
            ip_holder += 1
            zip_holder += 1
            host_index += 1

    # Run threads on hosts
    host_threads = []
    for i in range(len(hosts)-1):
        print(f"Call command {commands[i]} on {hosts[i]}")
        thread = threading.Thread(target=hosts[i].cmdPrint, args=(commands[i],))
        thread.start()
        host_threads.append(thread)

    for i in range(len(hosts)-1):
        if i > 0 or not broker_mode:
            print(f"Wait for {hosts[i].name} to be done")
            host_threads[i].join()

    if broker_mode:
        print("Terminating Broker")
        hosts[0].terminate()



def main():
    parsed_args = parse_args()

    # Create Topology
    print("Creating Topology")
    topology = MR_Topo(racks = parsed_args.racks,
                       publishers = parsed_args.publishers,
                       subscribers = parsed_args.subscribers,
                       broker_mode = parsed_args.broker_mode )

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
        execute(output_dir = output_dir,
                hosts=network.hosts,
                publishers = parsed_args.publishers,
                subscribers = parsed_args.subscribers,
                broker_mode = parsed_args.broker_mode,
                executions = parsed_args.executions)
    else:
        print(f"{output_dir} does not exist")
        os.mkdir(output_dir)




    print("Deactivating Network")
    network.stop()


if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
