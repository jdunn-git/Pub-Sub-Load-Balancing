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

    parser.add_argument("-w", "--record_time", default=False, action="store_true")
    parser.add_argument("-d", "--record_dir", type=str, default="timing_data", help="Directory to store timing data")
    
    # parse the args
    args = parser.parse_args()

    args.subs = max(args.subscribers, len(args.topics))
    args.pubs = max(args.publishers, len(args.topics))
    return args


def execute(output_dir, hosts, publishers, subscribers, broker_mode = False, executions=20, record_time = False, record_dir = "timing_data"):

    pub_commands = []
    pub_hosts = []
    sub_commands = []
    sub_hosts = []
    broker_commands = []

    commands = []

    zipcode = 10101

    if broker_mode:
        host_index = 1
        ip_holder = 1
        zip_holder = 10101
        # Allocate first host as broker
        #commands.append(f"python3 ./broker.py")
        broker_commands.append(f"python3 ./broker.py")
        # Allocate commands for publishers and subscribers
        for i in range(publishers):
            #commands.append(f"python3 ./publisher.py -s 10.0.0.1 -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_commands.append(f"python3 ./publisher.py -s 10.0.0.1 -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_hosts.append(hosts[host_index])
            zip_holder += 1
            host_index += 1

        zip_holder = 10101
        for i in range(subscribers):
            #commands.append(f"python3 ./subscriber.py -s 10.0.0.1 -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.csv")
            sub_commands.append(f"python3 ./subscriber.py -s 10.0.0.1 -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.csv")
            sub_hosts.append(hosts[host_index])
            zip_holder += 1
            host_index += 1

    else:
        host_index = 0
        ip_holder = 1
        zip_holder = 10101
        for i in range(publishers):
            #commands.append(f"python3 ./publisher.py -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_commands.append(f"python3 ./publisher.py -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_hosts.append(hosts[host_index])
            zip_holder += 1
            host_index += 1

        zip_holder = 10101
        for i in range(subscribers):
            #commands.append(f"python3 ./subscriber.py -s 10.0.0.{ip_holder} -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.csv")
            sub_commands.append(f"python3 ./subscriber.py -s 10.0.0.{ip_holder} -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.csv")
            sub_hosts.append(hosts[host_index])
            ip_holder += 1
            zip_holder += 1
            host_index += 1

    # Run threads on hosts
    host_threads = []
    host_names = []
    hosts_in_use = 0
    pubs_running = 0
    subs_running = 0
    hosts_to_run = len(sub_commands)+len(pub_commands)
    if broker_mode:
        hosts_to_run = hosts_to_run + 1

    while hosts_in_use < hosts_to_run:
        print(f"Hosts to run: {hosts_to_run}, hosts in use: {hosts_in_use}")
        if broker_mode and hosts_in_use == 0    :
            print(f"Call command {broker_commands[0]} on {hosts[hosts_in_use]}")
            thread = threading.Thread(target=hosts[0].cmdPrint, args=(broker_commands[0],))
            thread.start()
            host_threads.append(thread)
            host_names.append(hosts[hosts_in_use].name)
            hosts_in_use = hosts_in_use + 1

        if subs_running < len(sub_commands):
            print(f"Call command {sub_commands[subs_running]} on {sub_hosts[subs_running]}")
            #thread = threading.Thread(target=hosts[i].cmdPrint, args=(commands[i],))
            thread = threading.Thread(target=sub_hosts[subs_running].cmdPrint, args=(sub_commands[subs_running],))
            thread.start()
            host_threads.append(thread)
            host_names.append(sub_hosts[subs_running].name)
            subs_running = subs_running + 1
            hosts_in_use = hosts_in_use + 1

        if pubs_running < len(pub_commands):
            print(f"Call command {pub_commands[pubs_running]} on {pub_hosts[pubs_running]}")
            thread = threading.Thread(target=pub_hosts[pubs_running].cmdPrint, args=(pub_commands[pubs_running],))
            thread.start()
            host_threads.append(thread)
            host_names.append(pub_hosts[pubs_running].name)
            pubs_running = pubs_running + 1
            hosts_in_use = hosts_in_use + 1


    for i in range(hosts_in_use):
        if i > 0 or not broker_mode:
            print(f"Wait for {host_names[i]} to be done")
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
                executions = parsed_args.executions,
                record_time = parsed_args.record_time,
                record_dir = parsed_args.record_dir)
    else:
        print(f"{output_dir} does not exist")
        os.mkdir(output_dir)
        execute(output_dir = output_dir,
                hosts=network.hosts,
                publishers = parsed_args.publishers,
                subscribers = parsed_args.subscribers,
                broker_mode = parsed_args.broker_mode,
                executions = parsed_args.executions,
                record_time = parsed_args.record_time,
                record_dir = parsed_args.record_dir)


    print("Deactivating Network")
    network.stop()


if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
