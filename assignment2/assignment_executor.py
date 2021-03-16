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
    parser.add_argument("-f", "--ratio", type=float, default=1, help="Ratio of subscribers to publishers.")

    parser.add_argument("-b", "--broker_mode", default=False, action="store_true")

    parser.add_argument("-w", "--record_time", default=False, action="store_true")
    parser.add_argument("-d", "--record_dir", type=str, default="timing_data", help="Directory to store timing data")
    
    # parse the args
    args = parser.parse_args()

    args.subs = max(args.subscribers, len(args.topics))
    args.pubs = max(args.publishers, len(args.topics))
    return args


def execute(output_dir, hosts, publishers, subscribers, ratio, broker_mode = False, executions=20, record_time = False, record_dir = "timing_data"):

    pub_commands = []
    pub_hosts = []
    sub_commands = []
    sub_hosts = []
    zk_commands = []
    broker_commands = []

    commands = []

    zk_ip = hosts[len(hosts)-1].IP()

    zipcode = 10101

    pub_mod = 1
    sub_mod = 1

    if ratio > 1:
        sub_mod = 1/ratio
    else:
        pub_mod = ratio

    if broker_mode:
        host_index = 1
        ip_holder = 1
        zipcode = float(10101)
        # Allocate first host as broker
        #commands.append(f"python3 ./broker.py")
        zk_commands.append(f"/opt/zookeeper/bin/zkServer.sh start")
        zk_commands.append(f"/opt/zookeeper/bin/zkServer.sh stop")
        #broker_commands.append(f"python3 ./broker.py -s {zk_ip} -m {publishers*executions}")
        broker_commands.append(f"python3 ./broker.py -s {zk_ip} -a")
        broker_commands.append(f"\n")
        # Allocate commands for publishers and subscribers
        for i in range(publishers):
            zip_holder = int(zipcode)
            pub_commands.append(f"python3 ./publisher.py -s {zk_ip} -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            #pub_commands.append(f"python3 ./publisher.py -s {zk_ip} -z {zip_holder} -b -e {executions} -w -d {output_dir}{record_dir} &")
            pub_hosts.append(hosts[host_index])
            zipcode += 1 * pub_mod
            host_index += 1     

        zipcode = float(10101)
        for i in range(subscribers):
            zip_holder = int(zipcode)
            sub_commands.append(f"python3 ./subscriber.py -s {zk_ip} -z {zip_holder} -b -e {int(executions/2)} -i {i} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            #sub_commands.append(f"python3 ./subscriber.py -s {zk_ip} -z {zip_holder} -b -e {int(executions/2)} -i {i} -w -d {output_dir}{record_dir} &")
            sub_hosts.append(hosts[host_index])
            zipcode += 1 * sub_mod
            host_index += 1
            # Cycle the zipcode if need be
            if i+1 % publishers == 0:
                zipcode = float(10101)

    else:
        host_index = 1
        ip_end = float(1)
        zipcode = float(10101)
        zk_commands.append(f"/opt/zookeeper/bin/zkServer.sh start")
        zk_commands.append(f"/opt/zookeeper/bin/zkServer.sh stop")
        broker_commands.append(f"python3 ./broker.py -s {zk_ip} -a")
        broker_commands.append(f"\n")
        for i in range(publishers):
            zip_holder = int(zipcode)
            #commands.append(f"python3 ./publisher.py -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_commands.append(f"python3 ./publisher.py -s {zk_ip} -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            pub_hosts.append(hosts[host_index])
            zipcode += 1 * pub_mod
            host_index += 1

        zipcode = float(10101)
        for i in range(subscribers):
            ip_holder = int(ip_end)
            zip_holder = int(zipcode)
            #commands.append(f"python3 ./subscriber.py -s 10.0.0.{ip_holder} -z {zip_holder} -e {executions} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.csv")
            sub_commands.append(f"python3 ./subscriber.py -s {zk_ip} -z {zip_holder} -e {int(executions/2)} -i {i} -w -d {output_dir}{record_dir} &> {output_dir}{hosts[host_index].name}.out")
            sub_hosts.append(hosts[host_index])
            ip_end += 1 / ratio
            zipcode += 1 * sub_mod
            host_index += 1
            # Cycle the zipcode and the ip_end if need be
            if i+1 % publishers == 0:
                zipcode = float(10101)
                ip_end = float(1)

    # Run threads on hosts
    host_threads = []
    host_names = []
    hosts_in_use = 0
    pubs_running = 0
    subs_running = 0
    hosts_to_run = len(sub_commands)+len(pub_commands)
    hosts_to_run = hosts_to_run + 1
    #if broker_mode:
    #    hosts_to_run = hosts_to_run + 1

    while hosts_in_use < hosts_to_run:
        print(f"** Hosts to run: {hosts_to_run}, hosts in use: {hosts_in_use}")
        if hosts_in_use == 0:
            print(f"Call command {zk_commands[0]} on {hosts[len(hosts)-1]}")
            thread = threading.Thread(target=hosts[len(hosts)-1].cmdPrint, args=(zk_commands[0],))
            thread.start()
            host_threads.append(thread)
            host_names.append(hosts[len(hosts)-1].name)

            # sleep to allow time for zk to come up before starting broker
            time.sleep(3) # seconds

            print(f"Call command {broker_commands[0]} on {hosts[0]}")
            thread = threading.Thread(target=hosts[0].cmdPrint, args=(broker_commands[0],))
            thread.start()
            host_threads.append(thread)
            host_names.append(hosts[hosts_in_use].name)
            hosts_in_use = hosts_in_use + 1
           
            time.sleep(3) # seconds

        if subs_running < len(sub_commands):
            print(f"Call command {sub_commands[subs_running]} on {sub_hosts[subs_running]}")
            #thread = threading.Thread(target=hosts[i].cmdPrint, args=(commands[i],))
            thread = threading.Thread(target=sub_hosts[subs_running].cmdPrint, args=(sub_commands[subs_running],))
            thread.start()
            host_threads.append(thread)
            host_names.append(sub_hosts[subs_running].name)
            subs_running = subs_running + 1
            hosts_in_use = hosts_in_use + 1
            time.sleep(0.2) # seconds

        if pubs_running < len(pub_commands):
            print(f"Call command {pub_commands[pubs_running]} on {pub_hosts[pubs_running]}")
            thread = threading.Thread(target=pub_hosts[pubs_running].cmdPrint, args=(pub_commands[pubs_running],))
            thread.start()
            host_threads.append(thread)
            host_names.append(pub_hosts[pubs_running].name)
            pubs_running = pubs_running + 1
            hosts_in_use = hosts_in_use + 1
            time.sleep(0.2) # seconds

        print(f"** Hosts to run: {hosts_to_run}, hosts in use: {hosts_in_use}")

    print(f"** Have {hosts_in_use+1} hosts to join")
    for i in range(hosts_in_use):
        if i > 1:
            print(f"Wait for {host_names[i]} to be done")
            host_threads[i].join()
    '''
    thread = threading.Thread(target=hosts[0].cmdPrint, args=(broker_commands[1],))
    thread.start()
    thread.join()
    '''
    time.sleep(3)

    print("Just need to join the broker and zookeeper nodes")

    #host_threads[0].join()
    #host_threads[1].join()

    print("Terminating Broker")
    hosts[0].terminate()
    print("Terminated Broker")
    #hosts[len(hosts)-1].terminate()
    #time.sleep(3)
    thread = threading.Thread(target=hosts[len(hosts)-1].cmdPrint, args=(zk_commands[1],))
    thread.start()
    thread.join()
    time.sleep(3)


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
                ratio = parsed_args.ratio,
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
                ratio = parsed_args.ratio,
                broker_mode = parsed_args.broker_mode,
                executions = parsed_args.executions,
                record_time = parsed_args.record_time,
                record_dir = parsed_args.record_dir)


    print("Deactivating Network")
    try:
        network.stop()
    except:
        print("Everything should be done")

if __name__ == '__main__':
    # Tell mininet to print useful information
    setLogLevel('info')
    main ()
