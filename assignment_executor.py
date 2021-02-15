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
    parser.add_argument('topics', metavar="tops", type=str, nargs="+")
    parser.add_argument("-s", "--subscribers", type=int, default=10, help="Number of subscribers, default 10, minimum is number of topics")
    parser.add_argument("-p", "--publishers", type=int, default=2, help="Number of publishers, default 2, minimum is number of topics")
    parser.add_argument("-r", "--racks", type=int, default=1, help="Number of racks, choices 1, 2 or 3")
    parser.add_argument("-f", "--folder", type=str, default="NEW_REPORT")

    parser.add_argument("-b", "--broker_mode", default=False, action="store_true")

    # parse the args
    args = parser.parse_args ()

    args.subs = max(args.subscribers, len(args.topics))
    args.pubs = max(args.publishers, len(args.topics))
    return args


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
    network = Mininet(topology, link=TCLink, controller = OVSController)

    # activate the network
    print("Activating network")
    network.start()

    # debugging purposes
    print("Dumping host connections")
    dumpNodeConnections(network.hosts)
