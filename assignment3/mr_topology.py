
#
# Vanderbilt University, Computer Science
# CS4287-5287: Principles of Cloud Computing
# Author: Aniruddha Gokhale
# Created: Nov 2016
#
#  Purpose: To define a topology class for our map reduce framework to run on
#

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

# @NOTE@:  I do not think any change is needed to this logic

class MR_Topo(Topo):
    "Map Reduce Topology."
    # override the build method. We define the number of racks. If Racks == 1,
    # All the map and reduce nodes are on the same rack. If Racks==2, then master
    # node is on rack while map nodes are on second rack but reduce are back on
    # same switch as master node (sounds silly). If Racks==3 then the master is on one
    # rack, the map nodes on 2nd rack and reduce nodes on the third rack. Number of
    # switches equals the number of racks.

    def build(self, racks=1, publishers=10, subscribers=3, broker_mode=False):
        print(f"Topology: Racks = {racks}, "
                f"publishers = {publishers}, "
                f"subsribers = {subscribers}, "
                f"broker_mode = {broker_mode}")
        self.mr_switches = []
        self.mr_hosts = []
        # Python's range(N) generates 0..N-1
        for r in range(racks):
            # a switch per rack.
            switch = self.addSwitch (f's{r+1}')
            print("Added switch", switch)
            self.mr_switches.append (switch)
            if (r > 0):
                # connect the switches
                self.addLink (self.mr_switches[r-1], self.mr_switches[r], delay='5ms')
                print("Added link between", self.mr_switches[r-1], " and ", self.mr_switches[r])

        host_index = 0
        switch_index = 0
        # Now add the master node (host master) on rack 1, i.e., switch 1

        #if broker_mode:
        # Now add the master node (host master) on rack 1, i.e., switch 1
        host = self.addHost (f'ZKh{host_index+1}s{switch_index+1}')
        print("Added master host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1


        # Now add broker nodes
        # Broker 0
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Broker 1
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Broker 2
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Broker 3
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Broker 4
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Broker 5
        host = self.addHost (f'BROKERh{host_index+1}s{switch_index+1}')
        print("Added broker host", host)
        self.addLink (host, self.mr_switches[switch_index], delay='1ms')  # zero based indexing
        print(f"Added link between {host} and switch {self.mr_switches[switch_index]}")
        self.mr_hosts.append (host)
        host_index += 1

        # Now add the SUB nodes
        switch_index = 1
        for h in range (subscribers):
            SWITCH = (switch_index % racks) - 1;
            host = self.addHost('SUBh{}s{}'.format (host_index+1, SWITCH+1))
            print("Added next SUB host", host)
            self.addLink(host, self.mr_switches[SWITCH], delay='1ms')
            print(f"Added link between {host} and switch {self.mr_switches[SWITCH]}")
            self.mr_hosts.append(host)

            switch_index += 1
            host_index += 1

        # Now add the PUB nodes
        switch_index = 1
        for h in range (publishers):
            SWITCH = (switch_index % racks) - 1;
            host = self.addHost(f"PUBh{host_index+1}s{SWITCH+1}")
            print(f"Added next PUB host {host}")
            self.addLink(host, self.mr_switches[SWITCH], delay='1ms')
            print(f"Added link between {host} and switch {self.mr_switches[SWITCH]}")
            self.mr_hosts.append(host)

            switch_index += 1
            host_index += 1
