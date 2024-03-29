# import some basic packages just in case we need these
import os
import sys
import time

# Now import the kazoo package that supports Python binding
# to ZooKeeper
from kazoo.client import KazooClient   # client API
from kazoo.client import KazooState    # for the state machine

# to avoid any warning about no handlers for logging purposes, we
# do the following
import logging
logging.basicConfig ()

#--------------------------------------------------------------------------
# define a callback function to let us know what state we are in currently
# Kazoo is implemented in such a way that the system goes thru 3 states.
# ZooKeeper clients go thru 3 states:
#    LOST => when it is instantiated  or when not in a session with a server;
#    CONNECTED => when connected with server, and
#    SUSPENDED => when the connection  is lost or the server node is no
#                                longer part of the quorum
#
#--------------------------------------------------------------------------
def listener4state (state):
    if state == KazooState.LOST:
        print ("Current state is now = LOST")
    elif state == KazooState.SUSPENDED:
        print ("Current state is now = SUSPENDED")
    elif state == KazooState.CONNECTED:
        print ("Current state is now = CONNECTED")
    else:
        print ("Current state now = UNKNOWN !! Cannot happen")
        
# ------------------------------------------------------------------
# The driver class. Does not derive from anything
#
class ZK_Driver ():
    """ The ZooKeeper Driver Class """

    #################################################################
    # constructor
    #################################################################
    def __init__ (self, zk_ip, zk_port):
        self.zk = None  # session handle to the zookeeper server
        self.zkIPAddr = zk_ip  # ZK server IP address
        self.zkPort = zk_port # ZK server port num
        self.zkName = '/foo' # refers to the znode path being manipulated
        self.zkVal = b'bar' # refers to the znode value

    #-----------------------------------------------------------------------
    # Debugging: Dump the contents

    def dump (self):
        """dump contents"""
        print ("=================================")
        print ("Server IP: {}, Port: {}; Path = {} and Val = {}".format (self.zkIPAddr, self.zkPort, self.zkName, self.zkVal))
        print ("=================================")

    # -----------------------------------------------------------------------
    # Initialize the driver
    # -----------------------------------------------------------------------
    def init_driver (self):
        """Initialize the client driver program"""

        try:
            # debug output
            self.dump ()

            # instantiate a zookeeper client object
            # right now only one host; it could be the ensemble
            hosts = self.zkIPAddr + str (":") + str (self.zkPort)
            print ("Driver::init_driver -- instantiate zk obj: hosts = {}".format(hosts))

            # instantiate the kazoo client object
            self.zk = KazooClient (hosts)

            # register it with the state listener.
            # recall that the "listener4state" is a callback method
            # we defined above and so we are just passing the pointer
            # to this callback to the listener method on kazoo client.
            self.zk.add_listener (listener4state)
            print ("Driver::init_driver -- state after connect = {}".format (self.zk.state))
            
        except:
            print ("Unexpected error in init_driver:", sys.exc_info()[0])
            raise

    # -----------------------------------------------------------------------
    # A watcher function to see if value for a node in the znode tree
    # has changed
    # -----------------------------------------------------------------------
    def watch_znode_data_change (self):

        # we don't do anything inside this function but rather set an
        # actual watch function
        
        #*****************************************************************
        # This is the watch callback function that is supposed to be invoked
        # when changes get made to the znode of interest. Note that a watch is
        # effective only once. So the client has to set the watch every time.
        # To overcome the need for this, Kazoo has come up with a decorator.
        # Decorators can be of two kinds: watching for data on a znode changing,
        # and children on a znode changing
        @self.zk.DataWatch(self.zkName)
        def dump_data_change (data, stat):
            print ("\n*********** Inside watch_znode_data_change *********")
            print(("Data changed for znode: data = {}, stat = {}".format (data,stat)))
            print ("*********** Leaving watch_znode_data_change *********")

    # -----------------------------------------------------------------------
    # start a session with the zookeeper server
    #
    def start_session (self):
        """ Starting a Session """
        try:
            # now add_nodeconnect to the server
            self.zk.start ()

        except:
            print("Exception thrown in start (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # stop a session with the zookeeper server
    #
    def stop_session (self):
        """ Stopping a Session """
        try:
            print(self.zkIPAddr)
            #
            # now disconnect from the server
            self.zk.stop ()

        except:
            print("Exception thrown in stop (): ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # create a znode
    #
    def create_znode (self):
        """ ******************* znode creation ************************ """
        try:
            # here we create a node just like we did via the CLI. But here we are
            # also showcasing the ephemeral attribute which means that the znode
            # will be deleted automatically by the server when the session is
            # terminated by this client. The "makepath=True" parameter ensures that
            # the znode will first be created and then a value attached to it.
            #
            # Note that we do not check here if the node already exists. If it does,
            # then we will get an exception
            print ("Creating an ephemeral znode {} with value {}".format(self.zkName,self.zkVal))
            self.zk.create (self.zkName, value=self.zkVal, ephemeral=True, makepath=True)

        except:
            print("Exception thrown in create (): ", sys.exc_info()[0])
            return

        
    # -----------------------------------------------------------------------
    # Retrieve the value stored at a znode
    def get_znode_value (self):
        
        """ ******************* retrieve a znode value  ************************ """
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            print ("Checking if {} exists (it better be)".format(self.zkName))
            if self.zk.exists (self.zkName):
                print ("{} znode indeed exists; get value".format(self.zkName))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get (self.zkName)
                print(("Details of znode {}: value = {}, stat = {}".format (self.zkName, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(self.zkName))

        except:
            print("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # Modify the value stored at a znode
    def modify_znode_value (self, new_val):
        
        """ ******************* modify a znode value  ************************ """
        try:
            # Now let us change the data value on the znode and see if
            # our watch gets invoked
            print ("Setting a new value = {} on znode {}".format (new_val, self.zkName))

            # make sure that the znode exists before we actually try setting a new value
            if self.zk.exists (self.zkName):
                print ("{} znode still exists :-)".format(self.zkName))

                print ("Setting a new value on znode")
                self.zk.set (self.zkName, new_val)

                # Now see if the value was changed
                value,stat = self.zk.get (self.zkName)
                print(("New value at znode {}: value = {}, stat = {}".format (self.zkName, value, stat)))

            else:
                print ("{} znode does not exist, why?".format(self.zkName))

        except:
            print("Exception thrown checking for exists/set: ", sys.exc_info()[0])
            return

    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # -----------------------------------------------------------------------
    # Run the driver
    #
    # We do a whole bunch of things to demonstrate the use of ZooKeeper
    # Note that as you are trying this out, use the ZooKeeper CLI to verify
    # that indeed these things are happening on the server (just as a validation)
    # -----------------------------------------------------------------------
    def run_driver (self):
        """The actual logic of the driver program """
        try:
            # now start playing with the different CLI commands programmatically

            # first step is to start a session
            print ("\n")
            input ("Starting Session with the ZooKeeper Server -- Press any key to continue")
            self.start_session ()
 
            # Next we demo the creation of a znode. Here we create an ephemeral node
            print ("\n")
            input ("Creating a znode -- Press any key to continue:")
            self.create_znode ()

            # next we demo retrieving a stored value at a znode. 
            print ("\n")
            input ("Obtain stored value -- Press any key to continue")
            self.get_znode_value ()

            # next we demo modifying the value stored at a znode
            print ("\n")
            input ("Modify stored value -- Press any key to continue")
            self.modify_znode_value (b"bar2")

            # next we demo retrieving a stored value at a znode. 
            print ("\n")
            input ("Obtain the modified stored value -- Press any key to continue")
            self.get_znode_value ()

            # now let us disconnect. Doing so should delete our znode because
            # it is ephemeral
            print ("\n")
            input ("Disconnect from the server -- Press any key to continue")
            self.stop_session ()

            # start another session to see if the node magically comes back up
            print ("\n")
            input ("Starting new Session to the ZooKeeper Server -- Press any key to continue")
            self.start_session ()
 
            # now check if the znode still exists
            print ("\n")
            input ("check if the node still exists -- Press any key to continue")
            if self.zk.exists (self.zkName):
                print ("{} znode still exists -- not possible".format (self.zkName))
            else:
                print ("{} znode no longer exists as expected".format (self.zkName))

            # disconnect once again
            print ("\n")
            input ("Disconnecting for the final time -- Press any key to continue")
            self.stop_session ()

            # cleanup
            print ("\n")
            input ("Cleaning up the handle -- Press any key to continue")
            self.zk.close ()

        except:
            print("Exception thrown: ", sys.exc_info()[0])

    ## Functions added for assignment 2

    def add_node(self, name, value, persistent):
        # here we create a node just like we did via the CLI. But here we are
        # also showcasing the ephemeral attribute which means that the znode
        # will be deleted automatically by the server when the session is
        # terminated by this client. The "makepath=True" parameter ensures that
        # the znode will first be created and then a value attached to it.
        #
        # Note that we do not check here if the node already exists. If it does,
        # then we will get an exception
        print(f"Creating an ephemeral znode {name} with value {value}")
        self.zk.create(name, value=value, ephemeral=not persistent, makepath=True)

        #except:
            #print("Exception thrown in create (): ", sys.exc_info()[0])
            #return

    def update_value(self, name, value):
        print ("Checking if {} exists (it better be)".format(name))
        if self.zk.exists (name):
            print ("{} znode indeed exists; get value".format(name))

            self.zk.set (name, value)


    def check_for_node(self, name):
        try:
            print(f"Checking for an ephemeral znode {name}")
            return self.zk.exists(name)
        except:
            print("Exception thrown in exists (): ", sys.exc_info()[0])
            return False        

    def watch_node(self, name, watch_func):
        try:
            #print(f"Watching for an ephemeral znode {name}")
            return self.zk.exists(name, watch=watch_func)
        except:
            print("Exception thrown in get (): ", sys.exc_info()[0])
            return False        

    def get_node(self, name):
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            #print ("Checking if {} exists (it better be)".format(name))
            if self.zk.exists (name):
                #print ("{} znode indeed exists; get value".format(name))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get (name)
               # print(("Details of znode {}: value = {}, stat = {}".format (name, value, stat)))
                return value
            else:
                print ("{} znode does not exist, why?".format(name))

        except:
            print("Exception thrown checking for exists/get: ", sys.exc_info()[0])
            return

    def get_node_if_exists(self, name):
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            print ("Checking if {} exists".format(name))
            if self.zk.exists (name):
                print ("{} znode indeed exists; get value".format(name))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value,stat = self.zk.get (name)
                print(("Details of znode {}: value = {}, stat = {}".format (name, value, stat)))
                return value

        except:
            print("Exception thrown getting znode: ", sys.exc_info()[0])
            return

    def get_children(self, name):
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            print ("Checking if {} exists".format(name))
            if self.zk.exists (name):
                print ("{} znode indeed exists; get value".format(name))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value = self.zk.get_children(name)
                print(("Details of znode {}: value = {}".format (name, value)))
                return value

        except:
            print("Exception thrown getting children of znode: ", sys.exc_info()[0])
            return

    def delete_node(self, name):
        try:

            # Now we are going to check if the znode that we just created
            # exists or not. Note that a watch can be set on create, exists
            # and get/set methods
            #print ("Checking if {} exists (it better be)".format(name))
            if self.zk.exists (name):
                #print ("{} znode indeed exists; get value".format(name))

                # Now acquire the value and stats of that znode
                #value,stat = self.zk.get (self.zkName, watch=self.watch)
                value = self.zk.delete (name)
               # print(("Details of znode {}: value = {}, stat = {}".format (name, value, stat)))
                return value
            else:
                print ("{} znode does not exist, why?".format(name))

        except:
            print("Exception thrown deleting znode: ", sys.exc_info()[0])
            return

    #def get_or_watch_node(self, name, func):
    #    try:
    #        print ("Checking if {} exists".format(name))
    #        if self.zk.exists (name):
    #            print ("{} znode indeed exists; get value".format(name))
    #        else:
    #            print ("{} znode does not exist; watching".format(name))
    #    except:            
    #        print("Exception thrown trying to get or watch znode: ", sys.exc_info()[0])
    #        return
