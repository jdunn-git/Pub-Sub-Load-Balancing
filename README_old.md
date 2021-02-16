# Getting Started

You will need to run processes on separate hosts in order to manually tests the zmq api that we have developed.

To start mininet, you can run the command ```sudo mn -x --topo=tree,fanout=3,depth=2```

# Running the Processes

On separate hosts, you will need to be prepared to run commands separately based on which api mode you will be using. We are using command line arguments to switch between broker mode and brokerless mode.

Each of the three processes (publisher, subscriber, and broker) will have need their own unique command to run from inside of mininet.

## Publisher

```$ python3 publisher.py [zipcode] [broker_ip] [mode]```
 - zipcode: This is the custom zipcode for which the publisher will be producing weather data.
 - broker_ip: This will be the ip address of the broker node. If running brokerless, then this param will be ignored.
 - mode: This will determine broker vs brokerless mode. For brokerless mode, use 0, for all other values, use 1.

## Subscriber

```$ python3 subscriber.py [publisher_ip] [zipcode] [mode]```
 - publisher_ip: This will be the ip address of the broker node if using a broker. If not, then it should be the ip of a publisher.
 - zipcode: This is the custom zipcode for which the publisher will be producing weather data.
 - mode: This will determine broker vs brokerless mode. For brokerless mode, use 0, for all other values, use 1.


## Broker

```$ python3 broker.py```

This will start the broker. No other arguments are needed.

# Performing the Test

It is easiest to test brokerless mode and broker mode independently.

## Broker Test Example

This test will show multiple pubs sending data to the same sub, and mutliple subs receiving data from the same pub.

You will need to designate one node to be the broker, and any number of node to be publishers and subscribers. In this example, we'll let host 1 be the broker, host 2, 3 and 4 be publishers, and host 5, 6, and 7 be subscribers.

We will be using zip codes 10101 and 10102 to demonstrate data being routed correctly. You can add publishers and subscribers for either zipcode, or add any additional zipcode, and data will still flow properly.

Using this configuration, you would run the following commands on the corresponding hosts:

#### Broker:
On host 1, run:
```$ python3 broker.py```

#### Publishers:
On host 2, run:
```$ python3 publisher.py 10101 10.0.0.1 1```

On host 3, run:
```$ python3 publisher.py 10101 10.0.0.1 1```

On host 4, run:
```$ python3 publisher.py 10102 10.0.0.1 1```

#### Subscribers:
On host 5, run:
```$ python3 subscriber.py 10.0.0.1 10101 1```

On host 6, run:
```$ python3 subscriber.py 10.0.0.1 10102 1```

On host 7, run:
```$ python3 subscriber.py 10.0.0.1 10102 1```




## Brokerless Test Example

This test will show multiple pubs pushing data directly to multiple subs.

You will need to designate a couple of nodes as the publishers and several nodes as subscribers. In this example, we'll let hosts 1, 2, and 3 be publishers, and hosts 4, 5, and 6 be subscribers.

We will be using zip codes 10101 and 10102 to demonstrate data being routed correctly. You can add publishers and subscribers for either zipcode, or add any additional zipcode, and data will still flow properly.

Using this configuration, you would run the following commands on the corresponding hosts:

#### Publishers:
On host 1, run:
```$ python3 publisher.py 10101 10.0.0.1 0```

On host 2, run:
```$ python3 publisher.py 10101 10.0.0.1 0```

On host 3, run:
```$ python3 publisher.py 10102 10.0.0.1 0```

#### Subscribers:
On host 4, run:
```$ python3 subscriber.py 10.0.0.1 10101 0```

On host 5, run:
```$ python3 subscriber.py 10.0.0.3 10102 0```

On host 6, run:
```$ python3 subscriber.py 10.0.0.3 10102 0```
