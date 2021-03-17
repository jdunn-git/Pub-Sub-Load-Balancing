# Assignment 2

The functionality of the assignment depends on vagrant. To operate the assignment, you must have vagrant installed on host.

## Executing The Assignment
1. `vagrant up`
2. `vagrant ssh`
3. `cd /vagrant/`
4. Here you can either run Broker Mode or Flood Mode. This is designated by the -b flag.
The number of publishers and subscribers can be changed with the `-p` and `-s` respectivelly.
In order to control testing, you can change the amount of executions with `e`
You can also use the `-f` flag to set the ratio of subscribers to publishers. By default, this ratio of 1.
> Note that you need to consider the pub and sub count if using -f. If you want multiple publishers to send to the same subscriber, you can set `-f` to 0.5, but you also need to make sure `-s` is at least half of `-p`.

Broker Mode: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -f 1 -b -e 20`

Flood Mode: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -f 1 -e 20`


## Timing Experiments
To time it, add a couple of new parameters to the executable like so: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -b -e 20 -w -d "test_1"`

This will generate a new directory for "test_1" in the assignment_output directory.

Once you have finished, you can run `python3 timing_calculator.py` and it will generate a csv file in the assignment1/ folder using all of the timing data that has been generated so far.

We can run additional tests and increment the -d directoy name, and the output will include all tests ran up to that point. If we want to remove any data, then we will need to manually clean up that directory from the assignment_output folder.

> Note that the csv structure is: [test_name], [pub-sub pair 1 time difference 1], [pub-sub pair 1 time difference 2], ..., [pub-sub pair n time difference m], where n is the number of pub-sub pairs, and m is the number of messages received by the subscribers

## Performance and Unit Test Results:
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/stats.png)

![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P0_S10_Flood.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P10_S10_Broker.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P10_S50_Broker.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P10_S50_Flood.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P10_S100_Broker.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P10_S100_Flood.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P50_S10_Broker.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P50_S10_Flood.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P100_S10_Broker.png)
![](https://github.com/jdunn-git/CS6381-Assignment-1/blob/master/assignment2/assignment_output/images/P100_S10_Flood.png)

## Assignment Work Distribution

#### Pub/Sub Model: Joshua Dunn (3 hours)

#### Flooder: Joshua Dunn (3 hours)
* Implemented the flooder architecture with time tracking
* Iimplemented Flooder with data capture
* Incorporate ZooKeeper

#### Broker: Joshua Dunn (3 hours)
* Implemented the broker architecture.
* Incorporate ZooKeeper

#### Data Analytics: Terrence Williams (1 hour)
* Unit test data munging
* Data cleaning and analysis

#### Video Presentation and Documentation: Joshua Dunn and Terrence Williams (1 hour)
* Setup Graphs on readme
* Created the submission video
