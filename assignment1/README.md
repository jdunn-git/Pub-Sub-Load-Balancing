# Assignment 1

The functionality of the assignment depends on vagrant. To operate the assignment, you must have vagrant installed on host.

## Executing The Assignment
1. `vagrant up`
2. `vagrant ssh`
3. `cd /vagrant/`
4. Here you can either run Broker Mode or Flood Mode. This is designated by the -b flag.
The number of publishers and subscribers can be changed with the `-p` and `-s` respectivelly.
In order to control testing, you can change the amount of executions with `e`

Broker Mode: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -b -e 20`

Flood Mode: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -e 20`

## Timing Experiments
To time it, add a couple of new parameters to the executable like so: `sudo python3 /vagrant/assignment_executor.py -s 5 -p 5 -b -e 20 -w -d "test_1"`

This will generate a new directory for "test_1" in the assignment_output directory. 

Once you have finished, you can run `python3 timing_calculator.py` and it will generate a csv file in the assignment1/ folder using all of the timing data that has been generated so far.

We can run additional tests and increment the -d directoy name, and the output will include all tests ran up to that point. If we want to remove any data, then we will need to manually clean up that directory from the assignment_output folder.

> Note that the csv structure is: [test_name], [pub-sub pair 1 time difference 1], [pub-sub pair 1 time difference 2], ..., [pub-sub pair n time difference m], where n is the number of pub-sub pairs, and m is the number of messages received by the subscribers