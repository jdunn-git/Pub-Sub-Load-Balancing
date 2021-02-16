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
