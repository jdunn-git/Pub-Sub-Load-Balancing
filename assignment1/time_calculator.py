import datetime
import argparse
import sys
import os

# parse the command line
parser = argparse.ArgumentParser()

# add optional arguments
parser.add_argument("-d", "--directory", type=str, default="/tmp/assignment_output", help="Directory of test data")
# parse the args
args = parser.parse_args()


if not os.path.isdir(args.directory):
	os.exit()

# Get all entries from -d
entries = os.listdir(args.directory)

pub_data_dict = {}
sub_data_dict = {}

results_file = open("test_results.csv","w")

for ent in entries:
	if ent.startswith("test_"):

		results_file.write(ent + ", ")

		data_files = os.listdir(args.directory+'/'+ent)

		for file in data_files:
			
			# Parse pub files
			if file.startswith("pub"):
				zipcode = file[4:9]
				pub_data_dict[zipcode] = {}

				f = open(args.directory+'/'+ent+'/'+file)

				data_list = []

				for data_line in f.readlines():
					data_list.append(data_line.strip('\n'))
				
				f.close()

				pub_data_dict[zipcode] = data_list

			# Parse sub files
			if file.startswith("sub"):
				zipcode = file[4:9]
				sub_data_dict[zipcode] = {}

				f = open(args.directory+'/'+ent+'/'+file)

				data_list = []

				for data_line in f.readlines():
					data_list.append(data_line.strip('\n'))
				
				f.close()

				sub_data_dict[zipcode] = data_list

		result_count = 0

		# Calculate difference and save in csv file
		for zipcode in pub_data_dict:
			pub_times = pub_data_dict[zipcode]
			sub_times = sub_data_dict[zipcode]

			for i in range(len(pub_times)):
				time_1 = pub_times[i]
				time_2 = sub_times[i]

				delta = float(time_2) - float(time_1)

				result_count += 1

				results_file.write(str(delta))

				if result_count != len(pub_data_dict):
					results_file.write(', ')
		
		results_file.write('\n')

results_file.close()