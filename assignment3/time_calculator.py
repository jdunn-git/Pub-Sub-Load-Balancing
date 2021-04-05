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

results_file = open("test_results.csv","w")

for ent in entries:
	if ent.startswith("test_"):
		print(f"Generating data for {ent}")
		pub_data_dict = {}
		sub_data_dict = {}

		results_file.write(ent + ", ")

		data_files = os.listdir(args.directory+'/'+ent)

		for file in data_files:
			
			# Parse pub files
			if file.startswith("pub"):
				zipcode = file[4:file.find('.')]
				pub_data_dict[zipcode] = {}

				f = open(args.directory+'/'+ent+'/'+file)

				data_dict = {}

				for data_line in f.readlines():
					data = data_line.strip('\n')
					d_zip, d_num, d_temp, d_hum, d_time = data.split()
					key = f"{d_zip} {d_temp} {d_hum}"
					data_dict[key] = d_time
				
				f.close()

				pub_data_dict[zipcode] = data_dict

			# Parse sub files
			if file.startswith("sub"):
				zipcode = file[4:file.find('.')]
				sub_data_dict[zipcode] = {}

				f = open(args.directory+'/'+ent+'/'+file)

				data_dict = {}

				for data_line in f.readlines():
					data = data_line.strip('\n')
					d_zip, d_temp, d_hum, d_time = data.split()
					key = f"{d_zip} {d_temp} {d_hum}"
					data_dict[key] = d_time
				
				f.close()

				sub_data_dict[zipcode] = data_dict

		zip_count = 0

		# Calculate difference and save in csv file
		for zipcode_key in sub_data_dict:
			dash_index = zipcode_key.find('-')
			zipcode = zipcode_key
			if dash_index != -1:
				zipcode = zipcode_key[0:dash_index]
			pub_times = pub_data_dict[zipcode]
			sub_times = sub_data_dict[zipcode_key]

			zip_count+= 1

			result_count = 0

			for key in sub_times:
				# Get times in milliseconds
				if pub_times.get(key) == None:
					print(pub_times)
				time_1 = float(pub_times[key]) * 1000
				time_2 = float(sub_times[key]) * 1000

				delta = time_2 - time_1

				result_count += 1

				results_file.write(str(delta))

				# This wont print the comma on the last data entry from the last zipcode
				if zip_count != len(sub_data_dict) or result_count != len(sub_times):
					results_file.write(', ')
		
		results_file.write('\n')

results_file.close()