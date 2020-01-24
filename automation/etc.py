import requests
import urllib.request
from utilities.read_write_utilities import read_list_from_file,write_list_to_csv
import time
from bs4 import BeautifulSoup
import pickledb
import json
import glob
import csv



filename  = '/Users/sdey/Downloads/privia_utilization_data.csv'
output_filename = '/Users/sdey/Downloads/privia_utilization_raw_data.csv'

with open(filename, 'r') as input_file:
    with open(output_filename, 'w') as output_file:
        reader = csv.DictReader(input_file)
        writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames)
        writer.writeheader()
        number_of_lines = 0
        for row in reader:
            row['Medication Name'] = row['Medication Name'].replace(',',':')
            writer.writerow(row)
            number_of_lines+=1
            if number_of_lines % 10000 == 0 :
                print('%d lines'%number_of_lines)

#
# filename = '/Users/sandeep.dey/Downloads/pricing_nadac_cost_20190515.csv'
# output_filename = '/Users/sandeep.dey/Downloads/pricing_nadac_cost_20190515_output.csv'
#
# with open(filename, 'r') as input_file:
#     with open(output_filename, 'w') as output_file:
#         reader = csv.DictReader(input_file)
#         fieldnames = ['ndc','nadac_per_unit','effective_date','pricing_unit','otc',
#                       'explanation_code','classification_for_rate_setting','corresponding_generic_drug_nadac_per_unit',
#                       'corresponding_generic_drug_effective_date','as_of_date']
#         writer = csv.DictWriter(output_file, fieldnames=fieldnames)
#         writer.writeheader()
#         number_of_lines = 0
#         for row in reader:
#             row['explanation_code'] = row['explanation_code'].replace('\"','').replace(',','').replace(' ','')
#             row.pop('ndc_description')
#             row.pop('pharmacy_type_indicator')
#             writer.writerow(row)
#             number_of_lines+=1
#             if number_of_lines % 10000 == 0 :
#                 print('%d lines'%number_of_lines)
