import requests
import urllib.request
from utilities.read_write_utilities import read_set,write_to_csv
import time
from bs4 import BeautifulSoup
import pickledb
import json
import glob
import csv


drugs = read_set('/Users/sandeep.dey/Downloads/2020-02-06_scrape/drugs')
print(drugs)
output_records = []
# fields = ["equiv_name","coupon_network","npi","default_quantity","price_type","scrape_date","price","root","dosage",
#           "generic","drug_id","date","form_name","ncpdp","pharmacy","geo","slug","quantity"]

fields = ["equiv_name","default_quantity","root","dosage","generic","drug_id","form_name","slug"]

for drug in drugs:
    # print('/Users/sandeep.dey/Downloads/2020-02-06_scrape/%s'%drug)
    with open('/Users/sandeep.dey/Downloads/2020-02-06_scrape/%s'%drug) as json_file:
        for record in json.load(json_file):
            # print(record)
            output_records.append({field:str(record[field]) if field in record else '' for field in fields})
write_to_csv('/Users/sandeep.dey/Downloads/2020-02-06_scrape/units_of_use_data.csv',output_records)


# filename  = '/Users/sdey/Downloads/privia_utilization_data.csv'
# output_filename = '/Users/sdey/Downloads/privia_utilization_raw_data.csv'
#
# with open(filename, 'r') as input_file:
#     with open(output_filename, 'w') as output_file:
#         reader = csv.DictReader(input_file)
#         writer = csv.DictWriter(output_file, fieldnames=reader.fieldnames)
#         writer.writeheader()
#         number_of_lines = 0
#         for row in reader:
#             row['Medication Name'] = row['Medication Name'].replace(',',':')
#             writer.writerow(row)
#             number_of_lines+=1
#             if number_of_lines % 10000 == 0 :
#                 print('%d lines'%number_of_lines)

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
