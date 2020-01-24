import requests
import urllib.request
from utilities.read_write_utilities import read_list_from_file,write_list_to_csv
import time
from bs4 import BeautifulSoup
import pickledb
import json
import glob

def get_all_files():
    files = glob.glob("/Users/sandeep.dey/Downloads/2020-01-03_scrape/*")
    files_2 = glob.glob("/Users/sandeep.dey/Downloads/2020-01-02_scrape/*")
    files.extend(files_2)
    return files

def get_json_from_file(file_name):
    with open(file_name) as json_file:
        data = json.load(json_file)
        return data

def collect_fields(data_blobs):
    index = []
    output = []
    for data in data_blobs:
        key = ('%s:%s:%s',data['drug_id'],data['dosage'],data['form_name'])
        if key not in index:
            output.append({
                'default_quantity': data['default_quantity'],
                'dosage': data['dosage'],
                'drug_id': data['drug_id'],
                'form_name': data['form_name'],
                'slug': data['slug']})
            index.append(key)
    return output

def main():
    files = get_all_files()
    return_object = []
    for file in files:
        data = get_json_from_file(file)
        return_object.extend(collect_fields(data))
    # print(return_object)
    write_list_to_csv('units_of_use_raw_data.csv',return_object)

if __name__== "__main__":
    main()





