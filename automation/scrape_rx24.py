from utilities.scrapping_utilities import ScraperRequest
from os import path

from http import cookies
from utilities.logging_setup import logger
import time
import uuid
import datetime
import string
import json
from utilities.read_write_utilities import read_csv, print_object, write_to_csv

def main():
    base_dir = '/Users/sandeep.dey/Documents/data/rxpharmacycoupons/'
    urls = read_csv(base_dir+'manufacturer_coupons.csv')
    output_header = ['med_desc','product','saving_program_url','details','details_2','ended']
    # print_response(data)
    sc = ScraperRequest(base_url='https://www.rxpharmacycoupons.com/')

    dataset = []
    for n,item in enumerate(urls):
        print_object(item)
        print('%d th Object '%n)
        soup = sc.get_parsed_html(item['url'])
        med_desc = item['desc']
        data={}
        data['med_desc']=item['desc']
        data['product']=soup.find_all('p',class_='product')[0].string
        data['saving_program_url'] = soup.find_all('a', class_='btn btn-lg btn-offer')[0].get('href')

        middle_div = soup.find_all('div', class_='middle')[0]
        data['details'] = middle_div.find('h3').string
        data['details_2'] = middle_div.find('p',class_='details').string
        data['ended'] = middle_div.find('p',class_='ended').string if middle_div.find('p',class_='ended') is not None else ''

        dataset.append(data)

    write_to_csv(filename=base_dir + 'output.csv', data=dataset, append=False, header=output_header)


if __name__== "__main__":
    main()





