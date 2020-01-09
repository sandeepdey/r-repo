import requests
import urllib.request

from selenium.webdriver.support.wait import WebDriverWait

from utilities.read_write_utilities import read_list_from_file
import time
from bs4 import BeautifulSoup
import pickledb
import json
from selenium import webdriver
import sys
import six

# def get_links(i):
#     url = 'https://honeybeehealth.com/online-pharmacy?p=' + str(i)
#     response = requests.get(url)
#     soup = BeautifulSoup(response.text, 'html.parser')
#     links = soup.findAll('a')
#     drug_links = [s.get('href') for s in links if
#                   s.get('href') is not None and s.get('href').startswith('https://honeybeehealth.com/drugs/')]
#     return drug_links

# def main():
#     db = pickledb.load('honeybeehealth.db', True)
#     for i in range(1, 40):
#         print(str(i) + ' - Getting Page No')
#         if db.exists('page-' + str(i)):
#             print('\tPage Data Already Exists')
#             continue
#         drug_links = get_links(i)
#         with open('links.csv', 'a') as myfile:
#             for drug in drug_links:
#                 myfile.write(drug+'\n')
#         db.set('page-' + str(i), True)
#         print('\tNew Data Addition')
#         db.dump()
#         time.sleep(5)

# def main():
#     db = pickledb.load('honeybeehealth.db', True)
#     input_drug_file = 'hd-drugs.csv'
#     drugs = read_list_from_file(input_drug_file)
#     for index, drug_name in enumerate(drugs,start=1):
#         print(str(index) + ' - Getting Drug Info for - ' + drug_name)
#         if db.exists(drug_name):
#             print('\tDrug Data Already Exists')
#             continue
#         parse_drug_page(drug_name)
#         db.set(drug_name, True)
#         print('\tNew Drug Data Added')
#         db.dump()
#         time.sleep(1)


# def parse_drug_page(drug_name):
#
#     url = 'https://honeybeehealth.com/drugs/' + drug_name
#     response = requests.get(url)
#     soup = BeautifulSoup(response.text, 'html.parser')
#     content = [x for x in soup.find_all('script') if 'spConfig' in x.text]
#     t = json.loads(content[0].getText())
#     drug_data = t['#product_addtocart_form']['configurable']['spConfig']
#
#     drug_forms = {}
#     for s in drug_data['attributes']['147']['options']:
#         drug_forms[s['id']]=s['label']
#
#     drug_strengths = {}
#     for s in drug_data['attributes']['149']['options']:
#         drug_strengths[s['id']]=s['label']
#
#     package_size = {}
#     for s in drug_data['attributes']['148']['options']:
#         package_size[s['id']]=s['label']
#
#     drug_prices = {}
#     for s in drug_data['optionPrices'].keys():
#         drug_prices[s]=drug_data['optionPrices'][s]['finalPrice']['amount']
#
#     info = {}
#     with open('drug_scrape.csv', 'a') as myfile:
#         for s in drug_data['index'].keys():
#             info['strength'] = drug_strengths[drug_data['index'][s]['149']]
#             info['package'] = package_size[drug_data['index'][s]['148']]
#             info['form'] = drug_forms[drug_data['index'][s]['147']]
#             info['price'] = str(drug_prices[s])
#             myfile.write(drug_name+','+info['strength']+','+info['form']+','+info['package']+','+info['price']+'\n')




def get_json_data(url):
    PROXY = "12.345.678.910:8080"
    chrome_options = WebDriverWait.ChromeOptions()
    chrome_options.add_argument('--proxy-server=%s' % PROXY)
    executable_path = "/Users/sandeep.dey/src/r-repo/automation/chromedriver"

    browser = webdriver.Chrome(executable_path=executable_path)
    browser.get(url)
    soup = BeautifulSoup(browser.page_source, 'html.parser')
    content = [x for x in soup.find_all('script') if 'abTestData' in x.text]
    cd = content[0].getText()[17:-1].replace('undefined', '"undefined"')
    home_delivery_data = json.loads(cd)

def parse_json_data(json_data):
    pass

def main():
    url = 'https://www.goodrx.com/spiriva?dosage=60-doses-of-2.5mcg-per-actuation&form=respimat-inhaler&label_override=Spiriva&quantity=1'
    json_data = get_json_data(url)
    parse_json_data(json_data)

    pass

if __name__== "__main__":
    main()





