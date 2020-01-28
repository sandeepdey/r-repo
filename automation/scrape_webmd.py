from utilities.scrapping_utilities import ScraperRequest
from os import path

from http import cookies
from utilities.logging_setup import logger
import time
import uuid
import datetime
import string
import json
from utilities.read_write_utilities import read_csv
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




# def parse_urls_from_index(soup):
#     urls = soup.find_all(id='desktop-all-drugs-container')[0]
#     return [k.get('href')[1:] for k in urls.find_all('a')]

def main():
    extra_headers = {
        'client_id': 'e4e3f73a-0ceb-4d37-939e-90ddb1238360',
        'Accept': 'application/json',
        'DNT': '1',
        'enc_data': 'rVXhR/l0GMCjq+aJJ/l0wOcesWjLwV6yFFXc6JqW46c=',
        'timestamp': 'Mon, 27 Jan 2020 20:36:06 GMT'
    }
    sc = ScraperRequest(base_url='https://www.webmd.com/',extra_headers=extra_headers)
    input_file = './data/wedmdrx_mapping.csv'
    meds = read_csv(input_file)
    for i in meds:
        ndc = i['ndc']
        output_file = './data/webmdrx_json_data/%s'%ndc
        url = 'https://www.webmd.com/search/2/api/rx/forms/v3/%s?app_id=web'%ndc
        if path.exists(output_file):
            continue
        data = sc.get_parsed_json(url)
        print(data)
        with open(output_file, "w") as write_file:
            json.dump(data, write_file)

#curl --location --request GET 'https://www.webmd.com/search/2/api/rx/forms/v3/71800015631?app_id=web' \
#--header 'Accept: application/json' \
#--header 'timestamp: Mon, 27 Jan 2020 20:36:06 GMT' \
#--header 'DNT: 1' \
#--header 'enc_data: rVXhR/l0GMCjq+aJJ/l0wOcesWjLwV6yFFXc6JqW46c=' \
#--header 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36' \
#--header 'client_id: e4e3f73a-0ceb-4d37-939e-90ddb1238360'

# def parse_map_webmdrx_slugs_ndc():
#     input_file = "/Users/sandeep.dey/Downloads/webmdrx_sitemap/wedmdrx_drugs.csv"
#     output_file = '/Users/sandeep.dey/Downloads/webmdrx_sitemap/wedmdrx_mapping.csv'
#     list_of_webmd_slugs = read_list_from_file(input_file)
#     logger.info('%d Slugs In Input'%len(list_of_webmd_slugs))
#     current_slugs = read_csv(output_file)
#     # webmd_slugs_already_done = [ x['webmd_slug'] for x in current_slugs] if current_slugs is not None else []
#     # logger.info('%d Slugs already present'%len(webmd_slugs_already_done))
#
#     sr = Scraper_Request()
#     output_data = []
#
#     for webmd_slug in list_of_webmd_slugs:
#         try:
#             # if webmd_slug in webmd_slugs_already_done :
#             #     logger.info('Skipping Slug %s' % webmd_slug)
#             #     continue
#             logger.info('Processing Slug %s'%webmd_slug)
#             soup = sr.get_parsed_html('https://www.webmd.com/rx/drug-prices/%s'%webmd_slug)
#             content = [x for x in soup.find_all('script') if '__INITIAL_STATE__' in x.text and 'pagedata' in x.text]
#             json_data = json.loads(content[0].getText()[25:].split(';')[0])
#             output_dict = {
#                 'ndc' :json_data['drugInfo']['ndc'],
#                 'drugname' : json_data['drugInfo']['drugName'],
#                 'fdb' : json_data['drugInfo']['fdb'],
#                 'webmd_slug': webmd_slug
#             }
#             output_data.append(output_dict)
#         except:
#             logger.error('Data Error , Skipping')
#         time.sleep(1)
#     write_list_to_csv(output_file,output_data)




    # json_data = sr.get_parsed_json('http://lumtest.com/myip.json')
    # print(json_data)

    # for character in string.ascii_lowercase[0:3]:
    #     sc = Scraper(useFirefox=False)
    #     url = 'https://www.goodrx.com/drugs/%c'%character
    #     print(url)
    #     soup = sc.get_url_data_in_soup(url)
    #     drugs = parse_urls_from_index(soup)
    #     print(drugs)
    #     write_set_to_txt('goodrx_drug_directory.txt',drugs,append=True)
    #     sc.stop_scrape()

if __name__== "__main__":
    main()





