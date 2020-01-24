import requests
import urllib.request

from selenium.webdriver.support.wait import WebDriverWait

from utilities.read_write_utilities import read_list_from_file
import time
from bs4 import BeautifulSoup
from selenium import webdriver

class Scraper:
    PROXY = "127.0.0.1:24000"
    browser = None

    def __init__(self,useFirefox=False):
        self.start_scrape() if not useFirefox else self.start_scrape_firefox()
        pass

    def start_scrape(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--proxy-server=%s' % self.PROXY)
        chrome_options.add_argument('--headless')
        executable_path = "/Users/sandeep.dey/src/r-repo/automation/chromedriver"
        self.browser = webdriver.Chrome(executable_path=executable_path,chrome_options=chrome_options)

    def start_scrape_firefox(self):
        # chrome_options = webdriver.ChromeOptions()
        # chrome_options.add_argument('--proxy-server=%s' % self.PROXY)
        executable_path = "/Users/sandeep.dey/src/r-repo/automation/geckodriver"
        self.browser = webdriver.Firefox(executable_path=executable_path)

    def get_url_data_in_soup(self, url):
        self.browser.get(url)
        time.sleep(4)
        soup = BeautifulSoup(self.browser.page_source, 'html.parser')
        print(soup)
        return soup

    def stop_scrape(self):
        self.browser.quit()

