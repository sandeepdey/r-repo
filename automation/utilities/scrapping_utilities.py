import random
from utilities.logging_setup import logger
import requests
import urllib.request
import json

from requests import Session
from selenium.webdriver.support.wait import WebDriverWait
import time
from bs4 import BeautifulSoup
from selenium import webdriver


class Scraper:
    PROXY = "127.0.0.1:24000"
    browser = None

    def __init__(self, useFirefox=False):
        self.start_scrape() if not useFirefox else self.start_scrape_firefox()
        pass

    def start_scrape(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--proxy-server=%s' % self.PROXY)
        chrome_options.add_argument('--headless')
        executable_path = "/Users/sandeep.dey/src/r-repo/automation/chromedriver"
        self.browser = webdriver.Chrome(executable_path=executable_path, chrome_options=chrome_options)

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


class ScraperRequest:
    _cookie_jar = requests.cookies.RequestsCookieJar()
    _session = Session()
    _requests_made = 0
    _headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
    }
    _proxies = None

    def get_proxy_url(self):
        # dns-remote helps us resolve dns at the proxy and not at the super proxy
        # it's slower but it should give us similar ip as the proxy
        username = 'lum-customer-blink_health-zone-apiscraper-route_err-pass_dyn-dns-remote'
        proxy_password = '9l39ljqfuxzh'
        port = 22225
        session_id = random.random()
        super_proxy_url = ('http://%s-country-us-session-%s:%s@zproxy.lum-superproxy.io:%d' %
                           (username, session_id, proxy_password, port))
        return {
            'http': super_proxy_url,
            'https': super_proxy_url,
        }

    def __init__(self, base_url, extra_headers=None, extra_cookies=None):
        logger.info('Initiating Scraper')
        if extra_headers is not None:
            self._headers.update(extra_headers)
        if extra_cookies is not None:
            self._cookie_jar.update(extra_cookies)
        logger.info(str(self._headers))
        self._get_response(base_url)
        self._session.headers.update(self._headers)
        return

    def _get_response(self, url):
        logger.info(str(self._cookie_jar.items()))
        self._requests_made += 1
        if self._proxies is None or self._requests_made % 5 == 0:
            self._proxies = self.get_proxy_url()
            time.sleep(1)
            logger.info('Setting Up Proxy')
        response = self._session.get(url, cookies=self._cookie_jar, headers=self._headers, proxies=self._proxies)
        logger.info('Url Response : %s : %s ' % (response.status_code, url))
        return response

    def get_parsed_html(self, url):
        response = self._get_response(url)
        return BeautifulSoup(response.text, 'html.parser')

    def get_parsed_json(self, url):
        response = self._get_response(url)
        logger.info(str(response.text))
        return json.loads(response.text)
