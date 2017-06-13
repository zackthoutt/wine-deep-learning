from bs4 import BeautifulSoup
import requests
import re
import numpy as np


BASE_URL = 'http://www.winemag.com/?s=&drink_type=wine&page={0}'
session = requests.Session()
HEADERS = {
    'user-agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36')
}


class Scraper():
    """Scraper for Winemag.com to collect wine reviews"""

    def __init__(self):
        self.session = requests.Session()

    def scrape_site(self):
        response = self.session.get(BASE_URL.format(1), headers=HEADERS)


if __name__ == '__main__':
    winmag_scraper = Scraper()
    winmag_scraper.scrape_site()
