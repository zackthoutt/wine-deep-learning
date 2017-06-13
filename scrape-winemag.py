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

    def scrape_site(self, num_pages_to_scrape):
        for page in range(1, num_pages_to_scrape):
            self.scrape_page(page)

    def scrape_page(self, page):
        response = self.session.get(BASE_URL.format(page), headers=HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')
        # Drop the first review-item; it's always empty
        reviews = soup.find_all('li', {'class': 'review-item'})[1:]


if __name__ == '__main__':
    winmag_scraper = Scraper()
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    num_pages_to_scrape = 7071
    winmag_scraper.scrape_site(num_pages_to_scrape)
