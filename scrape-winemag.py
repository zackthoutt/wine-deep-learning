from bs4 import BeautifulSoup
import sys
import time
import requests
import re
import json
import numpy as np


BASE_URL = 'http://www.winemag.com/?s=&drink_type=wine&page={0}'
session = requests.Session()
HEADERS = {
    'user-agent': ('Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36')
}

UNKNOWN_FORMAT = 0
APPELLATION_FORMAT_1 = 1
APPELLATION_FORMAT_2 = 2


class Scraper():
    """Scraper for Winemag.com to collect wine reviews"""

    def __init__(self):
        self.session = requests.Session()
        self.data = []
        self.appellation_format = UNKNOWN_FORMAT
        self.start_time = 0
        self.current_review = 0

    def scrape_site(self, num_pages_to_scrape):
        self.start_time = time.time()
        # There are up to 30 reviews per page
        self.estimated_total_reviews = 7170 * 30
        for page in range(1, num_pages_to_scrape):
            self.scrape_page(page)
        self.save_data()

    def scrape_page(self, page):
        response = self.session.get(BASE_URL.format(page), headers=HEADERS)
        soup = BeautifulSoup(response.content, 'html.parser')
        # Drop the first review-item; it's always empty
        reviews = soup.find_all('li', {'class': 'review-item'})[1:]
        for review in reviews:
            self.current_review += 1
            self.scrape_review(review)
            self.update_scrape_status()

    def scrape_review(self, review):
        review_url = review.find('a', {'class': 'review-listing'})['href']
        review_response = self.session.get(review_url, headers=HEADERS)
        review_soup = BeautifulSoup(review_response.content, 'html.parser')
        self.determine_review_format(review_soup)
        try:
            self.parse_review(review_soup)
        except ReviewFormatException as e:
            print('\n-----\nError parsing: {}\n{}\n-----'.format(
                review_url,
                e.message
            ))

    def parse_review(self, review_soup):
        points = review_soup.find("span", {"id": "points"}).contents[0]
        description = review_soup.find("p", {"class": "description"}).contents[0]

        info_containers = review_soup.find(
            'ul', {'class': 'primary-info'}).find_all('li', {'class': 'row'})

        if self.price_index is not None:
            try:
                price_string = info_containers[self.price_index].find(
                    'div', {'class': 'info'}).span.span.contents[0].split(',')[0]
            except:
                raise ReviewFormatException('Unexpected price format')
            # Sometimes price is N/A
            try:
                price = int(re.sub('[$]', '', price_string))
            except ValueError:
                price = None
        else:
            price = None

        if self.designation_index is not None:
            try:
                designation = info_containers[self.designation_index].find('div', {'class': 'info'}).span.span.contents[0]
            except:
                raise ReviewFormatException('Unexpected designation format')
        else:
            designation = None

        if self.variety_index is not None:
            try:
                variety = info_containers[self.variety_index].find(
                    'div', {'class': 'info'}).span.findChildren()[0].contents[0]
            except:
                raise ReviewFormatException('Unexpected variety format')
        else:
            variety = None

        if self.appellation_index is not None:
            appellation_info = info_containers[self.appellation_index].find('div', {'class': 'info'}).span.findChildren()
            if self.appellation_format == APPELLATION_FORMAT_1:
                region_1 = appellation_info[0].contents[0]
                region_2 = None
                province = appellation_info[1].contents[0]
                country = appellation_info[1].contents[0]
            elif self.appellation_format == APPELLATION_FORMAT_2:
                region_1 = appellation_info[0].contents[0]
                region_2 = appellation_info[1].contents[0]
                province = appellation_info[2].contents[0]
                country = appellation_info[3].contents[0]
            else:
                raise ReviewFormatException('Unknown appellation format')
        else:
            region_1 = None
            region_2 = None
            province = None
            country = None

        if self.winery_index is not None:
            try:
                winery = info_containers[self.winery_index].find(
                    'div', {'class': 'info'}).span.span.findChildren()[0].contents[0]
            except:
                raise ReviewFormatException('Unexpected winery format')
        else:
            winery = None

        review_data = {
            'points': points,
            'description': description,
            'price': price,
            'designation': designation,
            'variety': variety,
            'region_1': region_1,
            'region_2': region_2,
            'province': province,
            'country': country,
            'winery': winery
        }
        self.data.append(review_data)

    def determine_review_format(self, review_soup):
        info_containers = review_soup.find(
            'ul', {'class': 'primary-info'}).find_all('li', {'class': 'row'})

        review_info = []
        for container in info_containers:
            review_info.append(str(container.find('span').contents[0]).lower())

        try:
            self.price_index = review_info.index('price')
        except ValueError:
            self.price_index = None
        try:
            self.designation_index = review_info.index('designation')
        except ValueError:
            self.designation_index = None
        try:
            self.variety_index = review_info.index('variety')
        except ValueError:
            self.variety_index = None
        try:
            self.appellation_index = review_info.index('appellation')
        except ValueError:
            self.appellation_index = None
        try:
            self.winery_index = review_info.index('winery')
        except ValueError:
            self.winery_index = None

        # The appellation format is changes based on where in the world the winery is located
        if self.appellation_index is not None:
            appellation_info = info_containers[self.appellation_index].find('div', {'class': 'info'}).span.findChildren()
            if len(appellation_info) == 3:
                self.appellation_format = APPELLATION_FORMAT_1
            elif len(appellation_info) == 4:
                self.appellation_format = APPELLATION_FORMAT_2
            else:
                self.appellation_format = UNKNOWN_FORMAT

    def save_data(self):
        with open('winmag-reviews.json', 'w') as fout:
            json.dump(data, fout)

    def update_scrape_status(self):
        elapsed_time = round(time.time() - self.start_time, 2)
        print('{0}/{1} reviews | {2} sec elapsed\r'.format(
            self.current_review, self.estimated_total_reviews, elapsed_time), end='')


class ReviewFormatException(Exception):
    """Exception when the format of a review page is not understood by the scraper"""
    def __init__(self, message):
        self.message = message
        super(Exception, self).__init__(message)


if __name__ == '__main__':
    winmag_scraper = Scraper()
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    num_pages_to_scrape = 7071
    winmag_scraper.scrape_site(num_pages_to_scrape)
