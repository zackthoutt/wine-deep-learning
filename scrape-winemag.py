import argparse

from bs4 import BeautifulSoup
from multiprocessing.dummy import Pool
import os
import shutil
import time
import requests
import re
import json
import glob


BASE_URL = "https://www.winemag.com/?s=&drink_type=wine&pub_date_web={1}&page={0}"
session = requests.Session()
HEADERS = {
    "user-agent": (
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/48.0.2564.109 Safari/537.36"
    )
}
DATA_DIR = "data"
FILENAME = "winemag-data"

UNKNOWN_FORMAT = 0
APPELLATION_FORMAT_0 = 1
APPELLATION_FORMAT_1 = 2
APPELLATION_FORMAT_2 = 3


class Scraper:
    """Scraper for Winemag.com to collect wine reviews"""

    def __init__(
        self, pages_to_scrape=(1, 1), num_jobs=1, clear_old_data=True, year=2020
    ):
        self.pages_to_scrape = pages_to_scrape
        self.num_jobs = num_jobs
        self.clear_old_data = clear_old_data
        self.session = requests.Session()
        self.start_time = time.time()
        self.cross_process_review_count = 0
        self.page_scraped = pages_to_scrape[0]
        self.estimated_total_reviews = (
            pages_to_scrape[1] + 1 - pages_to_scrape[0]
        ) * 20
        self.year = year

        if num_jobs > 1:
            self.multiprocessing = True
            self.worker_pool = Pool(num_jobs)
        else:
            self.multiprocessing = False

    def scrape_site(self):
        if self.clear_old_data:
            self.clear_data_dir()
        if self.multiprocessing:
            link_list = [
                BASE_URL.format(page, self.year)
                for page in range(self.pages_to_scrape[0], self.pages_to_scrape[1] + 1)
            ]
            records = self.worker_pool.map(self.scrape_page, link_list)
            self.worker_pool.terminate()
            self.worker_pool.join()
        else:
            for page in range(self.pages_to_scrape[0], self.pages_to_scrape[1] + 1):
                self.scrape_page(BASE_URL.format(page, self.year))
        print("Scrape finished...")
        self.condense_data()

    def scrape_page(self, page_url, isolated_review_count=0, retry_count=0):
        scrape_data = []
        try:
            response = self.session.get(page_url, headers=HEADERS)
        except:
            retry_count += 1
            if retry_count <= 3:
                self.session = requests.Session()
                self.scrape_page(page_url, isolated_review_count, retry_count)
            else:
                raise

        soup = BeautifulSoup(response.content, "html.parser")
        reviews = soup.find_all("li", {"class": "review-item"})
        for review in reviews:
            self.cross_process_review_count += 1
            isolated_review_count += 1
            review_url = review.find("a", {"class": "review-listing"})["href"]
            try:
                review_data = self.scrape_review(review_url)
            except Exception as e:
                print("Encountered error", e)
                continue
            scrape_data.append(review_data)
        self.page_scraped += 1
        self.update_scrape_status()
        self.save_data(scrape_data)

    def scrape_review(self, review_url):
        review_response = self.session.get(review_url, headers=HEADERS)
        review_soup = BeautifulSoup(review_response.content, "html.parser")
        try:
            return self.parse_review(review_soup)
        except ReviewFormatException as e:
            print(f"\n-----\nError parsing: {review_url}\n{e.message}\n-----")

    def parse_review(self, review_soup):
        review_format = self.determine_review_format(review_soup)
        points = review_soup.find("span", {"id": "points"}).contents[0]
        title = review_soup.title.string.split(" Rating")[0]
        description = review_soup.find("p", {"class": "description"}).contents[0]

        info_containers = review_soup.find("ul", {"class": "primary-info"}).find_all(
            "li", {"class": "row"}
        )

        if review_format["price_index"] is not None:
            try:
                price_string = (
                    info_containers[review_format["price_index"]]
                    .find("div", {"class": "info"})
                    .span.span.contents[0]
                    .split(",")[0]
                )
            except:
                raise ReviewFormatException("Unexpected price format")
            # Sometimes price is N/A
            try:
                price = int(re.sub("[$]", "", price_string))
            except ValueError:
                price = None
        else:
            price = None

        if review_format["designation_index"] is not None:
            try:
                designation = (
                    info_containers[review_format["designation_index"]]
                    .find("div", {"class": "info"})
                    .span.span.contents[0]
                )
            except:
                raise ReviewFormatException("Unexpected designation format")
        else:
            designation = None

        if review_format["variety_index"] is not None:
            try:
                variety = (
                    info_containers[review_format["variety_index"]]
                    .find("div", {"class": "info"})
                    .span.findChildren()[0]
                    .contents[0]
                )
            except:
                raise ReviewFormatException("Unexpected variety format")
        else:
            variety = None

        if review_format["appellation_index"] is not None:
            appellation_info = (
                info_containers[review_format["appellation_index"]]
                .find("div", {"class": "info"})
                .span.findChildren()
            )
            try:
                if review_format["appellation_format"] == APPELLATION_FORMAT_0:
                    region_1 = None
                    region_2 = None
                    province = appellation_info[0].contents[0]
                    country = appellation_info[1].contents[0]
                elif review_format["appellation_format"] == APPELLATION_FORMAT_1:
                    region_1 = appellation_info[0].contents[0]
                    region_2 = None
                    province = appellation_info[1].contents[0]
                    country = appellation_info[2].contents[0]
                elif review_format["appellation_format"] == APPELLATION_FORMAT_2:
                    region_1 = appellation_info[0].contents[0]
                    region_2 = appellation_info[1].contents[0]
                    province = appellation_info[2].contents[0]
                    country = appellation_info[3].contents[0]
                else:
                    region_1 = None
                    region_2 = None
                    province = None
                    country = None
            except:
                raise ReviewFormatException("Unknown appellation format")
        else:
            region_1 = None
            region_2 = None
            province = None
            country = None

        if review_format["winery_index"] is not None:
            try:
                winery = (
                    info_containers[review_format["winery_index"]]
                    .find("div", {"class": "info"})
                    .span.span.findChildren()[0]
                    .contents[0]
                )
            except:
                raise ReviewFormatException("Unexpected winery format")
        else:
            winery = None

        taster_url = review_soup.find("span", {"class": "taster-area"}).contents[0][
            "href"
        ]
        try:
            taster_name, taster_twitter_handle, taster_photo = self.scrape_taster(
                taster_url
            )
        except Exception as e:
            print("Encountered error", e)

        review_data = {
            "points": points,
            "title": title,
            "description": description,
            "taster_name": taster_name,
            "taster_twitter_handle": taster_twitter_handle,
            "taster_photo": taster_photo,
            "price": price,
            "designation": designation,
            "variety": variety,
            "region_1": region_1,
            "region_2": region_2,
            "province": province,
            "country": country,
            "winery": winery,
        }

        return review_data

    def scrape_taster(self, taster_url):
        taster_response = self.session.get(taster_url, headers=HEADERS)
        taster_soup = BeautifulSoup(taster_response.content, "html.parser")
        try:
            return self.parse_taster(taster_soup)
        except ReviewFormatException as e:
            print(f"\n-----\nError parsing: {taster_url}\n{e.message}\n-----")

    def parse_taster(self, taster_soup):
        name = taster_soup.title.string.split(" |")[0]
        twitter = taster_soup.find("li", {"class": "twitter"}).string
        photo = taster_soup.find("div", {"class": "contrib__photo"}).img["src"]
        return name, twitter, photo

    def determine_review_format(self, review_soup):
        review_format = {}
        info_containers = review_soup.find("ul", {"class": "primary-info"}).find_all(
            "li", {"class": "row"}
        )

        review_info = []
        for container in info_containers:
            review_info.append(str(container.find("span").contents[0]).lower())

        try:
            review_format["price_index"] = review_info.index("price")
        except ValueError:
            review_format["price_index"] = None
        try:
            review_format["designation_index"] = review_info.index("designation")
        except ValueError:
            review_format["designation_index"] = None
        try:
            review_format["variety_index"] = review_info.index("variety")
        except ValueError:
            review_format["variety_index"] = None
        try:
            review_format["appellation_index"] = review_info.index("appellation")
        except ValueError:
            review_format["appellation_index"] = None
        try:
            review_format["winery_index"] = review_info.index("winery")
        except ValueError:
            review_format["winery_index"] = None

        # The appellation format changes based on where in the world the winery is located
        if review_format["appellation_index"] is not None:
            appellation_info = (
                info_containers[review_format["appellation_index"]]
                .find("div", {"class": "info"})
                .span.findChildren()
            )
            if len(appellation_info) == 2:
                review_format["appellation_format"] = APPELLATION_FORMAT_0
            elif len(appellation_info) == 3:
                review_format["appellation_format"] = APPELLATION_FORMAT_1
            elif len(appellation_info) == 4:
                review_format["appellation_format"] = APPELLATION_FORMAT_2
            else:
                review_format["appellation_format"] = UNKNOWN_FORMAT

        return review_format

    def save_data(self, data):
        filename = "{}/{}_{}.json".format(DATA_DIR, FILENAME, time.time())
        try:
            os.makedirs(DATA_DIR)
        except OSError:
            pass
        with open(filename, "w") as fout:
            json.dump(data, fout)

    def clear_all_data(self):
        self.clear_data_dir()
        self.clear_output_data()

    def clear_data_dir(self):
        try:
            shutil.rmtree(DATA_DIR)
        except FileNotFoundError:
            pass

    def clear_output_data(self):
        try:
            os.remove("{}.json".format(FILENAME))
        except FileNotFoundError:
            pass

    def condense_data(self):
        print("Condensing Data...")
        condensed_data = []
        all_files = glob.glob("{}/*.json".format(DATA_DIR))
        for file in all_files:
            with open(file, "rb") as fin:
                condensed_data += json.load(fin)
        print(len(condensed_data))
        filename = "{}.json".format(FILENAME)
        with open(filename, "w") as fout:
            json.dump(condensed_data, fout)

    def update_scrape_status(self):
        elapsed_time = round(time.time() - self.start_time, 2)
        time_remaining = round(
            (self.estimated_total_reviews - self.cross_process_review_count)
            * (self.cross_process_review_count / elapsed_time),
            2,
        )
        print(
            "{4} page {0}/{1} reviews | {2} sec elapsed | {3} sec remaining\r".format(
                self.cross_process_review_count,
                self.estimated_total_reviews,
                elapsed_time,
                time_remaining,
                self.page_scraped,
            )
        )


class ReviewFormatException(Exception):
    """Exception when the format of a review page is not understood by the scraper"""

    def __init__(self, message):
        self.message = message
        super(Exception, self).__init__(message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pages", nargs=2, type=int, help="pages range to scrape")
    parser.add_argument("year", type=int, help="year filter")
    parser.add_argument("clear", type=bool, help="Clear old data")

    args = parser.parse_args()
    # Total review results on their site are conflicting, hardcode as the max tested value for now
    winmag_scraper = Scraper(
        pages_to_scrape=args.pages,
        num_jobs=10,
        clear_old_data=args.clear,
        year=args.year,
    )

    # Step 1: scrape data
    winmag_scraper.scrape_site()

    # Step 2: condense data
    winmag_scraper.condense_data()
