import configparser
import os
import json
import time
import shutil
import logging
import uuid
import time

from pathlib import Path

from api.scraping.scrapers import CADORSPageScrapper, CADORSQueryScrapper
from api.scraping.utils import Utils

URL_SCRAPING_FINISHED = False

logging_params = {
    "level": logging.INFO,
    "format": "%(asctime)s__[%(levelname)s, %(module)s.%(funcName)s](%(name)s)__[L%(lineno)d] %(message)s",
}


def scrape_urls(scraping_config):

    try:
        logging.info("Starting to scrape the occurance links of all CADORS incidents")

        # getting occurances
        query_scrapper = CADORSQueryScrapper(scraping_config)
        query_scrapper.scrape_occurances()

        logging.info("Completed scrapping occurance urls.")

    except Exception as e:
        logging.error(e)


def lambda_handler():
    config = configparser.ConfigParser()
    config.read("config.ini")
    scraping_config = dict(config.items("scraping"))

    logging.basicConfig(filename="cadorsqueryscraper.log", **logging_params)
    logging.error("Logging Init")  # Debug
    scrape_urls(scraping_config)


if __name__ == "__main__":
    lambda_handler()
