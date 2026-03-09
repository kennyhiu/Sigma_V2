import configparser
import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import quote
import argparse
import sys
import json
import csv
import os
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

from _api.logging_setup import setup_logging
from _jaspersoft.api import JaspersoftClient


# ---------------------------
# CLI + config
# ---------------------------

def parse_args():
    parser = argparse.ArgumentParser(description='Extract Jaspersoft jobs or reports.')
    parser.add_argument('config', help='Path to the config file (e.g., config.ini)')
    return parser.parse_args()


def read_config(config_path, logger=None):
    cfg = configparser.ConfigParser()
    read_files = cfg.read(config_path)
    if not read_files:
        if logger:
            logger.error("Could not read config file at %s", config_path)
        else:
            print(f"Error: Could not read config file at {config_path}")
        sys.exit(1)
    return cfg


def main():
    logger = setup_logging("logs")
    args = parse_args()
    cfg = read_config(args.config, logger=logger)
    logger.info("Starting Jaspersoft extractor with config: %s", args.config)

    base_url = cfg['JASPERSOFT']['base_url']
    service_username = cfg['JASPERSOFT']['username']
    service_password = cfg['JASPERSOFT']['password']

    client = JaspersoftClient(base_url, service_username, service_password)
    if not client.authenticate():
        logger.error("Authentication failed. Please check your credentials.")
        sys.exit(1)
    logger.info("Authentication successful.")
    reportlist = client.get_reports()
    logger.info("Fetched %d reports.", len(reportlist))
        

if __name__ == "__main__":
    main()
