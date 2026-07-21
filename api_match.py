import requests
from bs4 import BeautifulSoup
import json
import os
import boto3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv
import time

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s]\n%(filename)s:%(lineno)d --- %(message)s\n")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # AWS access key for S3 authentication
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # AWS secret key for S3 authentication
S3_RAW = os.getenv("S3_RAW")                  # Target S3 bucket name
DATA_PREFIX = os.getenv("DATA_PREFIX")
MATCH_PREFIX = os.getenv("MATCH_PREFIX")

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads


def read_json_s3(client, prefix, name):
    try:
        key = f"{DATA_PREFIX}{prefix}{name}"
        logger.info(f"Loading data from {name}")

        response = client.get_object(Bucket=S3_RAW, Key=key)

        if response: logger.info(f"Successfully read from S3 file: {name}")

        return json.loads(response['Body'].read().decode('utf-8'))
    
    except Exception as e:
        logger.error(f"Error reading '{name}' from S3: {e}")
        return None

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.content
    except requests.RequestException as e:
        logger.error(f"HTTP Error: {e}")
        return None
    
def fetch_webpage(url):
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--headless")  # Run in headless mode (no UI)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get(url)
        # Wait for the page to load
        wait = WebDriverWait(driver, 10)

        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        while True:
            try:
                # Check if the "Load More Commentary" button exists and is visible
                load_more_btn = wait.until(EC.presence_of_element_located((By.ID, "full_commentary_btn")))
                if load_more_btn.is_displayed():
                    driver.execute_script("arguments[0].click();", load_more_btn)  # Click the button
                    time.sleep(2)  # Wait for new content to load
                else:
                    break  # Exit if button is not found
            except Exception as e:
                break
        html = driver.page_source
    
    except Exception as e:
        logger.error(f"Error during Selenium fetch: {e}")
        # None  # Ignore any exceptions
    
    finally:
        driver.quit() # Close the browser

    return html

def get_live_data(match_id, url):

    pages = 0
    balls = []

    while True:
        api_url = f"https://www.cricbuzz.com/api/cricket-match/v1/{match_id}/commentary"

        headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'content-type': 'application/json',
            'priority': 'u=1, i',
            'referer': url,
            'sec-ch-ua': '"Google Chrome";v="147", "Not.A/Brand";v="8", "Chromium";v="147"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
        }

        api_url = f"https://www.cricbuzz.com/api/mcenter/commentary-pagination/{match_id}/2/{page_id}"

        response = requests.get(api_url, headers=headers)
        data = response.json()

def main():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

        now = datetime.now()
        date = str(datetime.strftime(now, "%B %d"))

        fixtures_df = pd.read_json("/home/rakshith/Data/IPL-2026/data/fixtures.json")

        match_rows = fixtures_df[fixtures_df['date'] < date]

        for i in range(len(match_rows)):
            match_row = match_rows.loc[i].to_dict()
            print(type(match_row))
            match = match_row['match_short']
            match_url = match_row['link']

            metadata_file = f"{match}_meta.json"
            MATCH_PREFIX = f"match/{match}/"
            metadata_key = f"{MATCH_PREFIX}{metadata_file}"

            if os.path.isdir(f"data/{match}"):
                logger.info("Directory exists.")
                meta_exists = True
            else:
                logger.info("Directory does not exist.")
                os.makedirs(f"data/{match}", mode=0o777, exist_ok=False)
                meta_exists = False
           
            if meta_exists:
                logger.info(f"Metadata file '{metadata_file}' exists.")
                # metadata = read_json_s3(s3_client, MATCH_PREFIX, metadata_file)
                logger.info(f"Reading metadata...")
                with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_meta.json", "r") as f:
                    metadata = json.load(f)

                logger.info("Extracting Ball event data..")

                # print(match_url)

                # match_id = match_url.split("/")[4]
                # print(match_id)

                webpage = fetch_webpage(match_url)
                


                

    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()