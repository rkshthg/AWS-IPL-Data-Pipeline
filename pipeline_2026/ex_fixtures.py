"""
IPL Match Fixtures Extractor

This module scrapes IPL cricket match fixtures from a specified website and uploads them to AWS S3.
It extracts match details including team names, venues, dates, and match live-commentary links, storing them in JSON format.

Dependencies:
    - requests: For making HTTP requests
    - beautifulsoup4: For HTML parsing
    - boto3: For AWS S3 interactions
    - python-dotenv: For environment variable management
"""
import requests
from bs4 import BeautifulSoup
import json
import os
import re
import boto3
import pandas as pd
import time
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # AWS access key for S3 authentication
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # AWS secret key for S3 authentication
S3_BUCKET = os.getenv("S3_BUCKET")            # Target S3 bucket name
RAW_PREFIX = os.getenv("RAW_PREFIX")          # Prefix for RAW data storage in S3
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")    # Prefix for BRONZE data storage in S3

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing match links
fixtures_url = os.getenv("FIXTURES_URL")      # URL to fetch match fixtures

def fetch_html(url):
    """
    Fetches the HTML content from a given URL.

    Args:
        url (str): The URL to fetch the HTML content from.

    Returns:
        str or None: The HTML content if successful, None if the request fails.

    Raises:
        requests.RequestException: If there's an error fetching the page.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.text
    except requests.RequestException as e:
        logger.error(f"HTTP Error: {e}")
        return None
    
def save_json_s3(client, data, prefix, name):
    """
    Uploads JSON data to AWS S3 bucket.

    Args:
        data (dict/list): The data to be converted to JSON and uploaded
        prefix (str): The S3 prefix (folder path) where the file will be stored
        name (str): The name of the JSON file (without extension)

    Raises:
        Exception: If there's an error uploading to S3
    """
    json_data = json.dumps(data, indent=4)
    filename = f"{name}.json"
    s3_key = f"{prefix}fixtures/{filename}"

    try:
        client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded {filename} to s3://{S3_BUCKET}/{s3_key}")

    except Exception as e:
        logger.error(f"Failed to upload {filename} to S3: \n{e}")

def extract_fixtures(url):
    """
    Extracts cricket match fixtures from the specified URL.

    Args:
        url (str): The URL containing match fixtures.

    Returns:
        list: A list of dictionaries containing match details with the following keys:
            - match_name (str): Full name of the match
            - short_name (str): Short identifier for the match
            - date (str): Match date and time in 'YYYY-MM-DD HH:MM:SS' format
            - team1 (str): Name of the first team
            - team2 (str): Name of the second team
            - venue (str): Match venue
            - link (str): URL to the match details
    """
    # Configure Selenium WebDriver options
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")        # Run browser in headless mode
    options.add_argument("--disable-gpu")     # Disable GPU acceleration
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    # wait = WebDriverWait(driver, 10)

    matches = []

    # Open Cricbuzz Matches page
    driver.get(url)
    # Allow time for page to load
    time.sleep(3)

    try:
        match_list = driver.find_elements(By.XPATH, "//a[contains(@class, 'w-full bg-cbWhite flex flex-col p-3 gap-1')]")
        if match_list:
            i=0
            for match in match_list:
                match_details = {}
                match_info_card = match.text
                match_info = match_info_card.replace(" • ", ",").replace("\n", ",").replace(", Mullanpur", "").split(",")

                i += 1
                logger.info(f"Extracting data for Match {i}...")

                match_details['match'] = match_info[0].strip()
                match_details['location'] = match_info[1].strip()
                match_details['stadium'] = match_info[2].strip()

                match_details['home_team'] = match_info[4].strip()
                match_details['away_team'] = match_info[5].strip()
                
                m_no = re.findall(r'[0-9]', match_details['match'])
                t1 = re.findall(r'[A-Z]', match_details['home_team'])
                t2 = re.findall(r'[A-Z]', match_details['away_team'])
                if len(m_no) > 1:
                    match_details['match_short'] = "".join(m_no) + '_' + "".join(t1) + 'vs' + "".join(t2)
                else: match_details['match_short'] = "0" + "".join(m_no) + '_' + "".join(t1) + 'vs' + "".join(t2)

                match_details['dayofweek'] = match_info[6].strip()
                match_details['date'] = match_info[7].strip()
                match_details['time'] = match_info[8].split(' ')[0].strip()

                match_reference = driver.find_element(By.CSS_SELECTOR, f"a[title*='{match_details['home_team']} vs {match_details['away_team']}']")
                match_details['link'] = match_reference.get_attribute("href")

                logger.info(f"Completed data extraction for Match {i}!")
                matches.append(match_details)

        else:
            logger.error("No Mqatches found!!")

    except Exception as e:
        logger.error(f"Error encountered in extract_fixtures(): \n{e}")

    finally:
        driver.quit()
    
    return matches

def main():
    """
    Main function to orchestrate the player data extraction process.
    
    This function:
    1. Initiates the player extraction process
    2. Handles the upload of extracted data to S3
    3. Manages the overall execution flow
    """
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    data = extract_fixtures(fixtures_url)
    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("Extraction returned no data. Skipping S3 upload.")
        return
    else: 
        df.to_csv('data/fixtures.csv')
        save_json_s3(s3_client, data, RAW_PREFIX, "fixtures")

if __name__ == "__main__":
    main()