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
import pandas as pd
import time
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_2026.utils import (
    fetch_html,
    save_file_s3,
    get_s3_client,
    logger,
    S3_BUCKET,
    S3_RAW as RAW_PREFIX
)

# Load environment variables
load_dotenv()

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing match links
fixtures_url = os.getenv("FIXTURES_URL")      # URL to fetch match fixtures



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
            i=2
            for match in match_list[2:]:
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
            logger.error("No Matches found!!")

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
    # S3 Setup
    s3_client = get_s3_client()
    
    # Process
    data = extract_fixtures(fixtures_url)
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning("Extraction returned no data. Skipping save.")
        return
    else: 
        logger.info("Uploading data to S3")
        save_file_s3(s3_client, data, S3_BUCKET, f"{RAW_PREFIX}fixtures/fixtures.json")
        save_file_s3(s3_client, df.to_csv(index=False), S3_BUCKET, f"{RAW_PREFIX}fixtures/fixtures.csv")

if __name__ == "__main__":
    main()