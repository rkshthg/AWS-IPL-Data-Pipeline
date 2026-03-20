"""
IPL Players Data Extractor

This module scrapes IPL cricket player information from Cricbuzz and uploads it to AWS S3.
It extracts detailed player information including name, team, country, role, and playing styles
for all teams participating in the IPL tournament.

Dependencies:
    - requests: For making HTTP requests
    - beautifulsoup4: For HTML parsing
    - selenium: For dynamic web scraping
    - boto3: For AWS S3 interactions
    - python-dotenv: For environment variable management
"""
import requests
from bs4 import BeautifulSoup
import json
import os
import time
import re
import boto3
import pandas as pd
from dotenv import load_dotenv

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
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
squad_url = os.getenv("SQUAD_URL")            # URL to fetch team squads

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
        # response.raise_for_status()  # Raise HTTP error if any
        return response.content
    except requests.RequestException as e:
        print(f"Failed to fetch the page: {e}")
        return None

def save_json_s3(data, prefix, name):
    """
    Uploads JSON data to AWS S3 bucket.

    Args:
        data (dict/list): The data to be converted to JSON and uploaded
        prefix (str): The S3 prefix (folder path) where the file will be stored
        name (str): The name of the JSON file (without extension)

    Raises:
        Exception: If there's an error uploading to S3
    """
    global datafiles  # Declare global before modifying

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    json_data = json.dumps(data, indent=4)
    filename = f"{name}.json"
    s3_key = f"{prefix}players/{filename}"

    try:
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )

        print(f"Successfully uploaded {filename} to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {filename} to S3: {e}")

def get_player(url, team):
    response = fetch_html(url)
    soup = BeautifulSoup(response, 'html.parser')
    name = soup.find('span', class_="text-xl font-bold text-[#000000DE]").text.strip()
    country = soup.find('span', class_="text-base text-gray-800").text.strip()
    info_dump = soup.find_all('div', class_="w-full bg-white flex tb:flex-col gap-4 tb:gap-px border-b wb:border-none tb:bg-inherit px-3 wb:px-4 py-1")

    details = {"Name": name, 
               "Team": team,
               "Country": country}

    for i in range(int(len(info_dump))):
        l = re.findall(r'>\s*([^<>\s][^<>]*?)\s*<', str(info_dump[i]))
        details[str(l[0])] = str(l[1])
        if str(l[0]) == 'Role':
            details['Keeper'] = True if "WK" in details['Role'] else False

    return details

def extract_players(url):
    """
    Extracts player information from the Cricbuzz Squads page.

    This function uses Selenium WebDriver to:
    1. Navigate through team tabs
    2. Extract basic player information
    3. Visit individual player pages for detailed information
    4. Compile comprehensive player profiles

    Args:
        url (str): The URL of the Cricbuzz Squads page.

    Returns:
        list: A list of dictionaries containing player information with the following keys:
            - name (str): Player's full name
            - team (str): Current IPL team
            - country (str): Player's nationality
            - role (str): Player's role (e.g., Batsman, Bowler)
            - batting_style (str): Preferred batting style
            - bowling_style (str): Preferred bowling style or "N/A"
    """
    # Configure Selenium WebDriver options
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")        # Run browser in headless mode
    options.add_argument("--disable-gpu")     # Disable GPU acceleration
    # options.add_argument("--window-size=1920x1080")  # Set window size
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    wait = WebDriverWait(driver, 10)

    all_players = []

    # Open Cricbuzz Squads page
    driver.get(url)
    
    # Allow time for page to load
    time.sleep(3)

    try:
        # Find and process team tabs
        team_elements = wait.until(EC.presence_of_all_elements_located(
                (By.XPATH, "//div[contains(@class, 'wb:hover:bg-[#d1d1d1]')]")
            ))

        for i in range(len(team_elements)):
            # Re-find elements to avoid StaleElementReferenceException
            teams = driver.find_elements(By.XPATH, "//div[contains(@class, 'wb:hover:bg-[#d1d1d1]')]")
            team_div = teams[i]
            team_name = team_div.find_element(By.TAG_NAME, "span").text
            
            print(f"Scraping: {team_name}")
            
            # 2. Click the team to load players on the right side
            driver.execute_script("arguments[0].click();", team_div)
            time.sleep(2) # Wait for the right-side panel to update

            # 3. Extract players from the right-side container
            player_cards = driver.find_elements(By.CSS_SELECTOR, ".fullscreen-container a")

            for card in player_cards:
                # Assuming the player name is within this link or a span inside it
                player_url = card.get_attribute("href")
                details = get_player(player_url, team_name)
                if details: all_players.append(details)
            
    finally:
        driver.quit()

    return all_players


def main():
    """
    Main function to orchestrate the player data extraction process.
    
    This function:
    1. Initiates the player extraction process
    2. Handles the upload of extracted data to S3
    3. Manages the overall execution flow
    """
    data = extract_players(squad_url)
    df = pd.DataFrame(data)
    df.to_csv('players.csv')
    save_json_s3(data, RAW_PREFIX, "players")


if __name__ == "__main__":
    main()