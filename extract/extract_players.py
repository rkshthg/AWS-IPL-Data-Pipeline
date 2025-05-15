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
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import time
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # AWS access key for S3 authentication
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # AWS secret key for S3 authentication
S3_BUCKET = os.getenv("S3_BUCKET")            # Target S3 bucket name
RAW_PREFIX = os.getenv("RAW_PREFIX")          # Prefix for RAW data storage in S3

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
        response.raise_for_status()  # Raise HTTP error if any
        return response.text
    except requests.RequestException as e:
        print(f"Failed to fetch the page: {e}")
        return None

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
    options.add_argument("--window-size=1920x1080")  # Set window size
    
    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Open Cricbuzz Squads page
    driver.get(url)
    # Allow time for page to load
    time.sleep(3)

    # Find and process team tabs
    team_tabs = driver.find_elements(By.CSS_SELECTOR, "a.cb-col-100.cb-series-brdr.cb-stats-lft-ancr")
    squad = []
    for tab in team_tabs:
        team_name = tab.text.strip()  # Extract current team name
        print(f"Extracting squad for: {team_name}")
        team_short = team_name.replace(" ", "")  # Extract team abbreviation
        
        # Click team tab and wait for content to load
        ActionChains(driver).move_to_element(tab).click().perform()
        time.sleep(2)  # Allow time for dynamic content to load

        # Get updated page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")

        # Find all player cards
        players = soup.find_all("a", class_="cb-col cb-col-50")

        # Extract player details from individual profile pages
        for player in players:
            info_link = base_url + str(player['href'])
            detail_soup = BeautifulSoup(fetch_html(info_link), "html.parser")
            
            # Extract basic information
            name = detail_soup.find("h1", class_="cb-font-40").text.strip() if detail_soup.find("h1", class_="cb-font-40") else None
            country = detail_soup.find("h3", class_="cb-font-18 text-gray").text.strip() if detail_soup.find("h3", class_="cb-font-18 text-gray") else None
            
            # Extract player role and playing styles
            role_tag = detail_soup.find("div", string="Role")
            role = role_tag.find_next_sibling("div").text.strip() if role_tag else "N/A"
            
            batting_tag = detail_soup.find("div", string="Batting Style")
            batting_style = batting_tag.find_next_sibling("div").text.strip() if batting_tag else "N/A"
            
            bowling_tag = detail_soup.find("div", string="Bowling Style")
            bowling_style = bowling_tag.find_next_sibling("div").text.strip() if bowling_tag else "N/A"

            squad.append({
                    "name": name, 
                    "team": team_name,
                    "country": country,
                    "role": role,
                    "batting_style": batting_style,
                    "bowling_style": bowling_style
                })
    save_json_s3(squad, RAW_PREFIX, "players")
            
    # Close the browser
    driver.quit()
    
    return squad


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


def main():
    """
    Main function to orchestrate the player data extraction process.
    
    This function:
    1. Initiates the player extraction process
    2. Handles the upload of extracted data to S3
    3. Manages the overall execution flow
    """
    global datafiles
    datafiles = []

    extract_players(squad_url)


if __name__ == "__main__":
    main()
