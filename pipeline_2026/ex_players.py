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
import re
import boto3
import pandas as pd
import time
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads

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
        return response.content
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
    s3_key = f"{prefix}players/{filename}"

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

def get_player(url, team):
    try:
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

    except Exception as e:
        logger.error(f"Error encountered in get_player(): \n{e}")

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
    try:
        response = fetch_html(url)
        soup = BeautifulSoup(response, "html.parser")
        
        team_cards = soup.find_all('div', class_="flex flex-col gap-px tb:grid tb:grid-cols-2 ml-[-14px]")

        squads = []

        for team_card in team_cards:
            teams = team_card.find_all('a', href=True)
            if teams:
                for team in teams[:10]:
                    team_url = base_url + team['href'] + "/players"
                    team_name = team_url.split("/")[-3].split("-")
                    name = "" + " ".join(word.capitalize() for word in team_name)

                    logger.info(f"Extracting player data for {name}...")

                    response_2 = fetch_html(team_url)
                    soup_2 = BeautifulSoup(response_2, "html.parser")

                    player_cards = soup_2.find_all('div', class_="flex flex-col gap-px tb:grid tb:grid-cols-2")
                    if player_cards:
                        for player_card in player_cards:
                            players = player_card.find_all('a', href=True)
                            for player in players:
                                player_url = base_url + player['href']
                                
                                squads.append(get_player(player_url, name))

                        logger.info(f"Completed player data extraction for {name}.")
                    else:
                        logger.warning(f"No player data found! Skipping {name}!!")

            else: 
                logger.warning("No team data found!")
                return None

    except Exception as e:
        logger.error(f"Error encountered in extract_players(): \n{e}")

    return squads 

def main():
    """
    Main function to orchestrate the player data extraction process.
    
    This function:
    1. Initiates the player extraction process
    2. Handles the upload of extracted data to S3
    3. Manages the overall execution flow
    """
        # global datafiles  # Declare global before modifying

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    data = extract_players(teams_url)
    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("Extraction returned no data. Skipping S3 upload.")
        return
    else: 
        df.to_csv('data/players.csv')
        save_json_s3(s3_client, data, RAW_PREFIX, "players")


if __name__ == "__main__":
    main()