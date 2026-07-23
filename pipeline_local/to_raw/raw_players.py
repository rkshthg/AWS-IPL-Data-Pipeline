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
import pandas as pd
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pipeline_local.utils import fetch_html, save_json_local, logger

# Load environment variables
load_dotenv()

# URL Configuration

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads




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
            details["Born"] = details["Born"].replace(",", "")
            # details["Birth Place"] = details["Birth Place"].replace(",", "")
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
                        break
                    else:
                        logger.warning(f"No player data found! Skipping {name}!!")

            else: 
                logger.warning("No team data found!")
                return None
        return squads 

    except Exception as e:
        logger.error(f"Error encountered in extract_players(): \n{e}")

def main():
    """
    Main function to orchestrate the player data extraction process.
    
    This function:
    1. Initiates the player extraction process
    2. Handles the saving of extracted data to local directory
    """
    # global datafiles  # Declare global before modifying

    data = extract_players(teams_url)
    df = pd.DataFrame(data)
    if df.empty:
        logger.warning("Extraction returned no data. Skipping save.")
        return
    else: 
        os.makedirs("data/raw", exist_ok=True)
        df.to_json('data/raw/players.json', orient='records', lines=True)
        df.to_csv('data/raw/players.csv', header=True, index=False)

if __name__ == "__main__":
    main()