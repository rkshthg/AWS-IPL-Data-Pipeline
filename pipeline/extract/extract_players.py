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

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = os.getenv("RAW_PREFIX")

base_url = os.getenv("BASE_URL")
squad_url = os.getenv("SQUAD_URL")

def fetch_html(url):
    """Fetches the HTML content of a given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.text
    except requests.RequestException as e:
        print(f"Failed to fetch the page: {e}")
        return None

def extract_players(url):
    """
    Extracts player information from the given Cricbuzz Squads page URL.

    Args:
        url (str): The URL of the Cricbuzz Squads page.

    Returns:
        list: A list of dictionaries containing player information.
    """
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (no UI)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")
    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # Open Cricbuzz Squads page
    driver.get(url)
    # Allow time for page to load
    time.sleep(3)

    # Find all team tabs (buttons)
    team_tabs = driver.find_elements(By.CSS_SELECTOR, "a.cb-col-100.cb-series-brdr.cb-stats-lft-ancr")
    squad = []
    # Iterate over each team tab
    for tab in team_tabs:
        team_name = tab.text.strip()  # Extract team name
        print(f"Extracting squad for: {team_name}")
        # Click on the team tab to load its squad
        ActionChains(driver).move_to_element(tab).click().perform()
        time.sleep(2)  # Wait for data to load
        # Get updated page source and parse with BeautifulSoup
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Find all player cards
        players = soup.find_all("a", class_="cb-col cb-col-50")
        # Iterate over each team tab
        for player in players:
            info_link = base_url + str(player['href'])
            detail_soup = BeautifulSoup(fetch_html(info_link), "html.parser")
            # Extract player names
            name = detail_soup.find("h1", class_="cb-font-40").text.strip() if detail_soup.find("h1", class_="cb-font-40") else None
            country = detail_soup.find("h3", class_="cb-font-18 text-gray").text.strip() if detail_soup.find("h3", class_="cb-font-18 text-gray") else None
            # Extract player role
            role_tag = detail_soup.find("div", string="Role")
            role = role_tag.find_next_sibling("div").text.strip() if role_tag else "N/A"
            # Extract batting style
            batting_tag = detail_soup.find("div", string="Batting Style")
            batting_style = batting_tag.find_next_sibling("div").text.strip() if batting_tag else "N/A"
            # Extract bowling style (optional)
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
    # Close the browser
    driver.quit()
    return squad

def save_json_s3(data, prefix, name):
    """Uploads JSON data to S3."""
    global datafiles  # Declare global before modifying

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )

    json_data = json.dumps(data, indent=4)
    filename = f"{name}.json"
    s3_key = f"{prefix}{filename}"

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
    """Main function to orchestrate the data extraction and S3 upload."""
    global datafiles
    datafiles = []

    squad = extract_players(squad_url)
    if squad:
        save_json_s3(squad, RAW_PREFIX, "squads")

if __name__ == "__main__":
    main()
