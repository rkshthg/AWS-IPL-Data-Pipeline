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
import json
import os
import datetime
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv

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
        print(f"Failed to fetch the page: {e}")
        return None

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
    html = fetch_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    match_classes = [
        "cb-col-100 cb-col cb-series-brdr cb-series-matches",
        "cb-col-100 cb-col cb-series-matches"
    ]

    match_cards = []
    for match_class in match_classes:
        match_cards.extend(soup.find_all("div", class_=match_class))

    print(f"Number of matches found: {len(match_cards)}")

    fixtures = []

    for match in match_cards:
        try:
            match_name_elem = match.find("div", class_="cb-col-60 cb-col cb-srs-mtchs-tm")
            timestamp_span = match.find("span", class_="schedule-date")
            venue_elem = match.find("div", class_="text-gray")

            if not match_name_elem or not venue_elem:
                continue

            match_name = match_name_elem.find("span").text.strip()
            venue = venue_elem.text.strip()

            # Extract timestamp if available
            timestamp = int(timestamp_span["timestamp"]) if timestamp_span and timestamp_span.has_attr("timestamp") else None
            date = datetime.datetime.fromtimestamp(timestamp / 1000).strftime("%Y-%m-%d %H:%M:%S") if timestamp else "Unknown"

            # Extract team names
            teams = match_name.split(",")[0].split(" vs ")
            team1, team2 = teams[0].strip(), teams[1].strip() if len(teams) > 1 else ("Unknown", "Unknown")

            # Extract match link
            link_elem = match.find("a", class_="text-hvr-underline")
            link_ref = link_elem["href"] if link_elem and link_elem.has_attr("href") else "Unknown"
            match_link = base_url + link_ref

            # Extract short name from the link reference
            # Format: converts 'match-name-1234' to '1234-match-name'
            short = link_ref.split("/")[-1].split("-match")[0]
            short_name = short.split("-")[-1][:-2] + "-" + short[:short.rindex('-')]

            fixtures.append({
                "match_name": match_name,
                "short_name": short_name,
                "date": date,
                "team1": team1,
                "team2": team2,
                "venue": venue,
                "link": match_link
            })
            
        except Exception as e:
            print(f"Error processing match: {e}")

    # Sort fixtures by date
    fixtures.sort(key=lambda x: datetime.datetime.strptime(x["date"], "%Y-%m-%d %H:%M:%S") if x["date"] != "Unknown" else datetime.datetime.max)

    return fixtures


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
    s3_key = f"{prefix}fixtures/{filename}"

    try:
        # Attempt to upload the JSON data to S3
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"Successfully uploaded {filename} to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        # Log any errors that occur during S3 upload
        print(f"Failed to upload {filename} to S3: {e}")


def main():
    """
    Main function to orchestrate the IPL match fixtures extraction pipeline.

    This function serves as the entry point for the data extraction process and performs
    the following operations:
    1. Initializes the data extraction process by fetching match fixtures from the source URL
    2. Processes the extracted fixtures data
    3. Uploads the processed data to AWS S3
    """
    global datafiles
    datafiles = []

    # Extract match fixtures from the configured URL
    fixtures = extract_fixtures(fixtures_url)
    
    # Upload fixtures to S3 if extraction was successful
    if fixtures:
        save_json_s3(fixtures, RAW_PREFIX, "fixtures")
        save_json_s3(fixtures, BRONZE_PREFIX, "players")

if __name__ == "__main__":
    main()

