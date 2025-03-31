import requests
import json
import os
import datetime
from bs4 import BeautifulSoup
import boto3
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")
S3_BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = os.getenv("RAW_PREFIX")

base_url = os.getenv("BASE_URL")
fixtures_url = os.getenv("FIXTURES_URL")

def fetch_html(url):
    """Fetches the HTML content of a given URL."""
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.text
    except requests.RequestException as e:
        print(f"Failed to fetch the page: {e}")
        return None

def extract_fixtures(url):
    """Extracts match fixtures from the given URL."""
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

            # Extract short name
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

    fixtures = extract_fixtures(fixtures_url)
    if fixtures:
        save_json_s3(fixtures, RAW_PREFIX, "fixtures")

if __name__ == "__main__":
    main()
