import json
import boto3
import requests
from bs4 import BeautifulSoup
import datetime
# from datetime import datetime, timedelta
import hashlib
import re
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX")

session = boto3.Session()
s3 = session.client("s3")

def fetch_html(url):
    """
    Fetches HTML content from a given URL.

    Args:
        url (str): The URL to fetch HTML content from

    Returns:
        str: The HTML content if successful, None if failed

    Raises:
        requests.RequestException: If the HTTP request fails
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.text
    except requests.RequestException as e:
        logger.error(f"Failed to fetch the page: {e}")
        return None


def list_json_files_s3(prefix):
    """
    Lists all JSON files in the S3 bucket with the given prefix.

    Args:
        prefix (str): The prefix to filter S3 objects

    Returns:
        list: List of file names (without .json extension) matching the prefix
              Returns empty list if no files found or on error
    """
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{BRONZE_PREFIX}{prefix}")
        if "Contents" not in response:
            return []
        return [obj["Key"].split("/")[-1] for obj in response["Contents"] if obj["Key"].endswith(".json")]
    
    except Exception as e:
        logger.error(f"Error fetching files from S3: {e}")
        return []
    

def read_json_s3(filename):
    """
    Reads a JSON file from an S3 bucket and returns its contents.

    Args:
        filename (str): The name of the JSON file to read (without extension)

    Returns:
        dict: The contents of the JSON file as a dictionary
    """
    try:
        key = f"{BRONZE_PREFIX}{filename}.json"
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error reading '{filename}' from S3: {e}")
        return None


def generate_id(keys):
    """
    Generates a unique SHA-256 hash ID from the provided keys.

    Args:
        keys (list): List of values to be concatenated and hashed

    Returns:
        str: A 64-character hexadecimal hash string
    """
    # Create a concatenated string
    data_string = "_".join(str(key) for key in keys)
    # Generate SHA-256 hash
    id = hashlib.sha256(data_string.encode()).hexdigest()
    return id


def extract_match_metadata(url):
    """
    Extracts match metadata by scraping the match page.

    Args:
        url (str): URL of the match page

    Returns:
        dict: Dictionary containing match metadata including:
            - match_title: Name of the match
            - match_date: Date of the match
            - match_venue: Venue where match is played
            - toss_winner: Team that won the toss
            - toss_decision: Decision made by toss winner (bat/field)
            - innings: Current innings number
            - batting_team: Team currently batting
            - bowling_team: Team currently bowling
            - score: Current score
            - overs: Current over
            - wickets: Current wickets
            - target: Target score (if applicable)
    """
    soup = BeautifulSoup(fetch_html(url), "html.parser")
    # Extracting Match Title (Teams Playing)
    match_title_tag = soup.find("h1", class_="cb-nav-hdr cb-font-18 line-ht24")
    match_title = match_title_tag.text.strip().split(" -")[0] if match_title_tag else "N/A"
    team1 = match_title.split("vs")[0].strip()
    team2 = match_title.split("vs")[-1].strip().split(",")[0]

    # Extracting Match Date
    date_tag = soup.find("span", itemprop="endDate")
    match_date = date_tag["content"] if date_tag else "N/A"

    # Extracting Venue (Eden Gardens, Kolkata)
    venue_tag = soup.find("a", itemprop="location")
    match_venue = venue_tag.get_text(strip=True) if venue_tag else "N/A"

    # Extracting Toss Information
    toss_info = soup.find("div", class_="cb-text-inprogress").text.strip()
    toss_winner = toss_info.split("opt")[0].strip() if toss_info else "N/A"
    toss_decision = toss_info.split("to")[-1].strip() if toss_info else "N/A"

    # Print Extracted Data
    print(f"Match: {match_title}")
    print(f"Date: {match_date}")
    print(f"Toss Winner: {toss_winner}")
    print(f"Toss Decision: {toss_decision}")

    if toss_winner == team1:
        if toss_decision == "bat":
            batting_team = team1
            bowling_team = team2
        else:
            batting_team = team2
            bowling_team = team1
    else:
        if toss_decision == "bat":
            batting_team = team2
            bowling_team = team1
        else:
            batting_team = team1
            bowling_team = team2

    match_metadata = {
        "match_title": match_title,
        "match_date": match_date,
        "match_venue": match_venue,
        "toss_winner": toss_winner,
        "toss_decision": toss_decision,
        "innings": 1,
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "score": 0,
        "overs": 0,
        "wickets": 0,
        "target":0
    }

    return match_metadata


def get_metadata(last_ball):
    """
    Extracts metadata from the last recorded ball to determine current match state.

    Args:
        last_ball (dict): Dictionary containing information about the last ball bowled

    Returns:
        dict: Updated match metadata including:
            - match_title: Name of the match
            - match_date: Date of the match
            - match_venue: Venue of the match
            - extract_id: Unique identifier for the extraction
            - innings: Current innings (1 or 2)
            - batting_team: Team currently batting
            - bowling_team: Team currently bowling
            - score: Current score
            - overs: Current over number
            - wickets: Current wickets fallen
            - target: Target score for the chasing team
    """
    if last_ball["innings"] == 1:
        if (float(f"{last_ball["over"]}.{last_ball["ball"]}") == 19.6 and \
            last_ball["rebowl"] == 0) or last_ball["current_wickets"] == 10:
            innings = 2
            batting_team = last_ball["bowling_team"]
            bowling_team = last_ball["batting_team"]
        else: 
            innings = 1
            batting_team = last_ball["batting_team"]
            bowling_team = last_ball["bowling_team"]
    else:
        innings = last_ball["innings"]
        batting_team = last_ball["batting_team"]
        bowling_team = last_ball["bowling_team"]

    match_metadata = {
        "match_title": last_ball["match"],
        "match_date": last_ball["date"],
        "match_venue": last_ball["venue"],\
        "extract_id": last_ball["extract_id"],
        "innings": innings,
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "score": last_ball["current_score"],
        "overs": float(f"{last_ball["over"]}.{last_ball["ball"]}"),
        "wickets": last_ball["current_wickets"],
        "target": last_ball["target"]
    }

    return match_metadata


def extract_balls_bowled(url, metadata):
    """
    Extracts ball-by-ball data for the current state of the match.

    Args:
        url (str): URL of the match page
        metadata (dict): Current match metadata

    Returns:
        list: List of dictionaries containing ball-by-ball information including:
            - match details (match name, date, venue)
            - ball specifics (over, ball number, batsman, bowler)
            - runs scored and extras
            - wicket information if applicable
            - current match state (score, wickets, target)
    """
    soup = BeautifulSoup(fetch_html(url), "html.parser")

    ball = soup.find("div", class_="cb-col cb-col-67 cb-nws-lft-col cb-comm-pg")

    batting_team = metadata["batting_team"]
    bowling_team = metadata["bowling_team"]
    innings = metadata["innings"]
    
    score = metadata["score"]
    wickets = metadata["wickets"]

    balls = []

    if ball.find("div", class_="cb-col cb-col-100"):
        runs = 0
        extra_runs = 0
        extra = 0
        extra_type = None
        rebowl = 0
        wicket = 0
        wicket_method = None
        out_batsman = None
        valid_ball = 1

        over = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num").text.strip().split(".")[0]
        ball_no = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num").text.strip().split(".")[1]
        info = ball.find("p", class_="cb-com-ln cb-col cb-col-90").text.strip()
        batsman = info.split(", ")[0].split(" to ")[1]
        bowler = info.split(" to ")[0]
        event_info = info.split(", ")[1].split(",")[0]

        if event_info.split(" ")[0] == "SIX": 
            runs = 6 
        elif event_info.split(" ")[0] == "FOUR": 
            runs = 4
        elif event_info.startswith("no run"): 
            runs = 0
        elif event_info.startswith("out"): 
            runs = 0
            wicket = 1
            if event_info.split(" ", 1)[1].split("!")[0].split(" by")[0].endswith("Run Out"):
                wicket_method = "Run Out"
                out_batsman = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            else: 
                wicket_method = event_info.split(" ", 1)[1].split("!")[0].split(" by")[0]
                out_batsman = batsman
        elif event_info.startswith("wide"): 
            runs = int(re.search("[0-9]+", info.split(", ", 2)[1])[0])
            extra = 1
            extra_runs = 1
            extra_type = "wide" 
            valid_ball = 0
            rebowl = 1
        elif event_info.startswith("no ball"): 
            runs = int(re.search("[0-9]+", info.split(", ", 2)[1])[0])
            extra = 1
            extra_runs = 1
            extra_type = "no ball"
            valid_ball = 0
            rebowl = 1
        elif event_info.startswith("leg byes"):  
            if info.split(", ", 2)[-1].split(", ")[0] == "FOUR":
                runs = 4
            elif info.split(", ", 2)[-1].split(", ")[0] == "SIX":
                runs = 6
            else:
                runs = int(info.split(", ", 2)[-1].split(" ")[0])
            extra = 1
            extra_type = "leg byes"
        elif event_info.startswith("byes"): 
            if info.split(", ", 2)[-1].split(", ")[0] == "FOUR":
                runs = 4
            elif info.split(", ", 2)[-1].split(", ")[0] == "SIX":
                runs = 6
            else:
                runs = int(info.split(", ", 2)[-1].split(" ")[0])
            extra = 1
            extra_type = "byes"
        elif event_info.split(" ")[0].isdigit(): 
            runs = int(event_info.split(" ")[0])
        else:
            runs = int(re.search("[0-9]+", info.split(", ")[0])[0])

        if wicket == 1:
            wickets = metadata["wickets"] + 1
        score = metadata["score"] + (runs+extra_runs)
        overs = float(f"{over}.{ball_no}")

        if (overs == 19.6 and rebowl == 0) or wickets == 10:
            target = score + 1
        else: target = metadata["target"]
        
        extract_id = generate_id([info.split(", ", 1)[-1][:100]])
        keys = [metadata["match_date"], innings, over, ball_no, rebowl, wicket]
        unique_id = generate_id(keys)

        balls.append({
            "match": metadata["match_title"],
            "date": metadata["match_date"],
            "venue": metadata["match_venue"],
            "extract_id": extract_id,
            "innings": innings,
            "batting_team": batting_team,
            "bowling_team": bowling_team,
            "ball_id": unique_id,
            "over": int(over),
            "ball": int(ball_no),
            "batsman": batsman,
            "bowler": bowler,
            "batter_runs": int(runs),
            "extra_runs": int(extra_runs),
            "runs_from_ball": int(runs) + int(extra_runs),
            "extra": extra,
            "extra_type": extra_type,
            "rebowl": rebowl,
            "wicket": wicket,
            "wicket_method": wicket_method,
            "out_batsman": out_batsman,
            "valid_ball": valid_ball,
            "current_score": score,
            "current_wickets": wickets,
            "target": target
            })
        logger.info(f"Successfully extracted data for {over}.{ball_no}")
    return balls


def write_json_s3(data, filename):
    """
    Writes JSON data to an S3 bucket.

    Args:
        data (dict/list): The data to be written to S3
        filename (str): Name of the file (without extension)

    Returns:
        bool: True if write successful, False otherwise

    Raises:
        botocore.exceptions.ClientError: If S3 operations fail
        TypeError: If data is not JSON serializable
    """
    try:
        key = f"{BRONZE_PREFIX}{filename}.json"
        json_data = json.dumps(data, indent=4)
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=json_data, ContentType="application/json")
        return True
    except Exception as e:
        logger.error(f"Error writing to S3 '{filename}': {e}")
        return False


def lambda_handler(event, context):
    """AWS Lambda entry point."""
    fixtures = read_json_s3("fixtures")

    now = datetime.datetime.now()

    match_1530 = now.replace(hour=10, minute=0, second=0, microsecond=0)
    match_1930 = now.replace(hour=14, minute=0, second=0, microsecond=0)

    if now >= match_1530 and now < match_1930:
        match_today = now.replace(hour=15, minute=30, second=0, microsecond=0)
    elif now >= match_1930:
        match_today = now.replace(hour=19, minute=30, second=0, microsecond=0)
    else: 
        match_today = now

    for fixture in fixtures:
        if datetime.datetime.strptime(fixture["date"], "%Y-%m-%d %H:%M:%S") == match_today:        
            # if str(match_today) == fixture["date"]:
            logger.info(f"Processing fixture: {fixture['short_name']}")
            filename = fixture["short_name"]
            print(filename)
            files = list_json_files_s3(filename)
            if files:
                for file in files:
                    file = file.split(".json")[0]
                    prev_data = read_json_s3(file)
                    print(prev_data)
                    last_ball = prev_data[-1]
                    metadata = get_metadata(last_ball)
                    next_ball = extract_balls_bowled(fixture["link"], metadata)
                    if last_ball["extract_id"] == next_ball[0]["extract_id"]:
                        logger.info("No new ball data, skipping S3 write")
                    else:
                        new_data = prev_data + next_ball if next_ball else None
                        if new_data:
                            write_json_s3(new_data, filename)
                            logger.info(f"File updated: {filename}")
            else:
                metadata = extract_match_metadata(fixture["link"])
                next_ball = extract_balls_bowled(fixture["link"], metadata)
                write_json_s3(next_ball, filename)
                logger.info("New file created")

    return {"statusCode": 200, "body": json.dumps("Ball data extraction completed.")}
