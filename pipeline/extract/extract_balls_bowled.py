from bs4 import BeautifulSoup
import json
import os
import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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

FIXTURES = os.getenv("FIXTURES")

def read_json_s3(filename):
    """
    Reads a JSON file from an S3 bucket and returns its contents.

    Args:
        filename (str): The name of the JSON file to read.

    Returns:
        dict or None: The contents of the JSON file as a dictionary, or None if an error occurs.
    """
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY
    )
    key = f"{RAW_PREFIX}{filename}.json"
        
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    return json.loads(json_data)


def fetch_ball_html(url):
    """
    Fetches the HTML content of a given URL using Selenium WebDriver.

    Args:
        url (str): The URL to fetch HTML content from.

    Returns:
        str: The HTML content of the webpage.
    """
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")  # Run in headless mode (no UI)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)

    # Wait for the page to load
    wait = WebDriverWait(driver, 10)

    try:
        while True:
            # Check if the "Load More Commentary" button exists and is visible
            load_more_btn = wait.until(EC.presence_of_element_located((By.ID, "full_commentary_btn")))
            
            if load_more_btn.is_displayed():
                driver.execute_script("arguments[0].click();", load_more_btn)  # Click the button
                time.sleep(2)  # Wait for new content to load
            else:
                break  # Exit if button is not found
        driver.quit()  # Close the browser
    except Exception as e:
        # print(f"Error: {e}")
        None  # Ignore any exceptions

    return driver.page_source

def extract_match_metadata(url):
    soup = BeautifulSoup(fetch_ball_html(url), "html.parser")
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
    toss_info = soup.find_all("p", class_="cb-com-ln ng-binding ng-scope cb-col cb-col-100")

    # Print Extracted Data
    print(f"Match: {match_title}")
    print(f"Date: {match_date}")

    # Extract the toss decision from the correct section
    for toss in toss_info:
        toss_text = toss.text.strip()
        if "have won the toss and have opted" in toss_text:  # Ensuring correct match
            print(f"Toss Decision: {toss_text}")
            break

    # Extracting Toss Winner
    toss_winner = toss_text.split("have")[0].strip() if toss_text else "N/A"
    toss_decision = toss_text.split("to")[-1].strip() if toss_text else "N/A"

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
        "batting_team": batting_team,
        "bowling_team": bowling_team
    }
    return match_metadata

def extract_balls_bowled(url):
    """
    Extracts the match details by every ball bowled from a given URL.

    Args:
        url (str): The URL of the match.

    Returns:
        int: The number of balls bowled.
    """
    soup = BeautifulSoup(fetch_ball_html(url), "html.parser")
    # Extracting Match Title (Teams Playing)

    balls_bowled = soup.find_all("div", class_="cb-col cb-col-100 ng-scope")
    print("\nNumber of balls bowled:", len(balls_bowled))

    batting_team = metadata["batting_team"]
    bowling_team = metadata["bowling_team"]
    innings = 1
    score = 0
    wickets = 0
    overs = 0

    balls = []

    for ball in reversed(balls_bowled):
        if ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope"):
            runs = 0
            extra_runs = 0
            extra = 0
            extra_type = None
            rebowl = 0
            wicket = 0
            wicket_method = None
            out_batsman = None
            valid_ball = 1

            over = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope").text.strip().split(".")[0]
            ball_no = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope").text.strip().split(".")[1]
            info = ball.find("p", class_="cb-com-ln ng-binding ng-scope cb-col cb-col-90").text.strip()
            batsman = info.split(", ")[0].split(" to ")[1]
            bowler = info.split(" to ")[0]
            event_info = info.split(", ")[1].split(",")[0]
            
            if event_info == "SIX": 
                runs = 6 
            elif event_info == "FOUR": 
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
                runs = 0
                extra = 1
                extra_runs = 1
                extra_type = "wide" 
                valid_ball = 0
                rebowl = 1
            elif event_info.startswith("no ball"): 
                runs = 0
                extra = 1
                extra_runs = 1
                extra_type = "no ball"
                valid_ball = 0
                rebowl = 1
            elif event_info.startswith("leg byes"):  
                if info.split(", ", 2)[-1].split(", ")[0] == "FOUR":
                    runs = 4
                else:
                    runs = int(info.split(", ", 2)[-1].split(" ")[0])
                extra = 1
                extra_type = "leg byes"
            elif event_info.startswith("byes"): 
                if info.split(", ", 2)[-1].split(", ")[0] == "FOUR":
                    runs = 4
                else:
                    runs = int(info.split(", ", 2)[-1].split(" ")[0])
                extra = 1
                extra_type = "byes"
            else: 
                runs = int(event_info.split(" ")[0])

            if wicket == 1:
                wickets += 1
            score += (runs+extra_runs)
            overs = float(f"{over}.{ball_no}")
            
            balls.append({
                "match": metadata["match_title"],
                "date": metadata["match_date"],
                "venue": metadata["match_venue"],
                "innings": innings,
                "batting_team": batting_team,
                "bowling_team": bowling_team,
                "over": int(over),
                "ball": int(ball_no),
                "batsman": batsman,
                "bowler": bowler,
                "event": info,
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
                "target": target if innings == 2 else None
                })

            if (overs == 19.6 and rebowl == 0) or wickets == 10:
                innings = 2
                target = score + 1
                batting_team, bowling_team = bowling_team, batting_team
                overs = 0
                score = 0
                wickets = 0
                
        else: continue

    return balls

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
    global metadata
    fixtures = read_json_s3("fixtures")
    now = datetime.datetime.now()
    match1 = now.replace(hour=15, minute=30, second=0, microsecond=0)
    match2 = now.replace(hour=19, minute=30, second=0, microsecond=0)
    if now > match1 and now < match2:
        today = now
    elif now > match2:
        today = match2
    else: 
        today = match1
    try:
        for fixture in fixtures:
            if datetime.datetime.strptime(fixture["date"], "%Y-%m-%d %H:%M:%S") < today:
                metadata = extract_match_metadata(fixture["link"])
                balls = extract_balls_bowled(fixture["link"])
                # match = fixture["match"].split(",")[-1].strip().replace(" ", "_")
                save_json_s3(balls, RAW_PREFIX, fixture["short_name"])
                print("Data saved locally for", fixture["match_name"])
                print("\n")
    except IndentationError as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
