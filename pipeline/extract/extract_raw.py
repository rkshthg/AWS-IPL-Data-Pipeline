import os
import re
import json
import boto3
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

S3_BUCKET = os.getenv("S3_BUCKET")
RAW_PREFIX = os.getenv("RAW_PREFIX")          # Prefix for RAW data storage in S3
BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")    # Prefix for BRONZE data storage in S3

session = boto3.Session()
s3 = session.client("s3")

def read_json(name):
    try:
        key = f"{RAW_PREFIX}{name}/{name}.json"
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error reading '{name}' from S3: {e}")
        return None

def list_json(name):
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"{RAW_PREFIX}{name}/{name}")
        if "Contents" not in response:
            return []
        return [obj["Key"].split("/")[-1] for obj in response["Contents"] if obj["Key"].endswith(".json")]
    except Exception as e:
        logger.error(f"Error fetching files from S3: {e}")
        return []

def save_json(data, dir, file, prefix=RAW_PREFIX):
    json_data = json.dumps(data, indent=4)
    filename = f"{file}.json"
    s3_key = f"{prefix}{dir}/{filename}"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded {filename} to s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        logger.error(f"Failed to upload {filename} to S3: {e}")

def fetch_html(url):
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--headless")  # Run in headless mode (no UI)
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920x1080")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    try:
        driver.get(url)
        # Wait for the page to load
        wait = WebDriverWait(driver, 10)
        while True:
            try:
                # Check if the "Load More Commentary" button exists and is visible
                load_more_btn = wait.until(EC.presence_of_element_located((By.ID, "full_commentary_btn")))
                if load_more_btn.is_displayed():
                    driver.execute_script("arguments[0].click();", load_more_btn)  # Click the button
                    time.sleep(2)  # Wait for new content to load
                else:
                    break  # Exit if button is not found
            except Exception as e:
                break
        html = driver.page_source
    
    except Exception as e:
        logger.error(f"Error during Selenium fetch: {e}")
        # None  # Ignore any exceptions
    
    finally:
        driver.quit() # Close the browser

    return html

def extract_toss_info(soup):
    # Extracting Toss Information
    toss_info = soup.find_all("p", class_="cb-com-ln ng-binding ng-scope cb-col cb-col-100")
    # Extract the toss decision
    toss_text = ""
    for toss in toss_info:
        text = toss.text.strip()
        if "have won the toss and have opted" in text:
            toss_text = text
            logger.info(f"Toss Decision: {toss_text}")
            break
    toss_winner = toss_text.split("have")[0].strip() if toss_text else "N/A"
    toss_decision = toss_text.split("to")[-1].strip() if toss_text else "N/A"

    return toss_winner, toss_decision

def super_over(soup, metadata):
    logger.info("Super Over")
    balls_bowled = []
    super_balls = soup.find_all("p", class_="cb-com-ln ng-binding ng-scope cb-col cb-col-100")
    for ball in reversed(super_balls):
        super_ball = ball.text.strip()
        if super_ball.startswith("Ball"):
            over = 0
            ball_count = super_ball.split(" ")[1]
            if ball_count == "one":
                ball_no = 1 
            elif ball_count == "two":
                ball_no = 2
            elif ball_count == "three":
                ball_no = 3
            elif ball_count == "four":
                ball_no = 4
            elif ball_count == "five":
                ball_no = 5
            elif ball_count == "six":
                ball_no = 6
            else: ball_no = 0
            info = super_ball.split(" - ", 1)[1]

            balls_bowled.append({
                "match": metadata["match"],
                "short_name" : metadata["short_name"],
                "date": metadata["date"],
                "time": metadata["time"],
                "venue": metadata["venue"],
                "over": over,
                "ball": int(ball_no),
                "event_info": info
            })
        else: continue
    
    return balls_bowled

def extract_balls_bowled(soup, metadata):
    balls_bowled = soup.find_all("div", class_="cb-col cb-col-100 ng-scope")
    logger.info("Number of balls bowled: %d", len(balls_bowled))

    balls = []

    for ball in reversed(balls_bowled):
        if ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope"):
            over = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope").text.strip().split(".")[0]
            ball_no = ball.find("div", class_="cb-mat-mnu-wrp cb-ovr-num ng-binding ng-scope").text.strip().split(".")[1]
            info = ball.find("p", class_="cb-com-ln ng-binding ng-scope cb-col cb-col-90").text.strip()
            event_info = info.split(", ")[1].split(",")[0]

            balls.append({
                "match": metadata["match"],
                "short_name" : metadata["short_name"], 
                "date": metadata["date"],
                "time": metadata["time"],
                "venue": metadata["venue"],
                "over": over,
                "ball": int(ball_no),
                "event_info": info
                })

            if "Super Over" in event_info:
                super = super_over(soup, metadata)
                balls.extend(super)

        else: continue

    return balls

def normalize_name(short_name):
    # Match pattern: digits followed by hyphen and rest of string
    match = re.match(r"(\d+)-(.*)", short_name)
    if match:
        number = match.group(1).zfill(2)  # Pad number to 2 digits
        rest = match.group(2)
        return f"{number}-{rest}"
    return short_name

def main():
    """
    Main function to orchestrate the ball-by-ball data extraction process.

    This function:
    1. Reads fixture information from S3
    2. Determines which matches to process based on current time
    3. Extracts match metadata and ball-by-ball data for each match
    4. Uploads processed data to S3
    """
    fixtures = read_json("fixtures")
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
        for fixture in reversed(fixtures):
        # for fixture in fixtures:
            metadata = {}
            if datetime.datetime.strptime(fixture["date"], "%Y-%m-%d %H:%M:%S") < today:
                soup = BeautifulSoup(fetch_html(fixture["link"]), "html.parser") 
                logger.info(f"Extracting for ball: {fixture["short_name"]}")
                metadata["match"] = fixture["match_name"]
                metadata["short_name"] = normalize_name(fixture["short_name"])
                metadata["home_team"] = metadata["match"].split(" vs ")[0]
                metadata["away_team"] = metadata["match"].split(" vs ")[1].split(",")[0]
                metadata["date"] = fixture["date"].split(" ")[0]
                metadata["time"] = fixture["date"].split(" ")[1]
                metadata["venue"] = fixture["venue"]
                metadata["toss_winner"], metadata["toss_decision"] = extract_toss_info(soup)
                balls = extract_balls_bowled(soup, metadata)
                save_json(metadata, "metadata", metadata["short_name"])
                save_json(metadata, "metadata", metadata["short_name"], BRONZE_PREFIX)
                save_json(balls, "ball-by-ball", metadata["short_name"])
                break
    except IndentationError as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()