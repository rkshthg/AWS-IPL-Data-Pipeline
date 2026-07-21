import requests
import time
from bs4 import BeautifulSoup
import json
import os
import boto3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s]\n%(filename)s:%(lineno)d --- %(message)s\n")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # AWS access key for S3 authentication
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # AWS secret key for S3 authentication
S3_RAW = os.getenv("S3_RAW")                  # Target S3 bucket name
DATA_PREFIX = os.getenv("DATA_PREFIX")
MATCH_PREFIX = os.getenv("MATCH_PREFIX")

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads

def save_file_s3(client, data, prefix, filename):
    if filename.endswith(".json"):
        file_data = json.dumps(data, indent=4)
    else:
        file_data = data
    key = f"{DATA_PREFIX}{prefix}{filename}"

    try:
        client.put_object(
            Bucket=S3_RAW,
            Key=key,
            Body=file_data,
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded {filename} to s3://{S3_RAW}/{key}")
        
    except Exception as e:
        logger.error(f"Failed to upload {filename} to S3: \n{e}")

def read_json_s3(client, prefix, name):
    try:
        key = f"{DATA_PREFIX}{prefix}{name}"
        logger.info(f"Loading data from {name}")

        response = client.get_object(Bucket=S3_RAW, Key=key)

        if response: logger.info(f"Successfully read from S3 file: {name}")

        return json.loads(response['Body'].read().decode('utf-8'))
    
    except Exception as e:
        logger.error(f"Error reading '{name}' from S3: {e}")
        return None

def fetch_webpage(url):
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

        last_height = driver.execute_script("return document.body.scrollHeight")

        while True:
            # Scroll down to the bottom
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Wait for new content to load (adjust time based on your internet speed)
            time.sleep(2)

            # Calculate new scroll height and compare with last scroll height
            new_height = driver.execute_script("return document.body.scrollHeight")
            
            if new_height == last_height:
                # If heights are the same, we've reached the absolute end
                break
                
            last_height = new_height

        html = driver.page_source
    
    except Exception as e:
        logger.error(f"Error during Selenium fetch: {e}")
        # None  # Ignore any exceptions
    
    finally:
        driver.quit() # Close the browser

    return html

def fetch_html(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise HTTP error if any
        return response.content
    except requests.RequestException as e:
        logger.error(f"HTTP Error: {e}")
        return None
    
def get_match_state(soup):
    try:
        logger.info("Getting Match Status...")

        match_state_0 = soup.find("div", class_="text-cbTextLink")
        if match_state_0:
            match_state_0 = match_state_0.text.strip().lower()
            # logger.info(f"MATCH STATE 0: {match_state_0}")
            if ("won by" in match_state_0):
                return False 

        match_state_1 = soup.find("div", class_="text-base text-cbLive")
        if match_state_1:
            logger.info(f"MATCH STATE 1: {match_state_1}").text.strip().lower()
            if ("delayed" in match_state_1) or ("cancelled" in match_state_1):
                logger.info(f"{match_state_1.upper()}")
                return False 

        match_state_2 = soup.find("div", class_="text-cbTxtLive")
        if match_state_2:
            match_state_2 = match_state_2.text.strip().lower()
            if ("timeout" in match_state_2) or ("break" in match_state_2):
                logger.info(f"{match_state_2.upper()}")
                return False
        
        return True

    except requests.RequestException as e:
        logger.error(f"Match State: {e}")
        return None

def extract_toss_info(soup):
    # Extracting Toss Information
    toss_winner, toss_decision = "", ""

    with open("test.html", "w") as f:
        f.write(soup.prettify())

    info_cards = soup.find_all("div", class_="flex mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver wb:border-0")

    for info_card in info_cards:
        # info = info_card.find_all("div", class_="flex flex-col gap-2 items-center")
        # for inf in info:
        cards = info_card.find_all("b")
        if cards == []: continue
        for card in cards:
            print("TOSS INFO: \n", len(card))
            toss_text = card.text.strip()
            if toss_text and "have won the toss and have opted to" in toss_text:
                logger.info(f"Toss Decision: {toss_text}")
                toss_winner = toss_text.split("have won the toss and have opted to")[0].strip()
                toss_decision = toss_text.split("have won the toss and have opted to")[-1].strip()
                return toss_winner, toss_decision

def extract_balls_bowled(soup, metadata):
    balls_bowled = soup.find_all("div", class_=["flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver wb:border-0",
                                                "flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver border-t-0 wb:border-0"])

    balls = []

    for ball in reversed(balls_bowled):
        try:
            if ball.find("div", class_="font-bold text-center !min-w-[1.5rem]"):
                over = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[0]
                ball_no = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[1]
                logger.info(f"BALL NO: {over}.{ball_no}")

                info = ball.find_all("div")[-1].text.strip()
                # event_info = info.split(", ")[1].split(",")[0]
                logger.info(f"\nOver: {over}.{ball_no},\nInfo: {info}")

                full_info = info.split(",")
                bowler = full_info[0].split(" to ")[0].strip()
                batsman = full_info[0].split(" to ")[1].strip()
                logger.info(f"BALL INFO: {full_info} \n{batsman} {bowler}")
                ball_event = full_info[1].strip()
                info = info.replace(",", ";").replace('"',"").split(";", 2)[-1].strip()
                
                balls.append(f"{metadata["short_name"]},{metadata["date"]},{metadata["time"]},{metadata["venue"]},{over},{ball_no},{bowler},{batsman},{ball_event},{info},{str(datetime.now())}")

            else: continue

        except IndexError as e:
            if ball.find("div", class_="font-bold text-center !min-w-[1.5rem]"):
                over = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[0]
                ball_no = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[1]
                # logger.info(f"BALL NO: {over}.{ball_no}")
                prev_ball = balls[-1].split(',')
                bowler = prev_ball[6]
                batsman = prev_ball[7]
                info = ball.find_all("div")[-1].text.strip()
                ball_event = "no run"
                # event_info = info.split(", ")[1].split(",")[0]
                # logger.info(f"\nOver: {over}.{ball_no},\nInfo: {info}")
                
                ball_event = info
                info = info.replace(",", ";").replace('"',"").split(";", 2)[-1].strip()
                
                balls.append(f"{metadata["short_name"]},{metadata["date"]},{metadata["time"]},{metadata["venue"]},{over},{ball_no},{bowler},{batsman},{ball_event},{info},{str(datetime.now())}")

            else: continue

    return balls

def main():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

        now = datetime.now()
        date = str(datetime.strftime(now, "%B %d"))
        hour = now.hour
        minute = now.minute

        fixtures_df = pd.read_json("/home/rakshith/Data/IPL-2026/data/fixtures.json")

##############################################################################################################################################################################################################
        # Getting Match Schedule Info...
        logger.info("Getting Match Schedule Info...")

        match_rows = fixtures_df[fixtures_df['date'] < date]
 
        for ind in range(35, len(match_rows)):
            match_row = match_rows.iloc[ind]
            match = match_row['match_short']
            match_url = match_row['link']

##############################################################################################################################################################################################################
            # Extract Metadata
            logger.info(f"Getting Match Metadata for {match}...")
            metadata_file = f"{match}_meta.json"
            MATCH_PREFIX = f"match/{match}/"
            metadata_key = f"{MATCH_PREFIX}{metadata_file}"

            if os.path.isdir(f"/home/rakshith/Data/IPL-2026/data/{match}"):
                logger.info("Directory exists.")
            else:
                logger.info("Directory does not exist.")
                os.makedirs(f"/home/rakshith/Data/IPL-2026/data/{match}", mode=0o777, exist_ok=False)

            try:
                s3_client.head_object(Bucket=S3_RAW, Key=f"data/{metadata_key}")
                meta_exists = True
            except Exception as e:
                logger.error(f"File does not exist in {S3_RAW}/{metadata_key}: \n{e}")
                meta_exists = False
            
            if meta_exists:
                logger.info(f"Metadata file '{metadata_file}' exists.")
                metadata = read_json_s3(s3_client, MATCH_PREFIX, metadata_file)

                logger.info("Extracting Ball event data..")
                
                webpage = fetch_webpage(match_url)
                soup = BeautifulSoup(webpage, "html.parser")  

                logger.info("Extracting balls bowled...")
                balls = extract_balls_bowled(soup, metadata)

                logger.info("Balls Bowled:", len(balls))
                ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0]}.csv"

                csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
                
                for ball in balls:
                    csv_data += f"{ball}\n"

                logger.info("Writing extracted data to local csv...")
                with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_data.csv", "a") as f:
                    for ball in balls:
                        f.write(f"{ball}\n")
                        logger.info(f"Ball data written to {match}_data.csv")

                logger.info(f"Writing extracted data for {match} to S3 csv file...")
                save_file_s3(s3_client, csv_data, MATCH_PREFIX, ball_file)

                print("===========================================================================================\n")
                logger.info(f"Data extraction for {match} complete!!")
                print("===========================================================================================\n\n")

                # time.sleep(30)
                # return None

            else:   
                logger.info(f"Creating new metadata file for {match}")

                metadata = {
                    "match": match_row['match'],
                    "short_name": match,
                    "home_team": match_row['home_team'],
                    "away_team": match_row['away_team'],
                    "date": date,
                    "time": str(now).split(' ')[1].split('.')[0],
                    "venue": match_row['stadium']
                }
                webpage = fetch_webpage(match_url)
                soup = BeautifulSoup(webpage, "html.parser")

                metadata["toss_winner"], metadata["toss_decision"] = extract_toss_info(soup)

                logger.info("Creating local csv file...")
                with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_data.csv", "w") as f:
                    f.write("match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n")
                
                logger.info("Creating local metadata file...")
                with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_meta.json", "w") as f:
                    json.dump(metadata, f, indent=4)

                logger.info("Writing metadata file to S3...")
                save_file_s3(s3_client, metadata, MATCH_PREFIX, metadata_file)


                balls = extract_balls_bowled(soup, metadata)
                ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0]}.csv"

                csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
                
                for ball in balls:
                    csv_data += f"{ball}\n"

                logger.info("Writing extracted data to local csv...")
                with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_data.csv", "a") as f:
                    for ball in balls:
                        f.write(f"{ball}\n")
                        logger.info(f"Ball data written to {match}_data.csv")

                logger.info(f"Writing extracted data for {match} to S3 csv file...")
                save_file_s3(s3_client, csv_data, MATCH_PREFIX, ball_file)

                print("===========================================================================================\n")
                logger.info(f"Data extraction for {match} complete!!")
                print("===========================================================================================\n\n")

                # return None

    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()