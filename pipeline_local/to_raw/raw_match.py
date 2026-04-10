import requests
from bs4 import BeautifulSoup
import json
import os
import boto3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

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
            logger.info(f"MATCH STATE 1: {match_state_0}")
            if ("won by" in match_state_0):
                return False 

        match_state_1 = soup.find("div", class_="text-base text-cbLive")
        if match_state_1:
            logger.info(f"MATCH STATE 2: {match_state_1}").text.strip().lower()
            if ("delayed" in match_state_1) or ("cancelled" in match_state_1):
                return False 

        match_state_2 = soup.find("div", class_="text-cbTxtLive")
        if match_state_2:
            match_state_2 = match_state_2.text.strip().lower()
            logger.info(f"MATCH STATE 3: {match_state_2}")
            if ("timeout" in match_state_2) or ("break" in match_state_2):
                return False
        
        return True

    except requests.RequestException as e:
        logger.error(f"Match State: {e}")
        return None

def extract_toss_info(soup):
    # Extracting Toss Information
    toss_winner, toss_decision = "", ""
    info_cards = soup.find_all("div", class_="text-cbPreview")
    for card in info_cards:
        # print("TOSS INFO: \n", card)
        toss_text = card.text.strip()
        if toss_text and "opt to" in toss_text:
            logger.info(f"Toss Decision: {toss_text}")
            toss_winner = toss_text.split("opt ")[0].strip()
            toss_decision = toss_text.split(" to ")[-1].strip()
        break

    return toss_winner, toss_decision

def extract_balls_bowled(soup, metadata):
    balls_bowled = soup.find_all("div", class_=["flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver wb:border-0",
                                                "flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver border-t-0 wb:border-0"])

    balls = []

    for ball in reversed(balls_bowled[:2]):
        if ball.find("div", class_="font-bold text-center !min-w-[1.5rem]"):
            over = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[0]
            ball_no = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[1]
            info = ball.find_all("div")[-1].text.strip()
            event_info = info.split(", ")[1].split(",")[0]
            logger.info(f"\nOver: {over}.{ball_no},\nEvent: {event_info},\nInfo: {info}")
            full_info = info.split(",")
            bowler = full_info[0].split(" to ")[0].strip()
            batsman = full_info[0].split(" to ")[1].strip()
            ball_event = full_info[1].strip()
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

        fixtures_df = pd.read_json("/home/rakshith/Data/IPL-2026/data/fixtures.json")

        match_row = fixtures_df[fixtures_df['date'] == date]
        if len(match_row)>1:
            if hour >= 19:
                match_row = match_row.iloc[1]
                match_url = match_row['link'].item()
            elif hour >= 15:
                match_row = match_row.iloc[0]
                match_url = match_row['link'].item()

        else:
            match_url = match_row['link'].item()

        match = match_row['match_short'].item()
        
        metadata_file = f"{match}_meta.json"
        MATCH_PREFIX = f"match/{match}/"
        metadata_key = f"{MATCH_PREFIX}{metadata_file}"

        if os.path.isdir(f"data/{match}"):
            logger.info("Directory exists.")
            meta_exists = True
        else:
            logger.info("Directory does not exist.")
            os.makedirs(f"data/{match}", mode=0o777, exist_ok=False)
            meta_exists = False

        # try:
        #     s3_client.head_object(Bucket=S3_RAW, Key=metadata_key)
        #     meta_exists = True
        # except Exception as e:
        #     logger.error(f"File does not exist in S3: {e}")
        #     meta_exists = False
        
        if meta_exists:
            logger.info(f"Metadata file '{metadata_file}' exists.")
            metadata = read_json_s3(s3_client, MATCH_PREFIX, metadata_file)

            logger.info("Extracting Ball event data..")
            response = fetch_html(match_url)
            soup = BeautifulSoup(response, "html.parser") 

            match_state = get_match_state(soup)
            if not match_state:
                return None

            balls = extract_balls_bowled(soup, metadata)
            ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0]}.csv"

            csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
            
            for ball in balls:
                csv_data += f"{ball}\n"

            with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_data.csv", "a") as f:
                for ball in balls:
                    f.write(f"{ball}\n")
                    logger.info(f"Ball data written to {match}_data.json")

            save_file_s3(s3_client, csv_data, MATCH_PREFIX, ball_file)

        else:   
            logger.info(f"Creating new metadata file for {match}")

            metadata = {
                "match": match_row['match'].item(),
                "short_name": match,
                "home_team": match_row['home_team'].item(),
                "away_team": match_row['away_team'].item(),
                "date": date,
                "time": str(now).split(' ')[1].split('.')[0],
                "venue": match_row['stadium'].item()
            }

            response = fetch_html(match_url)
            soup = BeautifulSoup(response, "html.parser") 

            metadata["toss_winner"], metadata["toss_decision"] = extract_toss_info(soup)

            with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_data.csv", "w") as f:
                f.write("match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n")
            
            with open(f"/home/rakshith/Data/IPL-2026/data/{match}/{match}_meta.json", "w") as f:
                json.dump(metadata, f, indent=4)

            save_file_s3(s3_client, metadata, MATCH_PREFIX, f"{match}_meta.json")

    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()