import requests
import time
from bs4 import BeautifulSoup
import json
import re
import io
import os
import sys
import boto3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pipeline_2026.utils import (
    fetch_webpage,
    save_file_s3,
    read_json_s3,
    read_file_s3,
    get_s3_client,
    logger,
    S3_RAW,
    S3_BRONZE
)

# Load environment variables
load_dotenv()

# AWS Configuration
DATA_PREFIX = os.getenv("DATA_PREFIX")

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads

def get_match_state(soup):
    try:
        logger.info("Getting Match Status...")

        match_state_0 = soup.find("div", class_="text-cbTextLink")
        if match_state_0:
            match_state_0 = match_state_0.text.strip().lower()
            if ("won by" in match_state_0):
                return False 

        match_state_1 = soup.find("div", class_="text-base text-cbLive")
        if match_state_1:
            logger.info(f"MATCH STATE: {match_state_1.text.strip().lower()}")
            if ("delayed" in match_state_1.text.strip().lower()) or ("cancelled" in match_state_1.text.strip().lower()):
                logger.info(f"{match_state_1.text.strip().upper()}")
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
    """
    Extracts toss winner and toss decision from the Cricbuzz toss HTML block.
    """
    toss_winner, toss_decision = "", ""
    
    # 1. Target the outer card container div
    container_class = "flex mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver wb:border-0"
    cards = soup.find_all("div", class_=container_class)
    
    for card in cards:
        # 2. Locate the <b> tag housing the toss statement
        b_tag = card.find("b")
        if not b_tag:
            continue
            
        text = b_tag.text.strip()
        
        # 3. Check if the text contains toss details
        if "won the toss" in text.lower():
            if "have won the toss and have opted to" in text.lower():
                parts = text.split("have won the toss and have opted to")
                toss_winner = parts[0].strip()
                toss_decision = parts[1].strip()
            elif "opted to" in text.lower():
                parts = text.split("opted to")
                toss_decision = parts[1].strip()
                toss_winner = parts[0].replace("have won the toss and", "").replace("won the toss and", "").strip()
            break
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
                # logger.info(f"BALL NO: {over}.{ball_no}")

                info = ball.find_all("div")[-1].text.strip()
                # logger.info(f"\nOver: {over}.{ball_no},\nInfo: {info}")

                full_info = info.split(",")
                bowler = full_info[0].split(" to ")[0].strip()
                batsman = full_info[0].split(" to ")[1].strip()
                # logger.info(f"BALL INFO: {full_info} \n{batsman} {bowler}")
                ball_event = full_info[1].strip()
                info = info.replace(",", ";").replace('"',"").split(";", 2)[-1].strip()
                
                balls.append(f"{metadata['short_name']},{metadata['date']},{metadata['time']},{metadata['venue']},{over},{ball_no},{bowler},{batsman},{ball_event},{info},{str(datetime.now())}")

            else: continue

        except IndexError as e:
            if ball.find("div", class_="font-bold text-center !min-w-[1.5rem]"):
                over = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[0]
                ball_no = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[1]
                prev_ball = balls[-1].split(',')
                bowler = prev_ball[6]
                batsman = prev_ball[7]
                info = ball.find_all("div")[-1].text.strip()
                
                ball_event = info
                info = info.replace(",", ";").replace('"',"").split(";", 2)[-1].strip()
                
                balls.append(f"{metadata['short_name']},{metadata['date']},{metadata['time']},{metadata['venue']},{over},{ball_no},{bowler},{batsman},{ball_event},{info},{str(datetime.now())}")

            else: continue

    return balls

def main():
    try:
        s3_client = get_s3_client()
        now = datetime.now()
        date = None
        extract_all = False
        extract_random = False

        if len(sys.argv) > 1:
            if sys.argv[1].lower() == "all":
                extract_all = True
                logger.info("Extracting data for all matches in fixtures.json")
            else:
                date = sys.argv[1]
                logger.info(f"Using mock date: {date}")
        else:
            extract_random = True
            logger.info("No arguments passed. Picking 3 random matches.")
            
        hour = now.hour
        minute = now.minute

        fixtures_data = read_file_s3(s3_client, S3_RAW, f"{DATA_PREFIX}fixtures/fixtures.json")
        if fixtures_data:
            clean_data = fixtures_data.strip()
            if not clean_data.startswith('[') and not clean_data.startswith('{'):
                clean_data = f"[{clean_data}]"
            fixtures_df = pd.read_json(io.StringIO(clean_data))
        else:
            fixtures_df = pd.DataFrame()

##############################################################################################################################################################################################################
        # Getting Match Schedule Info...
        logger.info("Getting Match Schedule Info...")

        if extract_all:
            match_rows = fixtures_df
        elif extract_random:
            match_rows = fixtures_df.sample(n=3)
        else:
            match_rows = fixtures_df[fixtures_df['date'] == date]
 
        for ind in range(len(match_rows)):
            match_row = match_rows.iloc[ind]
            match = match_row['match_short']
            match_url = match_row['link']
            match_date = match_row['date']
            match_time = match_row['time']

##############################################################################################################################################################################################################
            # Extract Metadata
            logger.info(f"Getting Match Metadata for {match}...")
            metadata_file = f"{match}_meta.json"
            MATCH_PREFIX = f"match/{match}/"
            metadata_key = f"{DATA_PREFIX}{MATCH_PREFIX}{metadata_file}"

            try:
                s3_client.head_object(Bucket=S3_RAW, Key=metadata_key)
                meta_exists = True
            except Exception as e:
                logger.info(f"File does not exist in S3: {e}")
                meta_exists = False
            
            if meta_exists:
                logger.info(f"Metadata file '{metadata_file}' exists.")
                metadata = read_json_s3(s3_client, S3_RAW, metadata_key)

                logger.info("Extracting Ball event data..")
                
                webpage = fetch_webpage(match_url)
                soup = BeautifulSoup(webpage, "html.parser")  

                logger.info("Extracting balls bowled...")
                balls = extract_balls_bowled(soup, metadata)

                logger.info(f"Balls Bowled: {len(balls)}")
                ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0].replace(':', '-')}.csv"

                csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
                
                for ball in balls:
                    csv_data += f"{ball}\n"

                logger.info(f"Writing extracted data for {match} to S3 csv file...")
                save_file_s3(s3_client, csv_data, S3_RAW, f"{DATA_PREFIX}{MATCH_PREFIX}{ball_file}")

                logger.info("===========================================================================================\n")
                logger.info(f"Data extraction for {match} complete!!")
                logger.info("===========================================================================================\n\n")

            else:   
                logger.info(f"Creating new metadata file for {match}")

                metadata = {
                    "match": match_row['match'],
                    "short_name": match,
                    "home_team": match_row['home_team'],
                    "away_team": match_row['away_team'],
                    "date": match_date,
                    "time": match_time,
                    "venue": match_row['stadium']
                }
                webpage = fetch_webpage(match_url)
                soup = BeautifulSoup(webpage, "html.parser")

                metadata["toss_winner"], metadata["toss_decision"] = extract_toss_info(soup)

                logger.info("Writing metadata file to S3...")
                save_file_s3(s3_client, metadata, S3_RAW, metadata_key)

                balls = extract_balls_bowled(soup, metadata)
                logger.info(f"Balls Bowled: {len(balls)}")
                ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0].replace(':', '-')}.csv"

                csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
                
                for ball in balls:
                    csv_data += f"{ball}\n"

                logger.info(f"Writing extracted data for {match} to S3 csv file...")
                save_file_s3(s3_client, csv_data, S3_RAW, f"{DATA_PREFIX}{MATCH_PREFIX}{ball_file}")

                logger.info("=" * 91)
                logger.info(f"Data extraction for {match} complete!!")
                logger.info("=" * 91)

                logger.info("Waiting 45 seconds for downstream Glue Silver Job processing...")
                time.sleep(45)


    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()