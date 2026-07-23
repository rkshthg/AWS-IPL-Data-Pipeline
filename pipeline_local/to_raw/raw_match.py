import requests
import time
from bs4 import BeautifulSoup
import json
import re
import os
import sys
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pipeline_local.utils import fetch_webpage, save_file_local, read_json_local, logger

# Load environment variables
load_dotenv()

# URL Configuration
base_url = os.getenv("BASE_URL")              # Base URL for constructing player profile links
teams_url = os.getenv("TEAMS_URL")            # URL to fetch team squads

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
                # event_info = info.split(", ")[1].split(",")[0]
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
                
                balls.append(f"{metadata['short_name']},{metadata['date']},{metadata['time']},{metadata['venue']},{over},{ball_no},{bowler},{batsman},{ball_event},{info},{str(datetime.now())}")

            else: continue

    return balls

def main():
    try:
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

        fixtures_df = pd.read_json("data/raw/fixtures.json")

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
            metadata_key = f"{MATCH_PREFIX}{metadata_file}"

            if os.path.isdir(f"data/raw/{match}"):
                logger.info("Directory exists.")
            else:
                logger.info("Directory does not exist.")
                os.makedirs(f"data/raw/{match}", mode=0o777, exist_ok=False)

            try:
                open(f"data/raw/{match}/{metadata_file}")
                meta_exists = True
            except Exception as e:
                logger.error(f"File does not exist locally: \n{e}")
                meta_exists = False
            
            if meta_exists:
                logger.info(f"Metadata file '{metadata_file}' exists.")
                metadata = read_json_local(f"{match}/", metadata_file)

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

                logger.info("Writing extracted data to local csv...")
                with open(f"data/raw/{match}/{match}_data.csv", "a") as f:
                    for ball in balls:
                        f.write(f"{ball}\n")
                        logger.info(f"Ball data written to {match}_data.csv")

                logger.info(f"Writing extracted data for {match} to local csv file...")
                save_file_local(csv_data, f"{match}/", ball_file)

                logger.info("===========================================================================================\n")
                logger.info(f"Data extraction for {match} complete!!")
                logger.info("===========================================================================================\n\n")

                # time.sleep(30)
                # return None

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

                logger.info("Creating local csv file...")
                with open(f"data/raw/{match}/{match}_data.csv", "w") as f:
                    f.write("match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n")
                
                logger.info("Creating local metadata file...")
                with open(f"data/raw/{match}/{match}_meta.json", "w") as f:
                    json.dump(metadata, f, indent=4)

                logger.info("Writing metadata file locally...")
                save_file_local(metadata, f"{match}/", metadata_file)


                balls = extract_balls_bowled(soup, metadata)
                logger.info(f"Balls Bowled: {len(balls)}")
                ball_file = f"{match}-{str(now).split(' ')[1].split('.')[0].replace(':', '-')}.csv"

                csv_data = "match,date,time,venue,over,ball,bowler,batsman,ball_event,event_info,extract_time\n"
                
                for ball in balls:
                    csv_data += f"{ball}\n"

                logger.info("Writing extracted data to local csv...")
                with open(f"data/raw/{match}/{match}_data.csv", "a") as f:
                    for ball in balls:
                        f.write(f"{ball}\n")
                        # logger.info(f"Ball data written to {match}_data.csv")

                logger.info(f"Writing extracted data for {match} to local csv file...")
                save_file_local(csv_data, f"{match}/", ball_file)

                logger.info("=" * 91)
                logger.info(f"Data extraction for {match} complete!!")
                logger.info("=" * 91)

                # return None

    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()