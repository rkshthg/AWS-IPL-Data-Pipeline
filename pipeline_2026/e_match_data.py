import os 
import re
import json
import boto3
from datetime import datetime
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

# AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
# AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# S3_BUCKET = os.getenv("S3_BUCKET")
# RAW_PREFIX = os.getenv("RAW_PREFIX")          # Prefix for RAW data storage in S3
# BRONZE_PREFIX = os.getenv("BRONZE_PREFIX")    # Prefix for BRONZE data storage in S3

# session = boto3.Session()
# s3 = session.client("s3")

test_url = "https://www.cricbuzz.com/live-cricket-scores/148963/krk-vs-qtg-2nd-match-pakistan-super-league-2026"

def fetch_html(url):
    # Set up Selenium WebDriver
    options = webdriver.ChromeOptions()
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    options.add_argument("--headless")  # Run in headless mode (no UI)
    options.add_argument("--disable-gpu")

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
    toss_info = soup.find_all("div", class_="flex  mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver  wb:border-0")
    
    # Extract the toss decision
    toss_text = ""
    for toss in toss_info:
        text = toss.text.strip()
        if "have won the toss and have opted" in text:
            toss_text = text
            logger.info(f"Toss Decision: {toss_text}")
            break
    toss_winner = toss_text.split("have ")[0].strip() if toss_text else "N/A"
    toss_decision = toss_text.split(" to ")[-1].strip() if toss_text else "N/A"

    return toss_winner, toss_decision

def extract_balls_bowled(soup, metadata):
    balls_bowled = soup.find_all("div", class_=["flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver wb:border-0",
                                                "flex gap-4 wb:gap-6 mx-4 wb:mx-4 py-2 border-t border-dotted border-cbChineseSilver border-t-0 wb:border-0"])
    logger.info("Number of balls bowled: %d", len(balls_bowled))

    balls = []

    for ball in reversed(balls_bowled):
        if ball.find("div", class_="font-bold text-center !min-w-[1.5rem]"):
            over = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[0]
            ball_no = ball.find("div", class_="font-bold text-center !min-w-[1.5rem]").text.strip().split(".")[1]
            info = ball.find_all("div")[-1].text.strip()
            event_info = info.split(", ")[1].split(",")[0]
            # print(over, ball_no, info, event_info, "\n")

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

            # if "Super Over" in event_info:
            #     super = super_over(soup, metadata)
            #     balls.extend(super)

        else: continue

    return balls


def main():
    """
    Main function to orchestrate the ball-by-ball data extraction process.

    This function:
    1. Reads fixture information from S3
    2. Determines which matches to process based on current time
    3. Extracts match metadata and ball-by-ball data for each match
    4. Uploads processed data to S3
    """
    now = datetime.now()
    # print(now)
    try:
        soup = BeautifulSoup(fetch_html(test_url), "html.parser") 
        with open("out.html", "w") as f:
            f.write(soup.prettify())
        # print(soup.prettify())
        metadata = {
            "match": "Test Match 1",
            "short_name": "TM_1",
            "home_team": "Quetta Gladiators",
            "away_team": "Karachi Kings",
            "date": str(now).split(' ')[0],
            "time": str(now).split(' ')[1].split('.')[0],
            "venue": "Indira Gandhi International Cricket Stadium, Haldwani"
        }
        metadata["toss_winner"], metadata["toss_decision"] = extract_toss_info(soup)
        print(metadata)
        # balls = extract_balls_bowled(soup, metadata)
        print()

    except IndentationError as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    main()