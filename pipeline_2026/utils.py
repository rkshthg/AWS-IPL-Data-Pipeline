import os
import json
import logging
import time
import requests
import boto3
from dotenv import load_dotenv



load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_RAW = os.getenv("S3_RAW")
S3_BRONZE = os.getenv("S3_BRONZE")

def setup_logger(name):
    """Configures and returns a standard logger."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s]\n%(filename)s:%(lineno)d --- %(message)s\n")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger(__name__)

def get_s3_client():
    aws_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret = os.getenv("AWS_SECRET_KEY")
    region = os.getenv("AWS_REGION", "us-east-1")
    if aws_key and aws_secret:
        return boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name=region)
    return boto3.client('s3', region_name=region)

def get_glue_client():
    aws_key = os.getenv("AWS_ACCESS_KEY")
    aws_secret = os.getenv("AWS_SECRET_KEY")
    region = os.getenv("AWS_REGION", "us-east-1")
    if aws_key and aws_secret:
        return boto3.client('glue', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name=region)
    return boto3.client('glue', region_name=region)

def fetch_html(url):
    """Fetches HTML content from a URL using requests."""
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logger.error(f"HTTP Error fetching {url}: {e}")
        return None

def fetch_webpage(url, retries=3):
    """Fetches full page HTML using Selenium with automatic scrolling and retry logic."""
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.common.exceptions import TimeoutException, WebDriverException

    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36')
    
    for attempt in range(1, retries + 1):
        driver = None
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(30)
            logger.info(f"Fetching URL (Attempt {attempt}/{retries}): {url}")
            driver.get(url)

            # Wait for content to load
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            last_height = driver.execute_script("return document.body.scrollHeight")
            while True:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)  # Added wait for dynamic content
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    break
                last_height = new_height
                
            # Click "Load More Commentary" button if it exists
            while True:
                try:
                    load_more_btn = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.ID, "full_commentary_btn"))
                    )
                    if load_more_btn.is_displayed():
                        driver.execute_script("arguments[0].click();", load_more_btn)
                        time.sleep(2)
                    else:
                        break
                except Exception:
                    break

            return driver.page_source

        except (TimeoutException, WebDriverException) as e:
            logger.warning(f"Scraping attempt {attempt} failed: {e}")
            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error during scraping: {e}")
            break
        finally:
            if driver:
                driver.quit()
                
    logger.error(f"Failed to fetch {url} after {retries} attempts.")
    return None

def save_file_s3(client, data, bucket, key):
    """Saves string or dict data to S3."""
    if key.endswith(".json") and not isinstance(data, str):
        file_data = json.dumps(data, indent=4)
        content_type = "application/json"
    else:
        file_data = data
        content_type = "text/plain" if key.endswith(".csv") else "binary/octet-stream"

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=file_data,
            ContentType=content_type
        )
        logger.info(f"Successfully uploaded to s3://{bucket}/{key}")
    except Exception as e:
        logger.error(f"Failed to upload {key} to S3: \n{e}")

def read_file_s3(client, bucket, key):
    """Reads raw string data from S3."""
    try:
        logger.info(f"Loading data from s3://{bucket}/{key}")
        response = client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        logger.error(f"Error reading '{key}' from S3: {e}")
        return None

def read_json_s3(client, bucket, key):
    """Reads and parses JSON data from S3."""
    data = read_file_s3(client, bucket, key)
    if data:
        return json.loads(data)
    return None

def list_csv_s3(client, bucket, prefix):
    """Lists all CSV files in a given S3 bucket prefix."""
    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.csv'):
                    files.append(obj['Key'])
        return files
    except Exception as e:
        logger.error(f"Error listing objects in s3://{bucket}/{prefix}: {e}")
        return []

def list_json_s3(client, bucket, prefix):
    """Lists all JSON files in a given S3 bucket prefix."""
    try:
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    files.append(obj['Key'])
        return files
    except Exception as e:
        logger.error(f"Error listing JSON objects in s3://{bucket}/{prefix}: {e}")
        return []
