import os
import json
import logging
import time
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

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

def save_json_local(data, prefix, filename, layer="raw"):
    """Saves data as a JSON file in the specified layer directory."""
    if not filename.endswith(".json"):
        filename += ".json"
    file_data = data if isinstance(data, str) else json.dumps(data, indent=4)
    local_path = f"data/{layer}/{prefix}{filename}"
    
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "w") as f:
            f.write(file_data)
        logger.info(f"Successfully saved {filename} to {local_path}")
    except Exception as e:
        logger.error(f"Failed to save {filename} locally: {e}")

def save_file_local(data, prefix, filename, layer="raw"):
    """Saves string data to a file in the specified layer directory."""
    local_path = f"data/{layer}/{prefix}{filename}"
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        if filename.endswith(".json") and not isinstance(data, str):
            data = json.dumps(data, indent=4)
        with open(local_path, "w") as f:
            f.write(data)
        logger.info(f"Successfully saved {filename} to {local_path}")
    except Exception as e:
        logger.error(f"Failed to save {filename} locally: {e}")

def read_json_local(prefix, filename, layer="raw"):
    """Reads a JSON file from the specified layer directory."""
    if not filename.endswith(".json"):
        filename += ".json"
    local_path = f"data/{layer}/{prefix}{filename}"
    try:
        with open(local_path, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"File {local_path} not found.")
        return None
    except Exception as e:
        logger.error(f"Failed to read {local_path}: {e}")
        return None

def read_file_local(prefix, filename, layer="raw"):
    """Reads a file's raw content from the specified layer directory."""
    local_path = f"data/{layer}/{prefix}{filename}"
    try:
        with open(local_path, "r") as f:
            return f.read()
    except FileNotFoundError:
        logger.warning(f"File {local_path} not found.")
        return None
    except Exception as e:
        logger.error(f"Failed to read {local_path}: {e}")
        return None

def list_csv(match, layer="raw", suffix="_data.csv"):
    """Lists CSV files matching a suffix in the specified match directory."""
    path = f"data/{layer}/{match}/"
    try:
        return [f for f in os.listdir(path) if f.endswith(suffix)]
    except Exception as e:
        logger.error(f"Error listing objects in {path}: {e}")
        return []
