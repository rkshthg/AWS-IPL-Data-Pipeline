import json
import os
import io
import boto3
import pandas as pd
import logging
from datetime import datetime

# from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from pipeline_2026.utils import (
        list_csv_s3,
        read_file_s3,
        save_file_s3,
        get_s3_client,
        get_glue_client,
        logger,
        S3_RAW,
        S3_BRONZE
    )
except ImportError:
    from utils import (
        list_csv_s3,
        read_file_s3,
        save_file_s3,
        get_s3_client,
        get_glue_client,
        logger,
        S3_RAW,
        S3_BRONZE
    )

# Load environment variables
load_dotenv()

# AWS Configuration
DATA_PREFIX = os.getenv("DATA_PREFIX")
MATCH_PREFIX = os.getenv("MATCH_PREFIX")

def extract_runs(df):
    try:        
        run_map = {
            'no run': 0,
            '1 run': 1,
            '2 runs': 2,
            '3 runs': 3,
            'four': 4,
            '5 runs': 5,
            'six': 6
        }
        extras_map = {
            'wide': 1,
            'no ball': 1,
            'no-ball': 1,
            'byes': 0,
            'leg byes': 0,
            'leg-byes': 0
        }

        runs = 0
        extra_runs = 0
        extra = 0
        extra_type = "N/A"
        rebowl = 0
        wicket = 0
        wicket_method = "Not Out"
        out_batsman = "N/A"

        ball_event = df['ball_event'].lower()
        event_info = df['event_info'].lower()

        runs = run_map.get(ball_event, 0)

        if ball_event in extras_map.keys():
            extra = 1
            extra_runs = extras_map[ball_event]
            rebowl = extras_map[ball_event]
            extra_type = ball_event
            extra_event = event_info.split(';')[0]
            if extra_event in run_map.keys():
                runs = run_map.get(extra_event, 0)

        elif ball_event == '5 wides':
            runs = 4
            extra_runs = 1
            extra = 1
            extra_type = 'wide'
            rebowl = 1

        elif ball_event.startswith('out'):
            wicket = 1
            if 'Run Out!' in ball_event:
                wicket_method = 'Run Out'
                out_batsman = ball_event.split('out ')[-1].split(' Run Out')[0]
            else:
                wicket_method = ball_event.split(' ')[1]
                out_batsman = df['batsman']

        data = {
            'match': df['match'],
            'date': df['date'],
            'time': df['time'],
            'over': df['over'],
            'ball': df['ball'],
            'bowler': df['bowler'],
            'batsman': df['batsman'],
            'runs': runs,
            'extra_runs': extra_runs,
            'extra': extra,
            'extra_type': extra_type,
            'rebowl': rebowl,
            'wicket': wicket,
            'wicket_method': wicket_method,
            'out_batsman': out_batsman,
            'total_runs': runs + extra_runs
        }

        return data

    except Exception as e:
        logger.error(f"Extraction failed: {e}")

def get_innings(row, final):
    """
    Calculates the current innings, score, and fallen wickets based on the match progression.
    """
    row_dict = row.to_dict()
    
    if not final:
        row_dict['innings'] = 1
        row_dict['score'] = row_dict['total_runs']
        row_dict['fallen_wickets'] = row_dict['wicket']
    else:
        prev_ball_innings = int(final[-1]['innings'])
        
        # New innings detection
        if row_dict['over'] == 0 and row_dict['ball'] == 1 and final[-1]['rebowl'] == 0:
            row_dict['innings'] = prev_ball_innings + 1 
        else: 
            row_dict['innings'] = prev_ball_innings        

        # Update running score and wickets
        if row_dict['innings'] == prev_ball_innings:
            row_dict['score'] = final[-1]['score'] + row_dict['total_runs']
            row_dict['fallen_wickets'] = final[-1]['fallen_wickets'] + row_dict['wicket']
        else:
            row_dict['score'] = 0
            row_dict['fallen_wickets'] = 0

    return row_dict

def lambda_handler(event, context):
    try:
        s3_client = get_s3_client()
        for record in event.get('Records', []):
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']

            logger.info(f"Processing s3://{bucket}/{key}")
            
            if not key.endswith('.csv'):
                logger.info(f"Skipping non-CSV file: {key}")
                continue

            # Assuming the key format is like data/raw/match/52_RRvsGT/52_RRvsGT-20-21-39.csv
            match = key.split('/')[-2]

            try:
                csv_data = read_file_s3(s3_client, bucket, key)
                data_as_file = io.StringIO(csv_data)
                df = pd.read_csv(data_as_file) 

                extract_data = []

                for index, row in df.iterrows():
                    extract_data.append(extract_runs(row))

                extract_df = pd.DataFrame(extract_data)
                extract_df.drop_duplicates().reset_index(drop=True, inplace=True)

                final = []
                for index, row in extract_df.iterrows():
                    data = get_innings(row, final)
                    final.append(data)

                new_df = pd.DataFrame(final)

                brnz_key = f"{MATCH_PREFIX}{match}/{match}_brnz.json"

                try:
                    s3_client.head_object(Bucket=S3_BRONZE, Key=brnz_key)
                    brnz_exists = True
                except Exception as e:
                    logger.info(f"File does not exist in S3: {e}")
                    brnz_exists = False
            
                if brnz_exists:
                    logger.info(f"Appending new data to existing datafile...")
                    bronze = read_file_s3(s3_client, S3_BRONZE, brnz_key)
                    bronze_df = pd.read_json(io.StringIO(bronze), lines=True)

                    bronze_df = pd.concat([bronze_df, new_df], ignore_index=True)
                    bronze_df.drop_duplicates(inplace=True)
                    bronze_df.reset_index(drop=True, inplace=True)

                    json_buffer = io.StringIO()
                    bronze_df.to_json(json_buffer, orient='records', lines=True)

                    save_file_s3(s3_client, json_buffer.getvalue(), S3_BRONZE, brnz_key)

                else:
                    logger.info(f"Creating new data file...")

                    json_buffer = io.StringIO()
                    new_df.to_json(json_buffer, orient='records', lines=True)

                    save_file_s3(s3_client, json_buffer.getvalue(), S3_BRONZE, brnz_key)

                # Trigger AWS Glue Silver Job automatically for the updated bronze key
                try:
                    glue_client = get_glue_client()
                    glue_job_name = os.getenv("GLUE_JOB_NAME", "ipl_silver_deliveries_job")
                    logger.info(f"Triggering Glue Job '{glue_job_name}' for key: {brnz_key}...")
                    glue_client.start_job_run(
                        JobName=glue_job_name,
                        Arguments={
                            '--s3_key': brnz_key,
                            '--match': match
                        }
                    )
                except Exception as glue_err:
                    logger.warning(f"Could not trigger Glue Job '{glue_job_name}': {glue_err}")

            except Exception as e:
                logger.error(f"Data Extraction Failed: {e}")
                raise e

    except Exception as e:
        logger.error(f"Lambda failed: {e}")
        raise e