import json
import os
import io
import sys
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

from dotenv import load_dotenv

import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pipeline_local.utils import list_csv, read_file_local, save_json_local, logger

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
    row_dict = row.to_dict()

    now = str(datetime.now())
    
    if not final:
        row_dict['innings'] = 1
        row_dict['score'] = row_dict['total_runs']
        row_dict['fallen_wickets'] = row_dict['wicket']

    else:
        prev_ball_innings = int(final[-1]['innings'])
        if row_dict['over'] == 0 and row_dict['ball'] == 1 and final[-1]['rebowl'] == 0:
            row_dict['innings'] = prev_ball_innings + 1 
            row_dict['score'] = 0
            row_dict['fallen_wickets'] = 0  
        else:
            row_dict['innings'] = prev_ball_innings        

        if row_dict['innings'] == prev_ball_innings:
            row_dict['score'] = final[-1]['score'] + row_dict['total_runs']
            row_dict['fallen_wickets'] = final[-1]['fallen_wickets'] + row_dict['wicket']
        else:
            row_dict['score'] = row_dict['total_runs']
            row_dict['fallen_wickets'] = row_dict['wicket']

    return row_dict

def main():
    try:
        raw_dir = "data/raw"
        if not os.path.exists(raw_dir):
            logger.error(f"Raw directory '{raw_dir}' does not exist.")
            return

        for match in os.listdir(raw_dir):
            match_path = os.path.join(raw_dir, match)
            if not os.path.isdir(match_path):
                continue
            
            csv_files = list_csv(match)
            if not csv_files:
                continue

            logger.info(f"Processing match {match}...")
            csv_files.sort()
            file = csv_files[-1]

            try:
                csv_data = read_file_local(f"{match}/", file)
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

                brnz_exists = os.path.exists(f"data/bronze/{match}/{match}_brnz.json")
            
                if brnz_exists:
                    logger.info(f"Appending new data to existing datafile for {match}...")
                    bronze = read_file_local(f"{match}/", f"{match}_brnz.json", layer="bronze")
                    if isinstance(bronze, str):
                        bronze_df = pd.read_json(io.StringIO(bronze), lines=True)
                    else:
                        bronze_df = pd.DataFrame(bronze)

                    bronze_df = pd.concat([bronze_df, new_df], ignore_index=True)
                    bronze_df.drop_duplicates(inplace=True)
                    bronze_df.reset_index(drop=True, inplace=True)

                    json_buffer = io.StringIO()
                    bronze_df.to_json(json_buffer, orient='records', lines=True)

                    save_json_local(json_buffer.getvalue(), f"{match}/", f"{match}_brnz", layer="bronze")

                else:
                    logger.info(f"Creating new data file for {match}...")
                    json_buffer = io.StringIO()
                    new_df.to_json(json_buffer, orient='records', lines=True)
                    save_json_local(json_buffer.getvalue(), f"{match}/", f"{match}_brnz", layer="bronze")

            except Exception as e:
                logger.error(f"Data Extraction Failed for {match}: {e}")

    except Exception as e:
        logger.error(f"Main failed: {e}")

if __name__ == "__main__":
    main()