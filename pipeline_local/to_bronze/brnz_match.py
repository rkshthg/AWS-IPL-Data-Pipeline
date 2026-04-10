import json
import os
import io
import boto3
import pandas as pd
import logging
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s] %(filename)s:%(lineno)d --- %(message)s")
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# AWS Configuration
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")  # AWS access key for S3 authentication
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")  # AWS secret key for S3 authentication
S3_RAW = os.getenv("S3_RAW")            # Target S3 bucket name
S3_BRONZE = os.getenv("S3_BRONZE")
DATA_PREFIX = os.getenv("DATA_PREFIX")
MATCH_PREFIX = os.getenv("MATCH_PREFIX")

def list_csv(client, bucket, path, prefix=MATCH_PREFIX):
    prefix_path = f"{prefix}{path}/"
    print(prefix_path)
    try:
        files = []
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix_path)

        if 'Contents' not in response:
            print("No objects found.")
            return files
        
        else:
            for obj in response.get('Contents', []):
                if obj['Key'].endswith('.csv'):
                    files.append(obj['Key'].split("/")[-1])

            return files
        
    except Exception as e:
        print(f"Error listing objects: {e}")
        return None

def read_file_s3(client, bucket, path, name, prefix=DATA_PREFIX):
    try:
        key = f"{prefix}{path}/{name}"
        logger.info(f"Loading data from {key}...")

        response = client.get_object(Bucket=bucket, Key=key)
        logger.info("Retreiving data from S3...")

        if name.endswith(".json"):
            return json.loads(response['Body'].read().decode('utf-8'))
        else:
            return response['Body'].read().decode('utf-8')
    
    except Exception as e:
        logger.error(f"Error reading '{name}' from {key}: {e}")
        return None

def save_json_s3(client, bucket, data, prefix, name):
    json_data = json.dumps(data, indent=4)
    filename = f"{name}.json"
    key = f"{prefix}/{filename}"

    try:
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded {filename} to s3://{bucket}/{key}")
        
    except Exception as e:
        logger.error(f"Failed to upload {filename} to S3: \n{e}")

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

        data = {}
        data['match'] = df['match']
        data['data'] = df['date']
        data['time'] = df['time']
        data['over'] = df['over']
        data['ball'] = df['ball']
        data['bowler'] = df['bowler']
        data['batsman'] = df['batsman']
        data['runs'] = runs
        data['extra_runs'] = extra_runs
        data['extra'] = extra
        data['extra_type'] = extra_type
        data['rebowl'] = rebowl
        data['wicket'] = wicket
        data['wicket_method'] = wicket_method
        data['out_batsman'] = out_batsman
        data['total_runs'] = runs + extra_runs

        return data

    except Exception as e:
        logger.error(f"Extraction failed: {e}")

def get_innings(row, final):
    row_dict = row.to_dict()

    now = str(datetime.now())
    
    if final == []:
        row_dict['innings'] = 1
        row_dict['score'] = row_dict['total_runs']
        row_dict['fallen_wickets'] = row_dict['wicket']

    else:
        prev_ball_innings = int(final[-1]['innings'])
        if row_dict['over'] == 0 and row_dict['ball'] == 1:
            if final[-1]['rebowl'] == 0:
                row_dict['innings'] = prev_ball_innings + 1 
                row_dict['score'] = 0
                row_dict['fallen_wickets'] = 0  
            else: row_dict['innings'] = prev_ball_innings
        else: row_dict['innings'] = prev_ball_innings        

        if row_dict['innings'] == prev_ball_innings:
            row_dict['score'] = final[-1]['score'] + row_dict['total_runs']
            row_dict['fallen_wickets'] = final[-1]['fallen_wickets'] + row_dict['wicket']
        else:
            row_dict['score'] = 0
            row_dict['fallen_wickets'] = 0

    return row_dict

def main():
    try:
        s3_client = boto3.client(
            "s3",
            aws_access_key_id=AWS_ACCESS_KEY,
            aws_secret_access_key=AWS_SECRET_KEY
        )

        now = datetime.now()
        date = str(datetime.strftime(now, "%B"))
        date += " " + str(now.day)
        hour = now.hour

        fixtures = read_file_s3(s3_client, S3_RAW, "fixtures", "fixtures.json")
        fixtures_df = pd.DataFrame(fixtures)

        match_row = fixtures_df[fixtures_df['date'] == date]

        if len(match_row)>1:
            if hour >= 19:
                match_row = match_row.iloc[1]
                match = match_row['match_short'].item()
            elif hour >= 15:
                match_row = match_row.iloc[0]
                match = match_row['match_short'].item()

        else:
            match = match_row['match_short'].item()

        csv_files = list_csv(s3_client, S3_RAW, match)

        file = csv_files[-1]

        try:
            csv_data = read_file_s3(s3_client, S3_RAW, match, file, MATCH_PREFIX)

            data_as_file = io.StringIO(csv_data)
            df = pd.read_csv(data_as_file) 

            extract_data = []

            for index, row in df.iterrows():
                # print(f"Extracting for: {row['over']}.{row['ball']}")
                extract_data.append(extract_runs(row))

            extract_df = pd.DataFrame(extract_data)
            extract_df.drop_duplicates().reset_index(drop=True, inplace=True)

            final = []
            for index, row in extract_df.iterrows():
                data = get_innings(row, final)
                final.append(data)

            new_df = pd.DataFrame(final)

            try:
                s3_client.head_object(Bucket=S3_BRONZE, Key=f"{MATCH_PREFIX}{match}/{match}_brnz.json")
                brnz_exists = True
            except Exception as e:
                logger.info(f"File does not exist in S3: {e}")
                brnz_exists = False
        
            if brnz_exists:
                logger.info(f"Appending new data to existing datafile...")
                bronze = read_file_s3(s3_client, S3_BRONZE, match, f"{match}_brnz.json", MATCH_PREFIX)
                bronze_df = pd.read_json(io.StringIO(bronze), lines=True)

                bronze_df = pd.concat([bronze_df, new_df], ignore_index=True)
                bronze_df.drop_duplicates(inplace=True)
                bronze_df.reset_index(drop=True, inplace=True)

                json_buffer = io.StringIO()
                new_df.to_json(json_buffer, orient='records', lines=True)

                save_json_s3(s3_client, S3_BRONZE, json_buffer.getvalue(), f"{MATCH_PREFIX}{match}", f"{match}_brnz")
                # s3_resource.Object(S3_BUCKET, f"{BRONZE_PREFIX}{match}/{match}_brnz.json").put(Body=json_buffer.getvalue())

            else:
                logger.info(f"Creating new data file...")

                json_buffer = io.StringIO()
                new_df.to_json(json_buffer, orient='records', lines=True)

                save_json_s3(s3_client, S3_BRONZE, json_buffer.getvalue(), f"{MATCH_PREFIX}{match}", f"{match}_brnz")
                # s3_resource.Object(S3_BUCKET, f"{BRONZE_PREFIX}{match}/{match}_brnz.json").put(Body=json_buffer.getvalue())

        except Exception as e:
            logger.error(f"Data Extraction Failed: {e}")

    except Exception as e:
        logger.error(f"Main failed: {e}")

if __name__ == "__main__":
    main()