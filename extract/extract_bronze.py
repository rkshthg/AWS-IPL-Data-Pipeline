import re
import json
import os
import boto3
import hashlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

S3_BUCKET = os.environ.get("S3_BUCKET")
RAW_PREFIX  = os.environ.get("RAW_PREFIX")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX")

session = boto3.Session()
s3 = session.client("s3")

def read_json(dir, filename):
    key = f"{RAW_PREFIX}{dir}/{filename}.json"     
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    json_data = response['Body'].read().decode('utf-8')
    return json.loads(json_data)

def save_json(data, name, prefix=BRONZE_PREFIX):
    json_data = json.dumps(data, indent=4)
    filename = f"{name}.json"
    s3_key = f"{prefix}ball-by-ball/{filename}"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        logger.info(f"Successfully uploaded {filename} to s3://{S3_BUCKET}ball-by-ball/{s3_key}")
    except Exception as e:
        logger.info(f"Failed to upload {filename} to S3: {e}")

def generate_id(keys):
    data_string = "-".join(str(key) for key in keys)
    # Generate SHA-256 hash
    id = hashlib.sha256(data_string.encode()).hexdigest()
    return id

def bat_bowl_team(innings, metadata):
    team1 = metadata["match"].split("vs")[0].strip()
    team2 = metadata["match"].split("vs")[-1].strip().split(",")[0]
    if metadata["toss_winner"] == team1:
        if metadata["toss_decision"] == "bat":
            batting_team = team1
            bowling_team = team2
        else:
            batting_team = team2
            bowling_team = team1
    else:
        if metadata["toss_decision"] == "bat":
            batting_team = team2
            bowling_team = team1
        else:
            batting_team = team1
            bowling_team = team2
    if innings == 2:
        batting_team, bowling_team = bowling_team, batting_team
    return batting_team, bowling_team

def extract_data(new_ball, metadata, last_ball):
    if last_ball == None:
        innings = 1
        score = 0
        wickets = 0
        target = 0
    else:
        if int(last_ball["over"]) - int(new_ball["over"]) > 5:
            innings = 2
            score = 0
            wickets = 0
            target = last_ball["current_score"]
        else:
            innings = last_ball["innings"]
            score = last_ball["current_score"]
            wickets = last_ball["current_wickets"]
            target = last_ball["target"]
    batting_team, bowling_team = bat_bowl_team(innings, metadata)
    info = new_ball["event_info"]
    batsman = info.split(", ")[0].split(" to ")[1]
    bowler = info.split(" to ")[0]
    event_info = info.split(", ", 1)[1]

    runs = 0
    extra_runs = 0
    extra = 0
    extra_type = "N/A"
    rebowl = 0
    wicket = 0
    wicket_method = "Not Out"
    out_batsman = "N/A"
    valid_ball = 1
    
    if event_info.startswith("SIX"): 
        runs = 6 
    elif event_info.startswith("FOUR"): 
        runs = 4
    elif event_info.startswith("no run"): 
        runs = 0
    elif event_info.startswith("out"): 
        runs = 0
        wicket = 1
        if event_info.split(" ", 1)[1].split("!")[0].split(" by")[0].endswith("Run Out"):
            wicket_method = "Run Out"
            out_batsman = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            if event_info.split("! ")[1].split(" ")[0].isdigit():
                if int(event_info.split("! ")[1].split(" ")[0]) > 5:
                    runs = 0
                else:
                    runs = int(event_info.split("! ")[1].split(" ")[0]) 
            else:
                runs = 0
        else: 
            wicket_method = event_info.split(" ", 1)[1].split("!")[0].split(" by")[0]
            out_batsman = batsman
            runs = int(event_info.split("! ")[1].split(" ")[0]) if event_info.split("! ")[1].split(" ")[0].isdigit() else 0
    elif event_info.startswith("wide"): 
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.startswith("out"): 
            wicket = 1
            wicket_method = "Run Out"
            out_batsman = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            if event_info.split("! ")[1].split(" ")[0].isdigit():
                if int(event_info.split("! ")[1].split(" ")[0]) > 5:
                    runs = 0
                else:
                    runs = int(event_info.split("! ")[1].split(" ")[0]) 
            else:
                runs = 0
        else:
            runs = int(re.search("[0-9]", event_info.split(", ")[0])) if event_info.split(", ")[0].isdigit() else 0
        extra = 1
        extra_runs = 1
        extra_type = "wide" 
        valid_ball = 0
        rebowl = 1
    elif event_info.startswith("no ball"): 
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.startswith("out"): 
            wicket = 1
            wicket_method = "Run Out"
            out_batsman = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            if event_info.split("! ")[1].split(" ")[0].isdigit():
                if int(event_info.split("! ")[1].split(" ")[0]) > 5:
                    runs = 0
                else:
                    runs = int(event_info.split("! ")[1].split(" ")[0]) 
            else:
                runs = 0
        else:
            runs = int(event_info.split(", ", 1)[-1].split(" ")[0]) if event_info.split(", ", 1)[-1].split(" ")[0].isdigit() else 0
        extra = 1
        extra_runs = 1
        extra_type = "no ball"
        valid_ball = 0
        rebowl = 1
    elif event_info.startswith("leg byes") or event_info.startswith("Leg byes"):  
        logger.info(event_info.split(", ", 1)[1].split(" ")[0])
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.split(", ", 1)[1].split(" ")[0] == "":
            runs = int(event_info.split(", ", 1)[1].split(" ")[1]) 
        else:
            runs = int(event_info.split(", ", 1)[1].split(" ")[0]) 
        extra = 1
        extra_type = "leg byes"
    elif event_info.startswith("byes"): 
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.split(", ", 1)[1].split(" ")[0] == "":
            runs = int(event_info.split(", ", 1)[1].split(" ")[1]) 
        else:
            runs = int(event_info.split(", ", 1)[1].split(" ")[0]) 
        extra = 1
        extra_type = "byes"
    elif event_info.split(" ")[0].isdigit(): 
        runs = int(event_info.split(" ")[0])
    else:
        words = info.split(" ")
        for word in words:
            if word.isdigit():
                runs = int(word)
                break
            elif word == "FOUR":
                runs = 4
                break
            elif word == "SIX":
                runs = 6
                break
            else:
                runs = 0
                break

    if wicket == 1:
        wickets += 1

    score = score + int(runs) + int(extra_runs)

    return {
        "match": metadata["match"],
        "date": metadata["date"],
        "venue": metadata["venue"],
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "ball_id": generate_id([new_ball["date"], innings, new_ball["over"], new_ball["ball"], new_ball["event_info"]]),
        "innings": innings,
        "over": new_ball["over"],
        "ball": new_ball["ball"],
        "batsman": batsman,
        "bowler": bowler,
        "event_info": new_ball["event_info"][:100],
        "batter_runs": int(runs),
        "extra_runs": int(extra_runs),
        "runs_from_ball": int(runs) + int(extra_runs),
        "extra": extra,
        "extra_type": extra_type,
        "rebowl": rebowl,
        "wicket": wicket,
        "wicket_method": wicket_method,
        "out_batsman": out_batsman,
        "valid_ball": valid_ball,
        "current_score": score,
        "current_wickets": wickets,
        "target": target #if innings == 2 else 0
    }
     
def lambda_handler(event, context):
    for record in event['Records']:
        object_key = record['s3']['object']['key']
        logger.info(f"New object uploaded: {object_key}")

        if object_key.endswith('.json'):
           match = str(object_key).split("/")[-1].split(".")[0]
        metadata = read_json("metadata", match)
        balls_bowled = read_json("ball-by-ball", match)
        logger.info(f"Balls bowled in match: {len(balls_bowled)}")
        
        data = []
        for i in range(0, len(balls_bowled)):
            new_ball = balls_bowled[i]
            logger.info(new_ball["event_info"])
            last_ball = data[-1] if i > 0 else None
            logger.info(last_ball)
            data.append(extract_data(new_ball, metadata, last_ball))
        save_json(data, match)

    return {"statusCode": 200, "body": json.dumps("Ball data extraction completed.")}