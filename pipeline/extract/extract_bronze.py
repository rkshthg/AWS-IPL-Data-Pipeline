import re
import json
import os
import boto3
from rapidfuzz import process, fuzz
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

def list_json(path, prefix=BRONZE_PREFIX):
    prefix_path = f"{prefix}{path}/"
    try:
        response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix_path)
        if 'Contents' not in response:
            print("No objects found.")
            return None
        return [str(obj['Key']).split("/")[-1] for obj in response['Contents']]
    except Exception as e:
        print(f"Error listing objects: {e}")
        return None

def read_json(dir, filename, prefix=RAW_PREFIX):
    key = f"{prefix}{dir}/{filename}.json"
    response = s3.get_object(Bucket=S3_BUCKET, Key=key)
    # json_data = response['Body'].read().decode('utf-8')
    return json.loads(response['Body'].read().decode('utf-8'))

def read_players(name):
    try:
        key = f"{RAW_PREFIX}{name}/{name}.json"
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except Exception as e:
        logger.error(f"Error reading '{name}' from S3: {e}")
        return None

def save_json(data, name, prefix=BRONZE_PREFIX):
    json_data = "\n".join(json.dumps(record, indent=4) for record in data)
    filename = f"{name}.json"
    path = "results" if str(name).endswith("results") else "ball-by-ball"
    s3_key = f"{prefix}{path}/{filename}"
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_key,
            Body=json_data,
            ContentType="application/json"
        )
        print(f"Successfully uploaded {filename} to s3://{S3_BUCKET}{path}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {filename} to S3: {e}")

def generate_id(keys):
    data_string = "-".join(str(key) for key in keys)
    # Generate SHA-256 hash
    id = hashlib.sha256(data_string.encode()).hexdigest()
    return id

def bat_bowl_team(innings, metadata):
    team1 = metadata["home_team"]
    team2 = metadata["away_team"]
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
    if innings == 2 or innings == 3:
        return bowling_team, batting_team
    else:
        return batting_team, bowling_team

def get_innings(last_ball, new_ball):
    if last_ball == None:
        innings = 1
        score = 0
        wickets = 0
        target = 0
    else:
        if last_ball["innings"] == 1 and (int(last_ball["over"]) - int(new_ball["over"]) > 5):
            innings = 2
            score = 0
            wickets = 0
            target = last_ball["current_score"]
        elif last_ball["innings"] == 2 and (int(last_ball["over"]) - int(new_ball["over"]) > 5):
            innings = 3
            score = 0
            wickets = 0
            target = 0
        elif last_ball["innings"] == 3 and (int(last_ball["ball"]) - int(new_ball["ball"]) > 3):
            innings = 4
            score = 0
            wickets = 0
            target = last_ball["current_score"]
        else:
            innings = last_ball["innings"]
            score = last_ball["current_score"]
            wickets = last_ball["current_wickets"]
            target = last_ball["target"]
    return innings, score, wickets, target

def correct_name(raw_name, reference_names, threshold=85):
    match = process.extractOne(raw_name, reference_names, scorer=fuzz.ratio)
    if match and match[1] >= threshold:
        return match[0]
    return None  # more reliable than returning the raw name

def get_player_name(team, name, metadata):
    squad = home_squad if team == metadata["home_team"] else (
        away_squad if team == metadata["away_team"] else []
    )
    for player in squad:
        if name.lower() in player.lower():
            return player
    # Try fuzzy match with full name
    match = correct_name(name, squad, threshold=85)
    if match:
        return match
    # Try fuzzy match on just first name
    first_name = name.split(" ")[0]
    match = correct_name(first_name, squad, threshold=90)
    if match:
        return match
    return None

def extract_data(new_ball, metadata, last_ball):
    innings, score, wickets, target = get_innings(last_ball, new_ball)
    batting_team, bowling_team = bat_bowl_team(innings, metadata)
    info = new_ball["event_info"]

    batsman_tag = info.split(", ")[0].split(" to ")[1]
    batsman = get_player_name(batting_team, batsman_tag, metadata)
    if batsman == None:
        batsman = batsman_tag

    bowler_tag = info.split(" to ")[0]
    bowler = get_player_name(bowling_team, bowler_tag, metadata)
    if bowler == None:
        bowler = bowler_tag

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
    elif event_info.startswith("wide"): 
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.split(", ", 1)[-1].startswith("out"): 
            wicket = 1
            wicket_method = "Run Out"
            dismissed = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            out_batsman = get_player_name(batting_team, dismissed, metadata)
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
        valid_ball = 1 if event_info.split(", ", 1)[-1].startswith("out") else 0
        rebowl = 1
    elif event_info.startswith("no ball") or event_info.startswith("no-ball"): 
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR" or event_info.split(", ", 1)[-1].startswith("FOUR"):
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX" or event_info.split(", ", 1)[-1].startswith("SIX"):
            runs = 6
        elif event_info.split(", ", 1)[-1].startswith("out"): 
            wicket = 1
            wicket_method = "Run Out"
            dismissed = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            out_batsman = get_player_name(batting_team, dismissed, metadata)
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
        valid_ball = 1 if event_info.split(", ", 1)[-1].startswith("out") else 0
        rebowl = 1
    elif event_info.startswith("leg byes") or event_info.startswith("Leg byes"):  
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
        if event_info.split(", ", 1)[-1].split(", ")[0] == "FOUR" or event_info.split("leg byes ", 1)[-1].split(", ")[0] == "FOUR":
            runs = 4
        elif event_info.split(", ", 1)[-1].split(", ")[0] == "SIX":
            runs = 6
        elif event_info.split(", ", 1)[1].split(" ")[0] == "":
            runs = int(event_info.split(", ", 1)[1].split(" ")[1]) 
        else:
            runs = int(event_info.split(", ", 1)[1].split(" ")[0]) 
        extra = 1
        extra_type = "byes"
    elif event_info.startswith("out"): 
        runs = 0
        wicket = 1
        if event_info.split(" ", 1)[1].split("!")[0].split(" by")[0].endswith("Run Out" or "run-out"):
            wicket_method = "Run Out"
            dismissed = event_info.split(" ", 1)[1].split("!")[0].split(" Run Out")[0].strip()
            out_batsman = get_player_name(batting_team, dismissed, metadata)
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
    elif event_info.startswith("run-out") and innings > 2:
        wicket_method = "Run Out"
        wicket = 1
        out_batsman = "N/A"
        if event_info.split("! ")[1].startswith("No run"):
            runs = 0
        elif event_info.split("! ")[1].startswith("One"):
            runs = 1
        elif event_info.split("! ")[1].startswith("Two"):
            runs = 2
        elif event_info.split("! ")[1].split(" ")[0].isdigit():
            if int(event_info.split("! ")[1].split(" ")[0]) > 5:
                runs = 0
            else:
                runs = int(event_info.split("! ")[1].split(" ")[0]) 
        else: runs = 0
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

    info = new_ball["event_info"].replace("! ", ", ").split(", ", 2)[1]

    return {
        "match": metadata["match"],
        "short_name": metadata["short_name"],
        "match_id": generate_id([metadata["short_name"]]),
        "date": metadata["date"],
        "time": metadata["time"],
        "venue": metadata["venue"],
        "batting_team": batting_team,
        "bowling_team": bowling_team,
        "ball_id": generate_id([innings, new_ball["over"], new_ball["ball"], info[:50]]),
        "innings": innings,
        "over": int(new_ball["over"]),
        "ball": int(new_ball["ball"]),
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
        "target": target
    }

def match_results(data):
    fixtures = read_json("fixtures", "fixtures")
    for fixture in fixtures:
        if fixture["short_name"] == data["short_name"]:
            if '58-' in fixture["short_name"]:
                winner = "NA"
                result = "Suspended"
            if data["innings"] == 1:
                    winner = "NA"
                    result = "Match abandoned"
            if data["innings"] == 2:
                if data["over"]<6:
                    winner = "NA"
                    result = "Match abandoned"
                else:
                    if data["current_score"] > data["target"]:
                        winner = data["batting_team"]
                        diff = 10 - data["current_wickets"]
                        result = f"won by {diff} wickets"
                    elif data["current_score"] < data["target"]:
                        winner = data["bowling_team"]
                        diff = data["target"] - data["current_score"]
                        result = f"won by {diff} runs"
                    else:
                        winner = "NA"
                        result = "NA"
            if data["innings"] == 4:
                if data["current_score"] > data["target"]:
                    winner = data["batting_team"]
                    result = "won the Super Over"
                elif data["current_score"] < data["target"]:
                    winner = data["bowling_team"]
                    result = "won the Super Over"
                else:
                    winner = "NA"
                    result = "NA"
            break

    return {
        "match": data["match"],
        "short_name": data["short_name"],
        "match_id": data["match_id"],
        "date": data["date"],
        "time": data["time"],
        "venue": data["venue"],
        "home_team": data["match"].split(" vs ")[0],
        "away_team": data["match"].split(" vs ")[1].split(",")[0],
        "winner": winner,
        "result": result
    }
    
def lambda_handler(event, context):
    global home_squad
    global away_squad
    global metadata
    for record in event['Records']:
        object_key = record['s3']['object']['key']
        logger.info(f"New object uploaded: {object_key}")

        if object_key.endswith('.json'):
           match = str(object_key).split("/")[-1].split(".")[0]
        metadata = read_json("metadata", match)

        home_squad = away_squad = []
        players = read_players("players")
        for player in players:
            if player["team"] == metadata["home_team"]:
                home_squad.append(player["name"])
            elif player["team"] == metadata["away_team"]:
                away_squad.append(player["name"])
            else: continue

        balls_bowled = read_json("ball-by-ball", match)
        print("Balls bowled in match: ", len(balls_bowled))

        data = []
        for i in range(0, len(balls_bowled)):
            new_ball = balls_bowled[i]
            logger.info(new_ball["event_info"])
            last_ball = data[-1] if i > 0 else None
            logger.info(last_ball)
            data.append(extract_data(new_ball, metadata, last_ball))
        save_json(data, match)

        result = []
        try:
            result.append(match_results(data[-1]))
        except Exception as e:
            logger.error(f"Error processing match results: {e}")
            result.append({
                "match": metadata["match"],
                "short_name": metadata["short_name"],
                "match_id": generate_id(metadata["short_name"]),
                "date": metadata["date"],
                "time": metadata["time"],
                "venue": metadata["venue"],
                "home_team": metadata["match"].split(" vs ")[0],
                "away_team": metadata["match"].split(" vs ")[1].split(",")[0],
                "winner": "NA",
                "result": "Match abandoned"
            })
        save_json(result, f"{match}-results")

    return {"statusCode": 200, "body": json.dumps("Ball data extraction completed.")}