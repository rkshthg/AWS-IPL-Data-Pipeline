"""
AWS Glue Job: Silver Layer Delivery ETL Transformation

This module transforms Bronze delivery-level JSON files into a standardized, enriched,
and partitioned Silver Delta Lake table on Amazon S3 (s3://<S3_SILVER>/deliveries).

Key Operations:
1. Ingestion: Reads Bronze delivery data and Raw match metadata from S3.
2. Team Derivation: Calculates `batting_team` and `bowling_team` based on toss winner, toss decision, and innings.
3. Team-Scoped Fuzzy Matching: Uses RapidFuzz restricted to playing team squads to normalize player names.
4. Feature Engineering: Computes over_decimal, innings_phase, dot_ball_pct, boundary flags, and legal delivery flags.
5. Delta Lake Storage: Writes partitioned Delta Lake tables with match-level predicate overwrites.
6. Downstream Trigger: Automatically triggers the Gold Glue Job upon successful execution.
"""
import sys
import os

import typing_extensions

# Compatibility patch for AWS Glue Python 3.9 typing_extensions
if not hasattr(typing_extensions, 'Buffer'):
    import abc
    class Buffer(abc.ABC):
        pass
    typing_extensions.Buffer = Buffer

import json
import io
import boto3
import pandas as pd
import pyarrow as pa
import logging
from datetime import datetime
from rapidfuzz import process
from deltalake import write_deltalake, DeltaTable

# Load environment variables if dotenv is installed (local dev)
# try:
#     from dotenv import load_dotenv
#     load_dotenv()
# except ImportError:
#     pass

# Logger setup
def setup_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s]\n%(filename)s:%(lineno)d --- %(message)s\n")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger("glue_silver_job")

# Attempt to load Glue job parameters into os.environ
try:
    from awsglue.utils import getResolvedOptions
    for param_name in ['S3_RAW', 'S3_BRONZE', 'S3_SILVER', 'AWS_REGION', 'DATA_PREFIX', 'MATCH_PREFIX']:
        try:
            opt = getResolvedOptions(sys.argv, [param_name])
            if opt.get(param_name):
                os.environ[param_name] = opt[param_name]
        except Exception:
            pass
except Exception:
    pass

# AWS Configurations
DATA_PREFIX = os.environ.get("DATA_PREFIX", "data/")
MATCH_PREFIX = os.environ.get("MATCH_PREFIX", "match/")
S3_RAW = os.environ.get("S3_RAW", "ipl-data-2026-raw")
S3_BRONZE = os.environ.get("S3_BRONZE", "ipl-data-2026-bronze")
S3_SILVER = os.environ.get("S3_SILVER", "ipl-data-2026-silver")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

def get_s3_client():
    aws_key = os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if aws_key and aws_secret:
        return boto3.client('s3', aws_access_key_id=aws_key, aws_secret_access_key=aws_secret, region_name=AWS_REGION)
    return boto3.client('s3', region_name=AWS_REGION)

def get_storage_options():
    """Builds storage options dict for Delta Lake S3 integration in AWS Glue / IAM Role environments."""
    storage_options = {
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_KEY")
    aws_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    if aws_key and aws_secret:
        storage_options["AWS_ACCESS_KEY_ID"] = aws_key
        storage_options["AWS_SECRET_ACCESS_KEY"] = aws_secret
        if aws_token:
            storage_options["AWS_SESSION_TOKEN"] = aws_token
        storage_options["AWS_REGION"] = aws_region
        return storage_options

    try:
        session = boto3.Session()
        credentials = session.get_credentials()
        if credentials:
            frozen = credentials.get_frozen_credentials()
            storage_options["AWS_ACCESS_KEY_ID"] = frozen.access_key
            storage_options["AWS_SECRET_ACCESS_KEY"] = frozen.secret_key
            if frozen.token:
                storage_options["AWS_SESSION_TOKEN"] = frozen.token
            storage_options["AWS_REGION"] = session.region_name or aws_region
            return storage_options
    except Exception as e:
        logger.warning(f"Could not resolve boto3 session credentials for Delta Lake: {e}")

    storage_options["AWS_REGION"] = aws_region
    return storage_options

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
        data_str = data.strip()
        if not data_str.startswith('[') and not data_str.startswith('{'):
            data_str = f"[{data_str}]"
        try:
            return json.loads(data_str)
        except Exception as e:
            logger.error(f"Failed to parse JSON for s3://{bucket}/{key}: {e}")
            return None
    return None

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

def load_player_database_s3(s3_client):
    """Loads player objects from S3 players.json, mapping team -> list of player names."""
    candidate_keys = [
        f"{DATA_PREFIX}players/players.json",
        f"{DATA_PREFIX}players.json",
        "players.json"
    ]
    
    players_data = None
    for key in candidate_keys:
        players_data = read_file_s3(s3_client, S3_RAW, key)
        if players_data:
            logger.info(f"Loaded master players list from s3://{S3_RAW}/{key}")
            break

    if not players_data:
        logger.warning(f"Could not load players.json from S3 bucket {S3_RAW}")
        return {}, []
    
    team_player_map = {}
    all_players = []
    for line in players_data.strip().split("\n"):
        line_clean = line.strip()
        if line_clean:
            try:
                p = json.loads(line_clean)
                name = p.get("Name")
                team = p.get("Team")
                if name:
                    all_players.append(name)
                    if team:
                        if team not in team_player_map:
                            team_player_map[team] = []
                        team_player_map[team].append(name)
            except Exception:
                continue

    return team_player_map, list(set(all_players))

def match_player_name(raw_name, player_choices, cutoff=75):
    """Normalizes player names using RapidFuzz against master players list."""
    if not raw_name or raw_name == "N/A" or pd.isna(raw_name):
        return "N/A"
    
    raw_name_clean = str(raw_name).strip()
    if not player_choices:
        return raw_name_clean

    match = process.extractOne(raw_name_clean, player_choices, score_cutoff=cutoff)
    if match:
        return match[0]
    return raw_name_clean

def get_batting_and_bowling_teams(match_meta, innings):
    """Determines batting_team and bowling_team based on toss_winner, toss_decision, and innings."""
    if not match_meta:
        return "N/A", "N/A"
    
    home = match_meta.get('home_team', 'N/A')
    away = match_meta.get('away_team', 'N/A')
    toss_winner = match_meta.get('toss_winner', 'N/A')
    toss_decision = str(match_meta.get('toss_decision', '')).lower()

    if home == 'N/A' or away == 'N/A':
        return "N/A", "N/A"

    toss_loser = away if toss_winner == home else home
    if toss_winner not in [home, away]:
        match = process.extractOne(toss_winner, [home, away])
        if match:
            toss_winner = match[0]
            toss_loser = away if toss_winner == home else home

    if 'bat' in toss_decision:
        inn1_batting = toss_winner
        inn1_bowling = toss_loser
    else:
        inn1_batting = toss_loser
        inn1_bowling = toss_winner

    try:
        inn_num = int(innings)
    except (ValueError, TypeError):
        inn_num = 1

    if inn_num % 2 == 1:
        return inn1_batting, inn1_bowling
    else:
        return inn1_bowling, inn1_batting

def get_team_player_choices(team_name, team_player_map, all_players):
    """Returns player choices restricted to team_name if available, else all_players."""
    if not team_name or team_name == 'N/A' or not team_player_map:
        return all_players
    if team_name in team_player_map:
        return team_player_map[team_name]
    
    match = process.extractOne(team_name, list(team_player_map.keys()), score_cutoff=70)
    if match:
        return team_player_map[match[0]]
    return all_players

def get_innings_phase(over):
    """Categorizes delivery into innings phases."""
    try:
        over_num = int(over)
        if over_num < 6:
            return "Powerplay"
        elif over_num < 15:
            return "Middle Overs"
        elif over_num < 20:
            return "Death Overs"
        else:
            return "Super Over"
    except (ValueError, TypeError):
        return "Unknown"

def to_arrow_source(df):
    """Converts Pandas DataFrame into a PyArrow RecordBatchReader stream guaranteed to work with Rust deltalake."""
    table = pa.Table.from_pandas(df)
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

def transform_to_silver(bronze_df, match_meta, team_player_map, all_players):
    """Enriches and standardizes bronze delivery records for the silver layer."""
    df = bronze_df.copy()

    # Data Types & Fillna
    df['over'] = pd.to_numeric(df['over'], errors='coerce').fillna(0).astype(int)
    df['ball'] = pd.to_numeric(df['ball'], errors='coerce').fillna(1).astype(int)
    df['runs'] = pd.to_numeric(df['runs'], errors='coerce').fillna(0).astype(int)
    df['extra_runs'] = pd.to_numeric(df['extra_runs'], errors='coerce').fillna(0).astype(int)
    df['total_runs'] = pd.to_numeric(df['total_runs'], errors='coerce').fillna(0).astype(int)
    df['wicket'] = pd.to_numeric(df['wicket'], errors='coerce').fillna(0).astype(int)
    df['innings'] = pd.to_numeric(df['innings'], errors='coerce').fillna(1).astype(int)
    if 'rebowl' not in df.columns:
        df['rebowl'] = 0

    # Derived Features
    df['over_decimal'] = (df['over'] + df['ball'] / 10.0).round(1)
    df['innings_phase'] = df['over'].apply(get_innings_phase)
    
    df['is_dot_ball'] = (df['total_runs'] == 0).astype(int)
    df['is_boundary'] = df['runs'].isin([4, 6]).astype(int)
    df['is_four'] = (df['runs'] == 4).astype(int)
    df['is_six'] = (df['runs'] == 6).astype(int)
    df['is_legal_delivery'] = (~df['extra_type'].str.lower().isin(['wide', 'no ball', 'no-ball', '5 wides'])).astype(int)

    # Calculate batting_team and bowling_team
    def assign_teams(row):
        inn = row['innings']
        bat_team, bowl_team = get_batting_and_bowling_teams(match_meta, inn)
        return pd.Series([bat_team, bowl_team])

    df[['batting_team', 'bowling_team']] = df.apply(assign_teams, axis=1)

    # Scoped Player Name Standardisation (Fuzzy Matching per team)
    for inn in df['innings'].unique():
        inn_mask = (df['innings'] == inn)
        bat_team = df.loc[inn_mask, 'batting_team'].iloc[0] if not df.loc[inn_mask, 'batting_team'].empty else 'N/A'
        bowl_team = df.loc[inn_mask, 'bowling_team'].iloc[0] if not df.loc[inn_mask, 'bowling_team'].empty else 'N/A'

        bat_choices = get_team_player_choices(bat_team, team_player_map, all_players)
        bowl_choices = get_team_player_choices(bowl_team, team_player_map, all_players)

        if 'batsman' in df.columns:
            batsmen_inn = df.loc[inn_mask, 'batsman'].unique()
            b_map = {b: match_player_name(b, bat_choices) for b in batsmen_inn}
            df.loc[inn_mask, 'batsman'] = df.loc[inn_mask, 'batsman'].map(b_map)

        if 'bowler' in df.columns:
            bowlers_inn = df.loc[inn_mask, 'bowler'].unique()
            bw_map = {b: match_player_name(b, bowl_choices) for b in bowlers_inn}
            df.loc[inn_mask, 'bowler'] = df.loc[inn_mask, 'bowler'].map(bw_map)

        if 'out_batsman' in df.columns:
            outs_inn = df.loc[inn_mask, 'out_batsman'].unique()
            o_map = {b: match_player_name(b, bat_choices) for b in outs_inn}
            df.loc[inn_mask, 'out_batsman'] = df.loc[inn_mask, 'out_batsman'].map(o_map)

    # Join Match Metadata if present
    if match_meta:
        df['venue'] = match_meta.get('venue', 'N/A')
        df['toss_winner'] = match_meta.get('toss_winner', 'N/A')
        df['toss_decision'] = match_meta.get('toss_decision', 'N/A')
        if not 'date' in df.columns or df['date'].isnull().all():
            df['date'] = match_meta.get('date', 'N/A')

    # Deduplicate
    df = df.drop_duplicates(subset=['match', 'innings', 'over', 'ball', 'rebowl']).reset_index(drop=True)

    return df

import argparse

def get_job_arguments():
    """Parses Glue job arguments (--s3_key or --match)."""
    target_key = None
    target_match = None
    
    try:
        from awsglue.utils import getResolvedOptions
        known_args = getResolvedOptions(sys.argv, ['s3_key'])
        target_key = known_args.get('s3_key')
    except Exception:
        pass

    if not target_key:
        try:
            from awsglue.utils import getResolvedOptions
            known_args = getResolvedOptions(sys.argv, ['match'])
            target_match = known_args.get('match')
        except Exception:
            pass

    if not target_key and not target_match:
        parser = argparse.ArgumentParser()
        parser.add_argument('--s3_key', type=str, default=None, help='Target S3 key')
        parser.add_argument('--match', type=str, default=None, help='Target match ID')
        parsed, _ = parser.parse_known_args()
        target_key = parsed.s3_key
        target_match = parsed.match

    return target_key, target_match

def main():
    try:
        s3_client = get_s3_client()
        logger.info("Loading master player database from S3...")
        team_player_map, all_players = load_player_database_s3(s3_client)

        target_key, target_match = get_job_arguments()

        if target_key:
            logger.info(f"Event-driven trigger received for target S3 key: '{target_key}'")
            bronze_keys = [target_key]
        elif target_match:
            logger.info(f"Event-driven trigger received for target match: '{target_match}'")
            bronze_keys = [f"data/match/{target_match}/{target_match}_brnz.json"]
        else:
            search_prefix = "data/match/" if not MATCH_PREFIX.startswith("data/") else MATCH_PREFIX
            logger.info(f"No specific target file provided. Scanning all bronze files in s3://{S3_BRONZE}/{search_prefix}...")
            bronze_keys = list_json_s3(s3_client, S3_BRONZE, search_prefix)

        if not bronze_keys:
            logger.info("No bronze JSON files found to process.")
            return

        all_silver_dfs = []

        for key in bronze_keys:
            if not key.endswith("_brnz.json"):
                continue

            match = key.split("/")[-2] if "/" in key else key.split("_brnz.json")[0]
            logger.info(f"Processing Bronze -> Silver for match: {match} ({key})...")

            bronze_data = read_file_s3(s3_client, S3_BRONZE, key)
            if not bronze_data or not bronze_data.strip():
                logger.warning(f"Skipping empty bronze file: s3://{S3_BRONZE}/{key}")
                continue

            try:
                bronze_df = pd.read_json(io.StringIO(bronze_data), lines=True)
            except Exception:
                try:
                    bronze_df = pd.read_json(io.StringIO(bronze_data))
                except Exception as e:
                    logger.warning(f"Failed to parse JSON for {key}: {e}")
                    continue

            if bronze_df.empty or 'over' not in bronze_df.columns:
                logger.warning(f"Skipping bronze data with missing schema for key: {key}")
                continue

            # Fetch metadata from Raw layer if present
            meta_key = f"data/match/{match}/{match}_meta.json"
            match_meta = read_json_s3(s3_client, S3_RAW, meta_key)

            silver_df = transform_to_silver(bronze_df, match_meta, team_player_map, all_players)
            all_silver_dfs.append(silver_df)

        if not all_silver_dfs:
            logger.info("No valid silver data generated.")
            return

        combined_silver_df = pd.concat(all_silver_dfs, ignore_index=True)
        arrow_source = to_arrow_source(combined_silver_df)

        s3_delta_path = f"s3://{S3_SILVER}/deliveries"

        storage_options = get_storage_options()
        logger.info(f"Using S3 storage options: {list(storage_options.keys())}")

        table_exists = False
        try:
            dt = DeltaTable(s3_delta_path, storage_options=storage_options)
            table_exists = True
        except Exception:
            table_exists = False

        matches_to_update = combined_silver_df['match'].unique().tolist()

        if table_exists and len(matches_to_update) == 1:
            target_match_id = matches_to_update[0]
            logger.info(f"Existing Delta table found. Overwriting partition for match '{target_match_id}'...")
            write_deltalake(
                table_or_uri=s3_delta_path,
                data=arrow_source,
                partition_by=["match", "innings"],
                mode="overwrite",
                predicate=f"match = '{target_match_id}'",
                storage_options=storage_options
            )
            logger.info(f"SUCCESS! Partition for match '{target_match_id}' updated in Delta table at {s3_delta_path} ({len(combined_silver_df)} records)")
        else:
            logger.info(f"Writing Delta table to '{s3_delta_path}' partitioned by ['match', 'innings']...")
            write_deltalake(
                table_or_uri=s3_delta_path,
                data=arrow_source,
                partition_by=["match", "innings"],
                mode="overwrite" if not table_exists else "append",
                storage_options=storage_options
            )
            logger.info(f"SUCCESS! Updated Delta Lake table at {s3_delta_path} with {len(combined_silver_df)} records")

        # Automatically trigger downstream Gold Glue Job
        gold_job_name = os.environ.get("GOLD_JOB_NAME", "ipl_gold_analytics_job")
        try:
            logger.info(f"Triggering downstream Gold Glue job '{gold_job_name}'...")
            glue_client = boto3.client('glue', region_name=AWS_REGION)
            glue_client.start_job_run(JobName=gold_job_name)
            logger.info(f"SUCCESS! Triggered downstream Gold Glue job '{gold_job_name}'.")
        except Exception as e:
            logger.warning(f"Could not trigger downstream Gold Glue job '{gold_job_name}': {e}")

    except Exception as e:
        logger.error(f"Silver Glue Job failed: {e}")
        raise e

if __name__ == "__main__":
    main()
