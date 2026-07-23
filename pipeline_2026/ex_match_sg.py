"""
AWS Glue Job: Gold Layer Analytics Transformation (ex_match_sg.py)

This module reads delivery-level data from the Silver Delta Lake table (s3://<S3_SILVER>/deliveries),
computes Gold-layer analytics aggregations (Batsman Stats, Bowler Stats, Team Performance, Tournament Standings),
and writes materialized Gold Delta Lake tables to Amazon S3 (s3://<S3_GOLD>/).

Gold Tables Produced:
1. gold_batsman_stats: Batting performance metrics (runs, SR, avg, 4s, 6s, highest score)
2. gold_bowler_stats: Bowling performance metrics (wickets, economy, overs, dots, avg, SR)
3. gold_team_stats: Team performance across innings phases (Powerplay, Death overs, Run rate)
4. gold_tournament_standings: Official IPL Tournament Points Table (matches played, won, lost, points, NRR)
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
from rapidfuzz import process
from deltalake import write_deltalake, DeltaTable

# Logger setup
def setup_logger(name):
    """Configures a standardized StreamHandler logger for AWS CloudWatch logs."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s.%(msecs)d %(levelname)-8s [%(processName)s] [%(threadName)s]\n%(filename)s:%(lineno)d --- %(message)s\n")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger

logger = setup_logger("glue_gold_job")

# Attempt to load Glue job parameters into os.environ
try:
    from awsglue.utils import getResolvedOptions
    for param_name in ['S3_SILVER', 'S3_GOLD', 'AWS_REGION']:
        try:
            opt = getResolvedOptions(sys.argv, [param_name])
            if opt.get(param_name):
                os.environ[param_name] = opt[param_name]
        except Exception:
            pass
except Exception:
    pass

S3_SILVER = os.environ.get("S3_SILVER", "ipl-data-2026-silver")
S3_GOLD = os.environ.get("S3_GOLD", "ipl-data-2026-gold")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")

def get_storage_options():
    """
    Builds storage options dictionary for Rust Delta Lake S3 integration.
    Extracts IAM role credentials from active boto3 session when running in AWS Glue.
    """
    storage_options = {"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_KEY")
    aws_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_region = os.environ.get("AWS_REGION", AWS_REGION)

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
    except Exception:
        pass

    storage_options["AWS_REGION"] = aws_region
    return storage_options

def to_arrow_source(df):
    """
    Converts Pandas DataFrame into a PyArrow RecordBatchReader stream 
    guaranteed to expose __arrow_c_stream__ interface for Rust deltalake.
    """
    table = pa.Table.from_pandas(df)
    return pa.RecordBatchReader.from_batches(table.schema, table.to_batches())

def load_silver_data():
    """Reads Silver layer delivery data from S3 Delta table."""
    s3_delta_path = f"s3://{S3_SILVER}/deliveries"
    try:
        logger.info(f"Loading Silver Delta Lake table from '{s3_delta_path}'...")
        opts = get_storage_options()
        dt = DeltaTable(s3_delta_path, storage_options=opts)
        return dt.to_pandas()
    except Exception as e:
        logger.error(f"Failed to read S3 Silver Delta table at {s3_delta_path}: {e}")
        return None

def compute_batsman_stats(df):
    """Calculates comprehensive batting statistics for all batsmen."""
    logger.info("Computing Gold Batsman Statistics...")
    df_bat = df[df['batsman'].notnull() & (df['batsman'] != 'N/A')].copy()

    match_scores = df_bat.groupby(['batsman', 'match'])['runs'].sum().reset_index()
    highest_scores = match_scores.groupby('batsman')['runs'].max().rename('highest_score')

    bat_stats = df_bat.groupby('batsman').agg(
        matches=('match', 'nunique'),
        total_runs=('runs', 'sum'),
        legal_balls=('is_legal_delivery', 'sum'),
        fours=('is_four', 'sum'),
        sixes=('is_six', 'sum'),
        dot_balls=('is_dot_ball', 'sum')
    ).reset_index()

    outs = df[df['out_batsman'].notnull() & (df['out_batsman'] != 'N/A')].groupby('out_batsman').size().rename('times_out')

    bat_stats = bat_stats.merge(highest_scores, on='batsman', how='left')
    bat_stats = bat_stats.merge(outs, left_on='batsman', right_index=True, how='left')
    bat_stats['times_out'] = bat_stats['times_out'].fillna(0).astype(int)

    bat_stats['strike_rate'] = (bat_stats['total_runs'] * 100.0 / bat_stats['legal_balls'].replace(0, 1)).round(2)
    bat_stats['batting_avg'] = (bat_stats['total_runs'] / bat_stats['times_out'].replace(0, 1)).round(2)
    bat_stats['dot_ball_pct'] = (bat_stats['dot_balls'] * 100.0 / bat_stats['legal_balls'].replace(0, 1)).round(2)

    return bat_stats.sort_values(by='total_runs', ascending=False).reset_index(drop=True)

def compute_bowler_stats(df):
    """Calculates comprehensive bowling statistics for all bowlers."""
    logger.info("Computing Gold Bowler Statistics...")
    df_bowl = df[df['bowler'].notnull() & (df['bowler'] != 'N/A')].copy()

    bowl_stats = df_bowl.groupby('bowler').agg(
        matches=('match', 'nunique'),
        total_runs_conceded=('total_runs', 'sum'),
        legal_balls=('is_legal_delivery', 'sum'),
        wickets=('wicket', 'sum'),
        dot_balls=('is_dot_ball', 'sum'),
        fours_conceded=('is_four', 'sum'),
        sixes_conceded=('is_six', 'sum')
    ).reset_index()

    bowl_stats['overs_bowled'] = (bowl_stats['legal_balls'] // 6) + (bowl_stats['legal_balls'] % 6) / 10.0
    total_overs_decimal = bowl_stats['legal_balls'] / 6.0
    bowl_stats['economy_rate'] = (bowl_stats['total_runs_conceded'] / total_overs_decimal.replace(0, 1)).round(2)
    bowl_stats['bowling_avg'] = (bowl_stats['total_runs_conceded'] / bowl_stats['wickets'].replace(0, 1)).round(2)
    bowl_stats['strike_rate'] = (bowl_stats['legal_balls'] / bowl_stats['wickets'].replace(0, 1)).round(2)
    bowl_stats['dot_ball_pct'] = (bowl_stats['dot_balls'] * 100.0 / bowl_stats['legal_balls'].replace(0, 1)).round(2)

    return bowl_stats.sort_values(by=['wickets', 'economy_rate'], ascending=[False, True]).reset_index(drop=True)

def compute_team_stats(df):
    """Calculates team-level aggregated performance metrics across innings phases."""
    logger.info("Computing Gold Team Performance Statistics...")

    if 'batting_team' not in df.columns or df['batting_team'].isnull().all():
        logger.warning("batting_team column missing in Silver data. Skipping team stats calculation.")
        return None

    df_team = df[df['batting_team'].notnull() & (df['batting_team'] != 'N/A')].copy()

    team_stats = df_team.groupby('batting_team').agg(
        matches=('match', 'nunique'),
        total_runs=('total_runs', 'sum'),
        total_wickets_lost=('wicket', 'sum'),
        legal_balls=('is_legal_delivery', 'sum'),
        fours=('is_four', 'sum'),
        sixes=('is_six', 'sum')
    ).reset_index().rename(columns={'batting_team': 'team'})

    team_stats['overs_batted'] = (team_stats['legal_balls'] // 6) + (team_stats['legal_balls'] % 6) / 10.0
    total_overs = team_stats['legal_balls'] / 6.0
    team_stats['overall_run_rate'] = (team_stats['total_runs'] / total_overs.replace(0, 1)).round(2)

    # Phase Breakdown (Powerplay vs Death Overs)
    if 'innings_phase' in df.columns:
        pp_df = df_team[df_team['innings_phase'] == 'Powerplay'].groupby('batting_team').agg(
            pp_runs=('total_runs', 'sum'),
            pp_balls=('is_legal_delivery', 'sum')
        ).reset_index()
        pp_df['pp_run_rate'] = (pp_df['pp_runs'] / (pp_df['pp_balls'] / 6.0).replace(0, 1)).round(2)
        
        team_stats = team_stats.merge(pp_df[['batting_team', 'pp_run_rate']], left_on='team', right_on='batting_team', how='left').drop(columns=['batting_team'], errors='ignore')

    team_stats = team_stats.sort_values(by='total_runs', ascending=False).reset_index(drop=True)
    return team_stats

def compute_tournament_standings(df):
    """Calculates official IPL Tournament Standings (Points Table, Wins, Losses, NRR, Avg RR)."""
    logger.info("Computing Gold Tournament Points Table & Standings...")
    if 'batting_team' not in df.columns or 'bowling_team' not in df.columns:
        logger.warning("batting_team or bowling_team missing in Silver data. Skipping tournament standings.")
        return None

    df_clean = df[
        (df['batting_team'].notnull()) & (df['batting_team'] != 'N/A') &
        (df['bowling_team'].notnull()) & (df['bowling_team'] != 'N/A')
    ].copy()

    match_team_summary = df_clean.groupby(['match', 'batting_team']).agg(
        runs_scored=('total_runs', 'sum'),
        legal_balls_batted=('is_legal_delivery', 'sum')
    ).reset_index().rename(columns={'batting_team': 'team'})

    match_opp_summary = df_clean.groupby(['match', 'bowling_team']).agg(
        runs_conceded=('total_runs', 'sum'),
        legal_balls_bowled=('is_legal_delivery', 'sum')
    ).reset_index().rename(columns={'bowling_team': 'team'})

    match_stats = match_team_summary.merge(match_opp_summary, on=['match', 'team'], how='outer').fillna(0)

    all_teams = df_clean['batting_team'].unique().tolist()
    match_winners = []
    for match_id, match_group in df_clean.groupby('match'):
        teams_in_match = match_group['batting_team'].unique()
        if len(teams_in_match) == 2:
            t1, t2 = teams_in_match[0], teams_in_match[1]
            t1_runs = match_group[match_group['batting_team'] == t1]['total_runs'].sum()
            t2_runs = match_group[match_group['batting_team'] == t2]['total_runs'].sum()

            if t1_runs > t2_runs:
                match_winners.append({'match': match_id, 'winner': t1, 'loser': t2, 'is_tie': False})
            elif t2_runs > t1_runs:
                match_winners.append({'match': match_id, 'winner': t2, 'loser': t1, 'is_tie': False})
            else:
                match_winners.append({'match': match_id, 'winner': None, 'loser': None, 'is_tie': True})

    df_winners = pd.DataFrame(match_winners)

    standings = []
    for team in all_teams:
        if not df_winners.empty:
            wins = len(df_winners[df_winners['winner'] == team])
            losses = len(df_winners[df_winners['loser'] == team])
            ties = len(df_winners[(df_winners['is_tie'] == True) & ((df_winners['winner'] == team) | (df_winners['loser'] == team))])
        else:
            wins, losses, ties = 0, 0, 0

        played = wins + losses + ties
        points = (wins * 2) + (ties * 1)

        team_matches = match_stats[match_stats['team'] == team]
        runs_for = team_matches['runs_scored'].sum()
        balls_for = team_matches['legal_balls_batted'].sum()
        runs_against = team_matches['runs_conceded'].sum()
        balls_against = team_matches['legal_balls_bowled'].sum()

        overs_for_dec = balls_for / 6.0
        overs_against_dec = balls_against / 6.0

        avg_run_rate = round(runs_for / overs_for_dec, 2) if overs_for_dec > 0 else 0.0
        conceded_run_rate = round(runs_against / overs_against_dec, 2) if overs_against_dec > 0 else 0.0
        nrr = round(avg_run_rate - conceded_run_rate, 3)

        standings.append({
            'team': team,
            'played': played,
            'won': wins,
            'lost': losses,
            'tied_nr': ties,
            'points': points,
            'net_run_rate': nrr,
            'avg_run_rate': avg_run_rate,
            'runs_for': int(runs_for),
            'overs_for': round(balls_for // 6 + (balls_for % 6) / 10.0, 1),
            'runs_against': int(runs_against),
            'overs_against': round(balls_against // 6 + (balls_against % 6) / 10.0, 1)
        })

    df_standings = pd.DataFrame(standings)
    df_standings = df_standings.sort_values(by=['points', 'net_run_rate', 'avg_run_rate'], ascending=[False, False, False]).reset_index(drop=True)
    df_standings['rank'] = df_standings.index + 1
    
    cols = ['rank', 'team', 'played', 'won', 'lost', 'tied_nr', 'points', 'net_run_rate', 'avg_run_rate', 'runs_for', 'overs_for', 'runs_against', 'overs_against']
    return df_standings[cols]

def save_gold_table_s3(df, table_name, storage_options):
    """Saves a Gold DataFrame as a materialized S3 Delta table."""
    if df is None or df.empty:
        logger.warning(f"No data to save for Gold table '{table_name}'.")
        return

    s3_path = f"s3://{S3_GOLD}/{table_name}"
    arrow_source = to_arrow_source(df)

    logger.info(f"Saving Gold Delta table '{table_name}' to {s3_path}...")
    write_deltalake(
        table_or_uri=s3_path,
        data=arrow_source,
        mode="overwrite",
        storage_options=storage_options
    )
    logger.info(f"SUCCESS! Created S3 Gold Delta table at {s3_path} with {len(df)} records")

def main():
    try:
        logger.info("=========================================================")
        logger.info("Starting AWS Glue Gold Layer Analytics Transformation")
        logger.info("=========================================================")

        silver_df = load_silver_data()
        if silver_df is None or silver_df.empty:
            logger.error("Stopping Gold Glue Job: Silver data is empty.")
            return

        logger.info(f"Loaded {len(silver_df)} delivery records from Silver Delta table.")
        storage_options = get_storage_options()

        # 1. Batsman Stats
        batsman_df = compute_batsman_stats(silver_df)
        save_gold_table_s3(batsman_df, "gold_batsman_stats", storage_options)

        # 2. Bowler Stats
        bowler_df = compute_bowler_stats(silver_df)
        save_gold_table_s3(bowler_df, "gold_bowler_stats", storage_options)

        # 3. Team Stats
        team_df = compute_team_stats(silver_df)
        if team_df is not None:
            save_gold_table_s3(team_df, "gold_team_stats", storage_options)

        # 4. Tournament Standings
        standings_df = compute_tournament_standings(silver_df)
        if standings_df is not None:
            save_gold_table_s3(standings_df, "gold_tournament_standings", storage_options)

        logger.info("=========================================================")
        logger.info("Gold Glue Job Complete Successfully!")
        logger.info("=========================================================")

    except Exception as e:
        logger.error(f"Gold Glue Job failed: {e}")
        raise e

if __name__ == "__main__":
    main()
