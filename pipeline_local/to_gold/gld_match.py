"""
IPL Data Pipeline - Gold Layer Transformation (Local Script)

This script reads delivery-level data from the Silver Delta Lake table (or Parquet/CSV),
computes business-ready Gold-layer analytics aggregations (Batsman Stats, Bowler Stats, Team Metrics),
and writes the Gold tables to local Delta Lake storage (or Parquet).

Gold Tables Produced:
1. gold_batsman_stats: High-level batting performance metrics (runs, SR, avg, 4s, 6s, highest score)
2. gold_bowler_stats: High-level bowling performance metrics (wickets, economy, overs, dots, avg, SR)
3. gold_team_stats: Aggregated team performance across innings phases (Powerplay, Death overs, Run rate)
"""
import os
import sys
import logging
import pandas as pd
from deltalake import DeltaTable, write_deltalake

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROJECT_ROOT = os.path.dirname(BASE_DIR)
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver", "deliveries")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")

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

logger = setup_logger("gold_transformation")

def get_storage_options():
    """Builds storage options dict for Delta Lake S3 integration."""
    storage_options = {"AWS_S3_ALLOW_UNSAFE_RENAME": "true"}
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SECRET_KEY")
    aws_token = os.environ.get("AWS_SESSION_TOKEN")
    aws_region = os.environ.get("AWS_REGION", "us-east-1")
    
    if aws_key and aws_secret:
        storage_options["AWS_ACCESS_KEY_ID"] = aws_key
        storage_options["AWS_SECRET_ACCESS_KEY"] = aws_secret
        if aws_token:
            storage_options["AWS_SESSION_TOKEN"] = aws_token
        storage_options["AWS_REGION"] = aws_region
        return storage_options

    try:
        import boto3
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

def load_silver_data():
    """Reads Silver layer delivery data from local Delta table or S3 Delta table."""
    if os.path.exists(SILVER_DIR) and os.path.exists(os.path.join(SILVER_DIR, "_delta_log")):
        try:
            logger.info(f"Reading local Silver Delta Lake table from '{SILVER_DIR}'...")
            return DeltaTable(SILVER_DIR).to_pandas()
        except Exception as e:
            logger.warning(f"Could not read local Delta table at {SILVER_DIR}: {e}")

    s3_silver_bucket = os.environ.get("S3_SILVER", "ipl-data-2026-silver")
    s3_delta_path = f"s3://{s3_silver_bucket}/deliveries"
    try:
        logger.info(f"Attempting to read S3 Silver Delta Lake table from '{s3_delta_path}'...")
        opts = get_storage_options()
        return DeltaTable(s3_delta_path, storage_options=opts).to_pandas()
    except Exception as e:
        logger.warning(f"Could not read S3 Delta table at {s3_delta_path}: {e}")

    # Fallback to search CSV files in data/silver
    alt_silver_csv = os.path.join(PROJECT_ROOT, "data", "silver")
    if os.path.exists(alt_silver_csv):
        csv_files = [os.path.join(r, f) for r, d, fs in os.walk(alt_silver_csv) for f in fs if f.endswith('.csv')]
        if csv_files:
            logger.info(f"Loading {len(csv_files)} Silver CSV files...")
            dfs = [pd.read_csv(f) for f in csv_files]
            return pd.concat(dfs, ignore_index=True)

    logger.error("No Silver layer data found!")
    return None

def compute_batsman_stats(df):
    """Calculates comprehensive batting statistics for all batsmen."""
    logger.info("Computing Gold Batsman Statistics...")
    
    # Exclude N/A batsmen
    df_bat = df[df['batsman'].notnull() & (df['batsman'] != 'N/A')].copy()

    # Per-match match stats for highest score
    match_scores = df_bat.groupby(['batsman', 'match'])['runs'].sum().reset_index()
    highest_scores = match_scores.groupby('batsman')['runs'].max().rename('highest_score')

    # Aggregations
    bat_stats = df_bat.groupby('batsman').agg(
        matches=('match', 'nunique'),
        total_runs=('runs', 'sum'),
        legal_balls=('is_legal_delivery', 'sum'),
        fours=('is_four', 'sum'),
        sixes=('is_six', 'sum'),
        dot_balls=('is_dot_ball', 'sum')
    ).reset_index()

    # Times out (dismissed)
    outs = df[df['out_batsman'].notnull() & (df['out_batsman'] != 'N/A')].groupby('out_batsman').size().rename('times_out')

    # Merge stats
    bat_stats = bat_stats.merge(highest_scores, on='batsman', how='left')
    bat_stats = bat_stats.merge(outs, left_on='batsman', right_index=True, how='left')
    bat_stats['times_out'] = bat_stats['times_out'].fillna(0).astype(int)

    # Derived KPIs
    bat_stats['strike_rate'] = (bat_stats['total_runs'] * 100.0 / bat_stats['legal_balls'].replace(0, 1)).round(2)
    bat_stats['batting_avg'] = (bat_stats['total_runs'] / bat_stats['times_out'].replace(0, 1)).round(2)
    bat_stats['dot_ball_pct'] = (bat_stats['dot_balls'] * 100.0 / bat_stats['legal_balls'].replace(0, 1)).round(2)

    # Sort by total runs descending
    bat_stats = bat_stats.sort_values(by='total_runs', ascending=False).reset_index(drop=True)
    return bat_stats

def compute_bowler_stats(df):
    """Calculates comprehensive bowling statistics for all bowlers."""
    logger.info("Computing Gold Bowler Statistics...")

    # Exclude N/A bowlers
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

    # Derived KPIs
    bowl_stats['overs_bowled'] = (bowl_stats['legal_balls'] // 6) + (bowl_stats['legal_balls'] % 6) / 10.0
    total_overs_decimal = bowl_stats['legal_balls'] / 6.0
    bowl_stats['economy_rate'] = (bowl_stats['total_runs_conceded'] / total_overs_decimal.replace(0, 1)).round(2)
    bowl_stats['bowling_avg'] = (bowl_stats['total_runs_conceded'] / bowl_stats['wickets'].replace(0, 1)).round(2)
    bowl_stats['strike_rate'] = (bowl_stats['legal_balls'] / bowl_stats['wickets'].replace(0, 1)).round(2)
    bowl_stats['dot_ball_pct'] = (bowl_stats['dot_balls'] * 100.0 / bowl_stats['legal_balls'].replace(0, 1)).round(2)

    # Sort by wickets descending, then economy rate ascending
    bowl_stats = bowl_stats.sort_values(by=['wickets', 'economy_rate'], ascending=[False, True]).reset_index(drop=True)
    return bowl_stats

def compute_team_stats(df):
    """Calculates team-level aggregated performance metrics."""
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

    # 1. Match-Level Summary (Total runs and legal balls per match per team)
    match_team_summary = df_clean.groupby(['match', 'batting_team']).agg(
        runs_scored=('total_runs', 'sum'),
        legal_balls_batted=('is_legal_delivery', 'sum')
    ).reset_index().rename(columns={'batting_team': 'team'})

    match_opp_summary = df_clean.groupby(['match', 'bowling_team']).agg(
        runs_conceded=('total_runs', 'sum'),
        legal_balls_bowled=('is_legal_delivery', 'sum')
    ).reset_index().rename(columns={'bowling_team': 'team'})

    match_stats = match_team_summary.merge(match_opp_summary, on=['match', 'team'], how='outer').fillna(0)

    # Determine winner for each match
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

    # 2. Team Standings Aggregation
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

        # Runs & Overs for NRR
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

def save_gold_table(df, name):
    """Saves a DataFrame as a Gold Delta Lake table."""
    if df is None or df.empty:
        logger.warning(f"No data to save for Gold table '{name}'.")
        return

    output_path = os.path.join(GOLD_DIR, name)
    os.makedirs(output_path, exist_ok=True)
    
    logger.info(f"Saving Gold Delta table '{name}' to {output_path}...")
    write_deltalake(output_path, df, mode="overwrite")
    
    # Save CSV copy for convenience
    csv_path = os.path.join(output_path, f"{name}.csv")
    df.to_csv(csv_path, index=False)
    logger.info(f"SUCCESS! Created Gold table '{name}' with {len(df)} records. CSV saved at {csv_path}")

def main():
    logger.info("=========================================================")
    logger.info("Starting Gold Layer Analytics Transformation")
    logger.info("=========================================================")

    silver_df = load_silver_data()
    if silver_df is None or silver_df.empty:
        logger.error("Stopping Gold transformation: Silver data is empty.")
        return

    logger.info(f"Loaded {len(silver_df)} delivery records from Silver layer.")

    # 1. Batsman Stats
    batsman_df = compute_batsman_stats(silver_df)
    save_gold_table(batsman_df, "gold_batsman_stats")

    # 2. Bowler Stats
    bowler_df = compute_bowler_stats(silver_df)
    save_gold_table(bowler_df, "gold_bowler_stats")

    # 3. Team Stats
    team_df = compute_team_stats(silver_df)
    if team_df is not None:
        save_gold_table(team_df, "gold_team_stats")

    # 4. Tournament Standings / Points Table
    standings_df = compute_tournament_standings(silver_df)
    if standings_df is not None:
        save_gold_table(standings_df, "gold_tournament_standings")

    logger.info("=========================================================")
    logger.info("Gold Layer Transformation Complete Successfully!")
    logger.info("=========================================================")

if __name__ == "__main__":
    main()
