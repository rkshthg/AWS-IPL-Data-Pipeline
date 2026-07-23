import os
import json
import io
import sys
import pandas as pd
from rapidfuzz import process
from datetime import datetime
from deltalake import write_deltalake, DeltaTable

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from pipeline_local.utils import read_file_local, read_json_local, save_file_local, logger

def load_player_database():
    """Loads master player objects from players.json, mapping team -> list of player names."""
    players_data = read_file_local("", "players.json", layer="raw")
    if not players_data:
        return {}, []
    
    team_player_map = {}
    all_players = []
    try:
        for line in players_data.strip().split("\n"):
            if line.strip():
                p = json.loads(line)
                name = p.get("Name")
                team = p.get("Team")
                if name:
                    all_players.append(name)
                    if team:
                        if team not in team_player_map:
                            team_player_map[team] = []
                        team_player_map[team].append(name)
        return team_player_map, list(set(all_players))
    except Exception as e:
        logger.error(f"Error loading players list: {e}")
        return {}, []

def match_player_name(raw_name, choices, cutoff=75):
    """Normalizes player names using RapidFuzz against candidate player choices."""
    if not raw_name or raw_name == "N/A" or pd.isna(raw_name):
        return "N/A"
    
    raw_name_clean = str(raw_name).strip()
    if not choices:
        return raw_name_clean

    match = process.extractOne(raw_name_clean, choices, score_cutoff=cutoff)
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

def main():
    try:
        target_match = sys.argv[1] if len(sys.argv) > 1 else None

        team_player_map, all_players = load_player_database()
        
        if target_match:
            logger.info(f"Single-match trigger: Processing Silver layer ONLY for match: '{target_match}'...")
            match_folders = [target_match]
        else:
            raw_dir = "data/bronze"
            if not os.path.exists(raw_dir):
                logger.error(f"Bronze data directory '{raw_dir}' does not exist.")
                return
            match_folders = [f for f in os.listdir(raw_dir) if os.path.isdir(os.path.join(raw_dir, f))]

        if not match_folders:
            logger.warning("No match directories found to process.")
            return

        all_silver_dfs = []

        for match in match_folders:
            bronze_file = f"{match}_brnz.json"
            bronze_data = read_file_local(f"{match}/", bronze_file, layer="bronze")
            
            if not bronze_data:
                logger.warning(f"No bronze data found for match {match}")
                continue

            logger.info(f"Processing Silver layer for match: {match}...")
            
            try:
                bronze_df = pd.read_json(io.StringIO(bronze_data), lines=True)
            except Exception:
                try:
                    bronze_df = pd.read_json(io.StringIO(bronze_data))
                except Exception as e:
                    logger.warning(f"Failed to parse JSON for {match}: {e}")
                    continue

            # Fetch metadata if available
            match_meta = read_json_local(f"{match}/", f"{match}_meta.json", layer="raw")

            # Transform to Silver
            silver_df = transform_to_silver(bronze_df, match_meta, team_player_map, all_players)
            all_silver_dfs.append(silver_df)

        if not all_silver_dfs:
            logger.warning("No silver datasets produced.")
            return

        combined_silver_df = pd.concat(all_silver_dfs, ignore_index=True)
        delta_table_path = "data/silver/deliveries"

        table_exists = os.path.exists(os.path.join(delta_table_path, "_delta_log"))
        matches_to_update = combined_silver_df['match'].unique().tolist()

        if table_exists and len(matches_to_update) == 1:
            target_match_id = matches_to_update[0]
            logger.info(f"Existing Delta table found. Overwriting partition for match '{target_match_id}'...")
            write_deltalake(
                table_or_uri=delta_table_path,
                data=combined_silver_df,
                partition_by=["match", "innings"],
                mode="overwrite",
                predicate=f"match = '{target_match_id}'"
            )
            logger.info(f"Successfully updated partition for match '{target_match_id}' in Delta Lake table!")
        else:
            logger.info(f"Writing Delta Lake table to '{delta_table_path}' partitioned by ['match', 'innings']...")
            write_deltalake(
                table_or_uri=delta_table_path,
                data=combined_silver_df,
                partition_by=["match", "innings"],
                mode="overwrite" if not table_exists else "append"
            )
            logger.info(f"Successfully updated Delta Lake table! Total deliveries: {len(combined_silver_df)}")

    except Exception as e:
        logger.error(f"Silver pipeline failed: {e}")

if __name__ == "__main__":
    main()
