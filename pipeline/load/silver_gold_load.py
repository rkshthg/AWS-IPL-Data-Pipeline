from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, current_date
from delta.tables import DeltaTable

# Initialize Spark with Delta support
spark = SparkSession.builder \
    .appName("silver_to_gold_merge") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport() \
    .getOrCreate()

silver_db = "ipl_silver"
gold_db = "ipl_gold"
gold_path = "s3://ipl-data-pipeline/data/gold/"

deliveries_df = spark.read.table(f"{silver_db}.silver_deliveries")
players_df = spark.read.json("s3://ipl-data-pipeline/data/bronze/players/players.json")

filtered_df = deliveries_df.filter(col("status") != "Suspended").filter(col("date") < current_date())
filtered_df.createOrReplaceTempView("master_data")

def merge_to_delta(df, db_name, table_name, path, merge_condition, partition_cols=None):
    try:
        if DeltaTable.isDeltaTable(spark, path):
            delta_table = DeltaTable.forPath(spark, path)
            delta_table.alias("target").merge(
                df.alias("source"),
                merge_condition
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        else:
            writer = df.write.format("delta").mode("overwrite")
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            writer.save(path)
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {db_name}.{table_name}
                USING DELTA
                LOCATION '{path}'
            """)
    except Exception as e:
        print(f"Error merging data to {db_name}.{table_name}: {e}")
        raise

# Dim: Players
merge_to_delta(players_df, gold_db, "dim_players", f"{gold_path}players/", "target.name = source.name", partition_cols=["team"])

# Dim: Teams
teams_df = filtered_df.selectExpr("home_team as team").union(filtered_df.selectExpr("away_team as team")).distinct()
merge_to_delta(teams_df, gold_db, "dim_teams", f"{gold_path}teams/", "target.team = source.team")

# Dim: Matches
matches_df = filtered_df.dropDuplicates(["match_code"])
merge_to_delta(matches_df, gold_db, "dim_matches", f"{gold_path}matches/", "target.match_code = source.match_code")

# Fact: Deliveries
deliveries_fact = filtered_df.drop("venue", "time", "batsman_country", "batsman_role", "batsman_batting_style",
                                   "bowler_country", "bowler_role", "bowler_bowling_style", "dismissed_batsman",
                                   "winner", "margin", "margin_type", "status", "away_team", "home_team")
merge_to_delta(deliveries_fact, gold_db, "fact_deliveries", f"{gold_path}deliveries/",
               "target.match_code = source.match_code AND target.ball_id = source.ball_id",
               partition_cols=["match_code", "innings", "over"])

# Fact: Batsman
batsman_df = spark.sql("""
SELECT batsman, batting_team as team, match_code, bowling_team as against,
       SUM(batter_runs) as runs_scored,
       SUM(valid_ball) as balls_faced,
       ROUND((SUM(batter_runs)*100.0)/NULLIF(SUM(valid_ball), 0), 2) as strike_rate,
       SUM(CASE WHEN batter_runs = 4 THEN 1 ELSE 0 END) as fours,
       SUM(CASE WHEN batter_runs = 6 THEN 1 ELSE 0 END) as sixes
FROM master_data
WHERE innings < 3
GROUP BY batsman, team, match_code, against
""")
merge_to_delta(batsman_df, gold_db, "fact_batsman_stats", f"{gold_path}batsman/",
               "target.match_code = source.match_code AND target.batsman = source.batsman",
               partition_cols=["batsman"])

# Fact: Bowler
bowler_df = spark.sql("""
SELECT bowler, bowling_team as team, match_code, batting_team as against,
       SUM(runs_from_ball) as runs_conceded,
       SUM(extra_runs) as extras,
       SUM(valid_ball) as balls_bowled,
       ROUND(SUM(valid_ball)/6 + (SUM(valid_ball)%6)/10.0, 1) as overs_bowled,
       ROUND(SUM(runs_from_ball)/NULLIF(SUM(valid_ball)/6.0, 0), 2) as economy,
       SUM(CASE WHEN dismissal_type NOT LIKE '%Run Out%' THEN wicket_flag ELSE 0 END) as wickets_taken
FROM master_data
WHERE innings < 3
GROUP BY bowler, team, match_code, against
""")
merge_to_delta(bowler_df, gold_db, "fact_bowler_stats", f"{gold_path}bowler/",
               "target.match_code = source.match_code AND target.bowler = source.bowler",
               partition_cols=["bowler"])

# Net Run Rate
batting = filtered_df.groupBy("batting_team").agg(
    expr("SUM(batter_runs) as runs_scored"),
    expr("ROUND(SUM(valid_ball)/6 + (SUM(valid_ball)%6)/10.0, 1) as overs_faced")
)
bowling = filtered_df.groupBy("bowling_team").agg(
    expr("SUM(runs_from_ball) as runs_conceded"),
    expr("ROUND(SUM(valid_ball)/6 + (SUM(valid_ball)%6)/10.0, 1) as overs_bowled")
)
nrr_df = batting.join(bowling, col("batting_team") == col("bowling_team")).selectExpr(
    "batting_team as team",
    "ROUND((runs_scored/NULLIF(overs_faced,0)) - (runs_conceded/NULLIF(overs_bowled,0)), 3) as net_run_rate"
)

# Fact: Points
matches_df = filtered_df.dropDuplicates(["match_code"])
home_matches = matches_df.selectExpr("match_code", "home_team as team", "away_team as opponent", "winner")
away_matches = matches_df.selectExpr("match_code", "away_team as team", "home_team as opponent", "winner")
all_matches = home_matches.union(away_matches)

points_df = all_matches.withColumn("result",
    expr("CASE WHEN winner = team THEN 'win' WHEN winner IS NOT NULL AND winner != team THEN 'loss' ELSE 'no result' END")
).groupBy("team").agg(
    expr("COUNT(*) as matches_played"),
    expr("SUM(CASE WHEN result = 'win' THEN 1 ELSE 0 END) as wins"),
    expr("SUM(CASE WHEN result = 'loss' THEN 1 ELSE 0 END) as losses"),
    expr("SUM(CASE WHEN result = 'no result' THEN 1 ELSE 0 END) as no_results"),
    expr("SUM(CASE WHEN result = 'win' THEN 2 WHEN result = 'no result' THEN 1 ELSE 0 END) as points")
).join(nrr_df, "team").selectExpr(
    "team as team_name", "matches_played", "wins", "losses", "no_results", "points", "net_run_rate"
)
merge_to_delta(points_df, gold_db, "fact_points", f"{gold_path}points/", "target.team_name = source.team_name")
