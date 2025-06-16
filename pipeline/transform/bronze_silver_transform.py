from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr, trim, current_date

# Initialize Spark with Delta support
spark = SparkSession.builder \
    .appName("bronze_to_silver_delta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://ipl-data-pipeline/data/silver/") \
    .enableHiveSupport() \
    .getOrCreate()

# Load data from S3 (adjust paths as needed)
ball_by_ball_df = spark.read.json("s3://ipl-data-pipeline/data/bronze/ball-by-ball/")
players_df = spark.read.json("s3://ipl-data-pipeline/data/bronze/players/")
results_df = spark.read.json("s3://ipl-data-pipeline/data/bronze/results/")
fixtures_df = spark.read.json("s3://ipl-data-pipeline/data/bronze/fixtures/fixtures.json")

# Filter for today's records (assuming `date` is in the ball_by_ball_df)
ball_by_ball_today = ball_by_ball_df.filter(col("date") == current_date())

# Rename players fields for bowler join
players_bowler_df = players_df.selectExpr(
    "name as bowler_name",
    "team as bowler_team",
    "country as bowler_country",
    "role as bowler_role",
    "batting_style as bowler_batting_style",
    "bowling_style as bowler_bowling_style"
)

# Join fixtures and results to get match metadata
fixtures_trimmed = fixtures_df.withColumn("short_name", trim(col("short_name")))
results_trimmed = results_df.withColumn("short_name", trim(col("short_name")))
match_results = fixtures_trimmed.join(
    results_trimmed,
    fixtures_trimmed.short_name == results_trimmed.short_name,
    "left"
).filter(fixtures_trimmed.date == current_date()).selectExpr(
    "fixtures.match_name as match",
    "fixtures.short_name as match_code",
    "cast(results.date as date) as date",
    "results.time",
    "fixtures.venue",
    "fixtures.team1 as home_team",
    "fixtures.team2 as away_team",
    "results.winner",
    "results.result as margin",
    """CASE 
        WHEN results.result LIKE '%wickets' THEN 'wickets'
        WHEN results.result LIKE '%runs' THEN 'runs'
        WHEN results.result LIKE '%Super Over' THEN 'Super Over'
        ELSE 'N/A' 
    END AS margin_type""",
    """CASE 
        WHEN results.result LIKE '%wickets' THEN 'Completed'
        WHEN results.result LIKE '%runs' THEN 'Completed'
        WHEN results.result LIKE '%Super Over' THEN 'Super Over'
        WHEN results.result LIKE '%Suspended%' THEN 'Suspended'
        ELSE 'Abandoned' 
    END AS status"""
)

# Join batsman info
batsman_joined = ball_by_ball_today.join(
    players_df,
    (ball_by_ball_today.batting_team == players_df.team) & (ball_by_ball_today.batsman == players_df.name),
    "left"
)

# Join bowler info
deliveries_with_players = batsman_joined.join(
    players_bowler_df,
    (batsman_joined.bowling_team == players_bowler_df.bowler_team) & (batsman_joined.bowler == players_bowler_df.bowler_name),
    "left"
)

# Add calculated fields
deliveries_enriched = deliveries_with_players.selectExpr(
    "match",
    "short_name as match_code",
    "cast(date as date)",
    "time",
    "venue",
    "batting_team",
    "bowling_team",
    "ball_id",
    "innings",
    "cast(over as int) as over",
    "ball",
    "((over * 10) + ball) / 10 as match_overs",
    """CASE 
        WHEN cast(over as int) < 5 THEN 'Powerplay'
        WHEN cast(over as int) < 15 THEN 'Middle'
        ELSE 'Death' 
    END AS innings_phase""",
    "batsman",
    "country as batsman_country",
    "role as batsman_role",
    "batting_style as batsman_batting_style",
    "bowler",
    "bowler_country",
    "bowler_role",
    "bowler_bowling_style",
    "batter_runs",
    "extra_runs",
    "runs_from_ball",
    "extra as extra_flag",
    "extra_type",
    "rebowl as rebowl_flag",
    "wicket as wicket_flag",
    "wicket_method as dismissal_type",
    "out_batsman as dismissed_batsman",
    "valid_ball",
    "current_score as score",
    "current_wickets as wickets",
    "target"
)

# Join match results
final_df = deliveries_enriched.join(
    match_results,
    "match_code",
    "left"
).select(
    "match", "home_team", "away_team", *deliveries_enriched.columns,
    "winner", "margin", "margin_type", "status"
)

# Write to Silver layer (Delta format recommended, or use Parquet)
final_df.write.mode("overwrite")\
    .partitionBy("match_code", "date")\
    .format("parquet")\
    .option("compression", "snappy")\
    .save("s3://ipl-data-pipeline/data/silver/master-data/")

# Final output path (Delta format)
output_path = "s3://ipl-data-pipeline/data/silver/master-data/"

# Glue Catalog target
database_name = "ipl_silver"
table_name = "silver_deliveries"

# Write to S3 in Delta format
final_df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("match_code", "date") \
    .option("overwriteSchema", "true") \
    .save(output_path)

# Register the table in the Glue Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{table_name}
    USING DELTA
    LOCATION '{output_path}'
""")