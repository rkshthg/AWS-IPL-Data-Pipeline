# 🏏 AWS IPL Medallion Data Pipeline (Raw → Bronze → Silver → Gold)

An end-to-end, event-driven **Serverless Data Engineering Pipeline** built with **Python, AWS Services (S3, Lambda, Glue, Athena)**, and **Delta Lake**. The pipeline ingests, cleans, enriches, and aggregates near real-time **IPL (Indian Premier League) ball-by-ball match data** using the **Medallion Architecture**.

---

## 🏗️ Architecture Overview

```
                      +-------------------------------------------------+
                      |              1. Ingestion (Raw)                 |
                      |  Cricbuzz Web Scraper (Selenium / BeautifulSoup) |
                      +------------------------+------------------------+
                                               |
                                               v
                      +-------------------------------------------------+
                      |            2. Raw & Bronze Layers               |
                      |  S3: s3://ipl-data-2026-raw/                    |
                      |  S3: s3://ipl-data-2026-bronze/                 |
                      +------------------------+------------------------+
                                               | (AWS Lambda / Event Driven)
                                               v
                      +-------------------------------------------------+
                      |             3. Silver Layer (ETL)               |
                      |  AWS Glue Job: ex_match_bs.py                   |
                      |  - Team-Scoped RapidFuzz Name Normalization     |
                      |  - Batting/Bowling Team Derivation              |
                      |  - Partition Overwrite Predicate                |
                      |  S3: s3://ipl-data-2026-silver/deliveries/      |
                      +------------------------+------------------------+
                                               | (Boto3 / EventBridge Trigger)
                                               v
                      +-------------------------------------------------+
                      |            4. Gold Layer (Analytics)            |
                      |  AWS Glue Job: ex_match_sg.py                   |
                      |  - Batsman Leaderboards & KPIs                  |
                      |  - Bowler Leaderboards & Economy Rates          |
                      |  - IPL Points Table & Net Run Rate (NRR)        |
                      |  S3: s3://ipl-data-2026-gold/                   |
                      +------------------------+------------------------+
                                               |
                                               v
                      +-------------------------------------------------+
                      |            5. Analytics & Dashboards            |
                      |  Amazon Athena / Power BI / Tableau             |
                      +-------------------------------------------------+
```

---

## 📐 Medallion Architecture Layers

### 1. 🥉 Raw & Bronze Layers (`s3://ipl-data-2026-raw` & `s3://ipl-data-2026-bronze`)
* **Raw Ingestion**: Python scrapers (`ex_match_raw.py`, `ex_fixtures.py`, `ex_players.py`) extract match schedules, player catalogs (`players.json`), and match metadata (`_meta.json`).
* **Bronze Structuring**: Raw delivery commentary is structured into flat JSON delivery records stored under `data/match/{match_id}/{match_id}_brnz.json`.

---

### 2. 🥈 Silver Layer (`s3://ipl-data-2026-silver/deliveries`)
* **Engine**: AWS Glue Python Shell Job (`ex_match_bs.py`).
* **Format**: Partitioned ACID **Delta Lake Table** (`partition_by=['match', 'innings']`).
* **Key Transformations**:
  * **Team Derivation**: Dynamically determines `batting_team` and `bowling_team` per delivery based on `toss_winner`, `toss_decision`, and `innings`.
  * **Team-Scoped Fuzzy Matching**: Uses **RapidFuzz** restricted *only* to the playing teams' squads (speeds up matching 10x and prevents false positive player matches).
  * **Feature Engineering**: Calculates `over_decimal`, `innings_phase` (Powerplay, Middle Overs, Death Overs, Super Over), `is_dot_ball`, `is_boundary`, `is_four`, `is_six`, and `is_legal_delivery`.
  * **Idempotent Partition Upsert**: Uses Delta Lake `predicate=match = '...'` overwrite mode to update updated match partitions while leaving historical data intact.

---

### 3. 🥇 Gold Layer (`s3://ipl-data-2026-gold`)
* **Engine**: AWS Glue Python Shell Job (`ex_match_sg.py`).
* **Format**: Materialized Delta Lake Analytics Tables ready for sub-second querying.
* **Tables Produced**:

| Table Name | Description | Key KPIs / Metrics |
| :--- | :--- | :--- |
| `gold_batsman_stats` | Batting performance leaderboard | Total Runs, Strike Rate, Batting Average, Fours, Sixes, Highest Score, Dot Ball % |
| `gold_bowler_stats` | Bowling performance leaderboard | Wickets, Economy Rate, Overs Bowled, Bowling Average, Bowling Strike Rate, Dot Ball % |
| `gold_team_stats` | Team-level performance breakdown | Matches Played, Total Runs, Wickets Lost, Powerplay Run Rate, Overall Run Rate |
| `gold_tournament_standings` | Official IPL Points Table | Rank, Matches Played, Won, Lost, Tied, Points ($2 \times W$), Net Run Rate ($\text{NRR}$), Avg Run Rate |

---

## ⚡ Serverless Orchestration & Triggers

1. **Raw Ingestion Trigger**: When a match completes or updates, `ex_match_raw.py` writes the extracted delivery CSV/JSON to S3 Raw/Bronze buckets.
2. **Silver Job Trigger**: S3 Bucket Notification or Lambda triggers `ipl_silver_deliveries_job` (`ex_match_bs.py`).
3. **Gold Job Trigger**: Upon successful completion, the Silver job automatically triggers `ipl_gold_analytics_job` (`ex_match_sg.py`) using `boto3`.

---

## 🛠️ Technology Stack & Dependencies

* **Language**: Python 3.9+
* **Storage**: Amazon S3, Apache Arrow / PyArrow (`==14.0.2`)
* **Table Format**: Delta Lake (`deltalake` Rust engine)
* **ETL & Compute**: AWS Glue (Python Shell), AWS Lambda
* **Query Engine**: Amazon Athena (Native Delta table support via `TBLPROPERTIES ('table_type'='DELTA')`)
* **Data Processing**: Pandas, RapidFuzz, BeautifulSoup4, Requests, Boto3

---

## 📂 Project Structure

```text
IPL-Data-Pipeline/
│
├── pipeline_2026/                    # Production AWS Cloud Pipeline Scripts
│   ├── utils.py                      # Shared S3, Boto3, Selenium, and Logger utilities
│   ├── ex_fixtures.py                # IPL Schedule Ingestion Script
│   ├── ex_players.py                 # Master Player Catalog Scraper
│   ├── ex_match_raw.py               # Live Match Ball-by-Ball Extractor
│   ├── ex_match_bs.py                # Silver Layer AWS Glue ETL Job (Delta Lake)
│   └── ex_match_sg.py                # Gold Layer AWS Glue Analytics Job (Delta Lake)
│
├── pipeline_local/                   # Standalone Local Testing Environment
│   ├── utils.py                      # Local File System Readers & Writers
│   ├── to_raw/                       # Local Raw Extractor
│   ├── to_bronze/                    # Local Bronze Structuring
│   ├── to_silver/slvr_match.py       # Local Silver Delta Transformation
│   └── to_gold/gld_match.py          # Local Gold Analytics Generator
│
├── data/                             # Local Data Directory (Git Ignored)
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── README.md                         # Comprehensive Project Documentation
└── requirements.txt                  # Python Package Dependencies
```

---

## 🚀 AWS Deployment Guide

### 1. AWS Glue Job Parameters
For both `ipl_silver_deliveries_job` and `ipl_gold_analytics_job`, configure the following under **Advanced Properties** $\rightarrow$ **Job Parameters**:

| Key | Value |
| :--- | :--- |
| `--additional-python-modules` | `pyarrow==14.0.2,deltalake,rapidfuzz,pandas` |
| `--S3_RAW` | `ipl-data-2026-raw` |
| `--S3_BRONZE` | `ipl-data-2026-bronze` |
| `--S3_SILVER` | `ipl-data-2026-silver` |
| `--S3_GOLD` | `ipl-data-2026-gold` |
| `--AWS_REGION` | `us-east-1` |

---

## 📊 Querying Data in Amazon Athena

Once Crawled or registered via DDL, run SQL queries in Athena:

### 🏆 IPL 2026 Points Table Query
```sql
SELECT 
    rank,
    team,
    played,
    won,
    lost,
    points,
    net_run_rate,
    avg_run_rate
FROM ipl_db.gold_tournament_standings
ORDER BY rank ASC;
```

### 🏏 Top 10 Orange Cap Run Scorers
```sql
SELECT 
    batsman,
    total_runs,
    legal_balls,
    strike_rate,
    fours,
    sixes,
    highest_score
FROM ipl_db.gold_batsman_stats
ORDER BY total_runs DESC
LIMIT 10;
```

---

## 📝 License
This project is licensed under the MIT License - see the `LICENSE` file for details.
