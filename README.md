# 🏏 IPL Data Pipeline Project (AWS + Python)

This project is an end-to-end data engineering pipeline that collects, processes, transforms, and analyzes **IPL ball-by-ball match data** in near real-time using **AWS services** and **Python**. The final dataset powers a dashboard built in Tableau or Power BI.

---

## 📌 Project Objective

Build a robust and scalable pipeline that:

* Scrapes real-time IPL match data from Cricbuzz
* Stores data using the Medallion Architecture (Raw → Bronze → Silver → Gold)
* Uses AWS services (S3, Lambda, Glue, etc.) for serverless data processing
* Outputs aggregated insights for use in dashboards and analytics

---

## 🗺️ Architecture Overview

**Technologies Used:**

* **AWS S3** – Data Lake storage (Raw, Bronze, Silver, Gold layers)
* **AWS Lambda** – Serverless ingestion and transformation triggers
* **AWS Glue** – ETL for Silver and Gold layer transformations
* **Athena** – SQL queries over S3 data
* **Delta Lake** – ACID-compliant table format used for Silver and Gold layers
* **Databricks (optional)** – Delta Lake transformations
* **Python** – Used for extraction, cleaning, and orchestration
* **Selenium** – Scraping Cricbuzz website
* **RapidFuzz** – Fuzzy name correction

---

## 🧱 Step-by-Step Pipeline Breakdown

### 1. **Data Ingestion (Raw Layer)**

* **Python + Selenium** used to scrape match schedule, player info, and ball-by-ball commentary from Cricbuzz.
* Data saved to S3 in `raw/` directory, organized by type and match ID.
* **S3 Event Trigger** invokes an **AWS Lambda function** upon new file upload.

### 2. **Initial Structuring (Bronze Layer)**

* Lambda function or local Python script reads from `raw/` and transforms nested JSON into flat structured JSON.
* Output is stored in `bronze/ball-by-ball/{match_id}/...`
* Applied transformations:

  * Extract delivery-level fields
  * Standardize timestamp, team names
  * Deduplicate deliveries

### 3. **Data Cleaning and Enrichment (Silver Layer)**

* **AWS Glue** job scheduled at 11:45 PM IST daily.
* Reads only today’s partitioned data from Bronze.
* Joins player and match metadata.
* Corrects misspelled names using **RapidFuzz**.
* Adds derived fields: `innings_phase`, `over_decimal`, `rebowl_flag`, etc.
* Stores output as **Delta Lake tables** partitioned by `match_code` and `date`
* **Registers Delta tables** in the **Glue Catalog** for downstream use.

### 4. **Match Results Table**

* Separate Glue job creates `silver_match_results` with one row per match.
* Includes: winner, result, margin, toss info, DLS flag.

### 5. **Gold Layer Aggregations**

* Aggregates and materializes Gold layer tables:

  * `fact_batting`, `fact_bowling`, `fact_match_summary`, `fact_team_performance`, `fact_points`
* Includes derived metrics:

  * Strike rate, economy rate, batting position, win flags, NRR, etc.
* **Uses incremental upserts (MERGE INTO)** for fact tables to avoid full overwrites
* All tables are saved in **Delta format** and **registered in Glue Catalog**

### 6. **Points Table Generation**

* Created with Athena/SQL using `silver_match_results`
* Aggregates matches, wins, no-results, points, and **Net Run Rate (NRR)**

---

## 📊 Dashboard Use (Yet to complete)

* Output tables from Gold layer are connected to **Tableau** or **Power BI** using **Athena connector** or **JDBC/ODBC** via Glue Catalog.

---

## 🧾 Tables Created

### ✅ Silver Layer Tables

* `silver_deliveries` – All delivery-level structured and enriched match data

### ✅ Gold Layer Tables (yet to complete)

**Dimensions:**

* `dim_players` – Player name, team, nationality, playing style, etc.
* `dim_teams` – List of participating IPL teams
* `dim_matches` – Match metadata like venue, date, match_code, teams

**Facts:**

* `fact_deliveries` – Granular ball-by-ball data for performance analytics
* `fact_batsman_stats` – Match-wise player performance with runs, strike rate, boundaries
* `fact_bowler_stats` – Match-wise bowler performance with economy, wickets, overs
* `fact_points` – Points table with matches played, wins, losses, NRR, total points

All tables are stored in **Delta Lake format** and **registered in the Glue Catalog** for querying via Athena or BI tools.

---

## ✅ Summary

This project demonstrates a modern data engineering stack using **Python and AWS** to build a scalable, maintainable pipeline for sports analytics, ready to power advanced dashboards and analytics.

It now includes:

* **Delta Lake** ACID capabilities
* **Incremental MERGE logic** to avoid recomputation
* **Glue Catalog integration** for full lakehouse-style analytics
