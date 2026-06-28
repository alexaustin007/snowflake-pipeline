# Snowflake Flight Routes Pipeline

This is an data engineering project built on Snowflake. It takes a raw airline flight routes dataset and moves it through ingestion, transformation, AI enrichment, a live dashboard, and a natural language query interface, all wired together with an orchestration layer.

---

## What it does

Raw flight route data (1.67M records, 24 columns) starts as a CSV on disk. From there it gets loaded into Snowflake through an internal stage, tracked incrementally with a CDC stream, cleaned and transformed through a dbt pipeline, enriched with Cortex AI for route classification and natural language summaries, and surfaced through a Streamlit dashboard hosted inside Snowflake.

On top of that, an MCP server connects Claude Desktop directly to the Snowflake tables so anyone can ask plain English questions and get answers without writing SQL.

---

## Stack

- Snowflake — warehouse, stages, streams, tasks, Cortex AI, Streamlit hosting
- dbt — transformation layer with staging and mart models, schema tests
- Prefect OSS — local orchestration, daily scheduling, observable task runs
- Streamlit in Snowflake — dashboard with filters, charts, world map, AI insights, NL-to-SQL chatbot
- MCP (Model Context Protocol) — Python server connecting Claude Desktop to Snowflake

---

## Data flow

```
routes_sample_data.csv
  -> Snowflake internal stage (snow stage copy)
  -> RAW.RAW_ROUTES (COPY INTO)
  -> RAW.RAW_ROUTES_STREAM (CDC stream, append-only)
  -> CURATED.CLEAN_ROUTES (Snowflake task, incremental)
  -> dbt models (staging -> marts)
  -> AI.ENRICHED_ROUTES (Cortex CLASSIFY_TEXT + COMPLETE)
  -> Reporting views
  -> Streamlit dashboard
  -> MCP server (Claude Desktop plain-English queries)
```

---

## Pipeline phases

**Phase 0 - SQL pipeline**
Sets up the warehouse, schemas, file formats, raw tables, streams, and tasks. All SQL is idempotent using CREATE OR REPLACE. Stream is created before data loads so every row is captured incrementally.

**Phase 1 - Streamlit dashboard**
Dashboard deployed natively inside Snowflake. Includes KPI tiles, bar and pie charts, a PyDeck world map of flight routes, a Cortex AI route insights panel, and an NL-to-SQL chatbot powered by Cortex COMPLETE.

**Phase 2 - Prefect orchestration**
Replaces manual SQL execution with a Python flow. Each step is a named Prefect task with retries and structured logging. Runs on a daily cron schedule with the full pipeline visible in the Prefect UI at localhost:4200.

**Phase 3 - dbt transformation**
7 models across staging and marts folders. Replaces hand-written SQL files for the curated and AI layers. 9 schema tests (not_null, unique, accepted_values) all passing. dbt run and dbt test are called directly from the Prefect flow.

**Phase 4 - MCP server**
A Python MCP server exposes three tools to Claude Desktop: list_tables, describe_table, and query_snowflake. Read-only by design. Credentials loaded from environment variables, not hardcoded.

---

## Running it

Start the Prefect server in one terminal:
```bash
prefect server start
```

Run the full pipeline in another:
```bash
python flows/pipeline_flow.py
```

Run dbt independently:
```bash
cd dbt_flight
../venv/bin/dbt run
../venv/bin/dbt test
```

Deploy the Streamlit dashboard:
```bash
cd streamlit_app
snow streamlit deploy
```

The MCP server starts automatically when Claude Desktop launches, as long as it is registered in the Claude Desktop config file.

---

## Dataset

The sample file committed to this repo is `data/routes_sample_data.csv` with 1001 rows. The full dataset is 1.67M rows across 24 columns covering airline code, flight number, origin and destination airports, cities, countries, coordinates, distance, seats, aircraft type, and flight date.

---

## Credentials

All Snowflake credentials are stored in a local `.env` file.
