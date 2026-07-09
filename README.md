# DuckDB-HA

<img width="1254" height="1254" alt="ChatGPT Image Jul 9, 2026, 02_46_16 PM" src="https://github.com/user-attachments/assets/368a35d4-d482-4563-a640-bdf946e894e2" />


**DuckDB-HA** is a deliberately over-engineered staging pipeline for loading a HaloArchives-style SQLite archive into either a local DuckDB analytical database or a MotherDuck-hosted DuckDB database.

It turns a source SQLite file into a query-friendly staging layer with audit metadata, source manifests, table inventory views, and JSON run output.

## Overview

DuckDB-HA is designed around a simple staging philosophy:

```text
source archive -> staging layer -> downstream transformations
```

The source SQLite archive remains untouched.

DuckDB or MotherDuck becomes the analytical workspace.

The staging layer is useful for:

* Local data exploration
* Archive inspection
* Table-level validation
* Reproducible staging runs
* Lightweight OLAP-style querying
* Future transformation pipelines
* MotherDuck-backed cloud querying

## Default File Layout

```text
.
├── assets/
│   └── duckdb-ha-logo.png
├── data/
│   ├── blam-fragment-store.sqlite
│   ├── haloarchives.duckdb
│   └── stage_manifest.json
├── stage_haloarchives_duckdb.py
└── README.md
```

The default source SQLite file is intentionally named:

```text
data/blam-fragment-store.sqlite
```

The default local DuckDB output file is:

```text
data/haloarchives.duckdb
```

The default JSON manifest path is:

```text
data/stage_manifest.json
```

## What This Does

The staging script:

* Attaches a source SQLite database into DuckDB
* Discovers source tables dynamically
* Materializes each discovered table into a staging schema
* Adds stage metadata columns to each landed table
* Tracks run-level and table-level audit metadata
* Captures source column manifests
* Creates utility views for inspection
* Writes a JSON manifest for every run
* Supports full reloads
* Supports selective table reloads
* Supports no-overwrite mode
* Supports local DuckDB targets
* Supports MotherDuck cloud targets

## Requirements

Python 3.10 or newer is recommended.

Install DuckDB:

```bash
pip install duckdb
```

The script uses DuckDB's SQLite extension so it can attach and query SQLite databases directly.

## Quick Start

Create the expected data directory:

```bash
mkdir -p data
```

Place your source SQLite archive here:

```text
data/blam-fragment-store.sqlite
```

Run the staging pipeline:

```bash
python3 stage_haloarchives_duckdb.py
```

This creates or updates:

```text
data/haloarchives.duckdb
```

It also writes a JSON run manifest to:

```text
data/stage_manifest.json
```

## Local DuckDB Usage

Local DuckDB is the default target.

```bash
python3 stage_haloarchives_duckdb.py
```

Equivalent explicit command:

```bash
python3 stage_haloarchives_duckdb.py \
  --target local \
  --sqlite-db data/blam-fragment-store.sqlite \
  --duckdb-db data/haloarchives.duckdb
```

## MotherDuck Support

DuckDB-HA can also stage the local HaloArchives SQLite source into a MotherDuck-hosted database.

MotherDuck mode is enabled with:

```bash
python3 stage_haloarchives_duckdb.py --target motherduck
```

<img width="638" height="391" alt="image" src="https://github.com/user-attachments/assets/5a83af4d-6c49-4a73-9cc5-cdc2e91d473e" />


## MotherDuck Authentication

MotherDuck can authenticate through the lowercase environment variable:

```bash
export motherduck_token="your_motherduck_token"
```

The script also supports the common uppercase local variable:

```bash
export MOTHERDUCK_TOKEN="your_motherduck_token"
```

If `motherduck_token` is not already set, the script will copy `MOTHERDUCK_TOKEN` into `motherduck_token` before connecting.

## Default MotherDuck Database

By default, MotherDuck staging writes to:

```text
md:duckdb_ha
```

Override the MotherDuck database name:

```bash
python3 stage_haloarchives_duckdb.py \
  --target motherduck \
  --motherduck-db haloarchives_cloud
```

## Full MotherDuck Run

```bash
export MOTHERDUCK_TOKEN="your_motherduck_token"

python3 stage_haloarchives_duckdb.py \
  --target motherduck \
  --motherduck-db duckdb_ha \
  --sqlite-db data/blam-fragment-store.sqlite \
  --verbose
```

## Stage Specific Tables

Stage one table locally:

```bash
python3 stage_haloarchives_duckdb.py --table games
```

Stage multiple tables locally:

```bash
python3 stage_haloarchives_duckdb.py \
  --table games \
  --table player_stats \
  --table gamertags
```

Stage specific tables to MotherDuck:

```bash
python3 stage_haloarchives_duckdb.py \
  --target motherduck \
  --motherduck-db duckdb_ha \
  --table games \
  --table player_stats
```

## Disable Overwrite

By default, the script replaces existing staging tables.

To skip tables that already exist:

```bash
python3 stage_haloarchives_duckdb.py --no-overwrite
```

## Verbose Logging

```bash
python3 stage_haloarchives_duckdb.py --verbose
```

## Custom Schemas

The default staging schema is:

```text
staging
```

The default internal metadata schema is:

```text
_halo_internal
```

Override them like this:

```bash
python3 stage_haloarchives_duckdb.py \
  --staging-schema raw_stage \
  --metadata-schema _archive_ops
```

## Output Schemas

After a successful run, the target database contains two main schemas.

## `staging`

This contains the materialized source tables.

Each staged table receives two additional metadata columns:

```text
_ha_stage_run_id
_ha_stage_loaded_at_utc
```

These identify the staging run and the load timestamp.

## `_halo_internal`

This contains operational metadata tables:

```text
_halo_internal.stage_audit
_halo_internal.source_manifest
```

## Utility Views

The script creates utility views inside the staging schema.

## `staging.v_stage_table_inventory`

Lists staged relations visible in the staging schema.

```sql
SELECT *
FROM staging.v_stage_table_inventory;
```

## `staging.v_latest_stage_audit`

Shows the latest audit record for each staged source table.

```sql
SELECT *
FROM staging.v_latest_stage_audit;
```

## `staging.v_source_manifest_latest`

Shows the latest captured source-column manifest.

```sql
SELECT *
FROM staging.v_source_manifest_latest;
```

## Inspecting the Local DuckDB Database

Open DuckDB locally:

```bash
duckdb data/haloarchives.duckdb
```

Show staged tables:

```sql
SHOW TABLES FROM staging;
```

Inspect audit records:

```sql
SELECT *
FROM staging.v_latest_stage_audit;
```

Inspect source manifests:

```sql
SELECT *
FROM staging.v_source_manifest_latest;
```

Count rows from a staged table:

```sql
SELECT COUNT(*)
FROM staging.games;
```

## Inspecting the MotherDuck Database

Connect to the MotherDuck database:

```bash
duckdb md:duckdb_ha
```

Then inspect:

```sql
SHOW TABLES FROM staging;

SELECT *
FROM staging.v_latest_stage_audit;

SELECT *
FROM staging.v_source_manifest_latest;
```

## JSON Manifest

Every run writes a JSON manifest to:

```text
data/stage_manifest.json
```

The manifest includes:

* Run ID
* Target mode
* Target database
* Source SQLite path
* Local DuckDB path
* MotherDuck database name
* Source alias
* Staging schema
* Metadata schema
* Completion timestamp
* Overwrite mode
* Number of discovered tables
* Number of selected tables
* Successful tables
* Skipped tables
* Failed tables
* Table-level row counts
* Table-level durations
* Error messages, if any

Example manifest:

```json
{
  "run_id": "7e8f7b73e0a6a0f1",
  "target_mode": "local",
  "target_database": "data/haloarchives.duckdb",
  "source_sqlite_db": "data/blam-fragment-store.sqlite",
  "local_duckdb_db": "data/haloarchives.duckdb",
  "motherduck_db": "duckdb_ha",
  "source_alias": "halo_src",
  "staging_schema": "staging",
  "metadata_schema": "_halo_internal",
  "completed_at_utc": "2026-07-09T16:21:00.000000+00:00",
  "overwrite": true,
  "tables_discovered": 3,
  "tables_selected": 3,
  "successful_tables": 3,
  "skipped_tables": 0,
  "failed_tables": 0
}
```

## CLI Reference

```text
--sqlite-db PATH
    Path to the source SQLite database.
    Default: data/blam-fragment-store.sqlite

--target local|motherduck
    Target backend.
    Default: local

--duckdb-db PATH
    Path to the local DuckDB database.
    Default: data/haloarchives.duckdb

--motherduck-db NAME
    MotherDuck database name.
    Default: duckdb_ha

--motherduck-token-env NAME
    Environment variable containing a MotherDuck token.
    Default: MOTHERDUCK_TOKEN

--source-alias NAME
    Logical alias used when attaching the SQLite source.
    Default: halo_src

--staging-schema NAME
    Schema where staged tables are materialized.
    Default: staging

--metadata-schema NAME
    Schema where audit and manifest metadata are stored.
    Default: _halo_internal

--table NAME
    Stage only a specific source table.
    Can be provided multiple times.

--no-overwrite
    Skip materializing tables that already exist in the staging schema.

--manifest-json PATH
    Path where the JSON run manifest is written.
    Default: data/stage_manifest.json

--verbose
    Enable verbose debug logging.
```

## Exit Codes

```text
0 = staging completed without table failures
1 = one or more tables failed
2 = fatal pipeline failure
```

## Local Example Run

```bash
mkdir -p data

python3 stage_haloarchives_duckdb.py --verbose

duckdb data/haloarchives.duckdb
```

Inside DuckDB:

```sql
SHOW TABLES FROM staging;

SELECT *
FROM staging.v_latest_stage_audit;

SELECT *
FROM staging.v_source_manifest_latest;
```

## MotherDuck Example Run

```bash
export MOTHERDUCK_TOKEN="your_motherduck_token"

python3 stage_haloarchives_duckdb.py \
  --target motherduck \
  --motherduck-db duckdb_ha \
  --sqlite-db data/blam-fragment-store.sqlite \
  --verbose
```

Then connect:

```bash
duckdb md:duckdb_ha
```

Inside MotherDuck:

```sql
SHOW TABLES FROM staging;

SELECT *
FROM staging.v_latest_stage_audit;

SELECT *
FROM staging.v_source_manifest_latest;
```

## Why DuckDB?

DuckDB is useful here because it acts as a lightweight analytical warehouse without requiring a server.

It supports:

* Local persistent databases
* Fast analytical querying
* SQLite attachment
* Simple SQL workflows
* Portable `.duckdb` files
* Smooth upgrade path to MotherDuck

## Why MotherDuck?

MotherDuck gives the same DuckDB-style workflow a cloud target.

That makes the staged HaloArchives data easier to query across machines, share between environments, or use as a lightweight hosted analytical backend.

## Project Philosophy

DuckDB-HA intentionally treats staging as a first-class layer.

The SQLite file is the archive.

DuckDB or MotherDuck is the analytical workspace.

The metadata schema is the operational record.

The staging schema is the clean landing zone for future transformations, validation, dashboards, exports, or archive tooling.
