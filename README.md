<img width="1254" height="1254" alt="ChatGPT Image Jul 9, 2026, 02_46_16 PM" src="https://github.com/user-attachments/assets/368a35d4-d482-4563-a640-bdf946e894e2" />

# DuckDB Scaffolding for HaloArchives

This script implements a lightweight DuckDB-based [[motherduck]] staging pipeline for HaloArchives data by attaching the source SQLite database through DuckDB’s SQLite extension, dynamically introspecting all available base tables, and materializing them into a normalized `staging` schema. 

## Set Defaults 

Set the defaults

```bash
DEFAULT_SQLITE_DB = Path("data/neon-warthog-cache.sqlite")
DEFAULT_DUCKDB_DB = Path("data/haloarchives.duckdb")
```

This repository contains a deliberately over-structured local staging utility for loading a HaloArchives-style SQLite data source into a DuckDB analytical warehouse layer.

The goal is to preserve the original source archive as an immutable upstream dependency while creating a separate DuckDB-backed staging layer for exploration, validation, transformation, and downstream OLAP-style querying.

## What This Does

The staging script:

- Attaches a source SQLite database into DuckDB
- Discovers source tables dynamically
- Materializes each discovered table into a DuckDB `staging` schema
- Adds stage metadata columns to each landed table
- Tracks run-level and table-level audit information
- Captures source column manifests
- Creates utility views for inspection
- Writes a JSON manifest for each staging execution
- Supports full reloads, selective table reloads, and no-overwrite mode

## Default File Layout

```text
.
├── data/
│   ├── blam-fragment-store.sqlite
│   ├── haloarchives.duckdb
│   └── stage_manifest.json
├── stage_haloarchives_duckdb.py
└── README.md
```

## JSON Manifest

Every run writes a JSON manifest to:

```bash
data/stage_manifest.json
```

The manifest includes:

```text
Run ID
Source SQLite path
Target DuckDB path
Staging schema
Metadata schema
Number of discovered tables
Number of selected tables
Successful tables
Skipped tables
Failed tables
Table-level row counts
Table-level durations
Error messages, if any
```

Example:

```json
{
  "run_id": "7e8f7b73e0a6a0f1",
  "source_sqlite_db": "data/blam-fragment-store.sqlite",
  "target_duckdb_db": "data/haloarchives.duckdb",
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

## Why DuckDB?

DuckDB is useful here because it can act as a lightweight analytical warehouse without requiring a server. It also supports direct SQLite attachment, columnar-style analytical queries, local persistence, and fast inspection of staged datasets.
