# duckdb-haloarchives
This script implements a lightweight DuckDB-based staging pipeline for HaloArchives data by attaching the source SQLite database through DuckDB’s SQLite extension, dynamically introspecting all available base tables, and materializing them into a normalized `staging` schema. 
