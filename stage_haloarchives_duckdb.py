#!/usr/bin/env python3
"""
stage_haloarchives_duckdb.py

A deliberately over-engineered local staging utility for moving HaloArchives-style
SQLite data into a DuckDB analytical staging layer.

Default source:
    data/blam-fragment-store.sqlite

Default target:
    data/haloarchives.duckdb
"""

import argparse
import hashlib
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb


DEFAULT_SQLITE_DB = Path("data/blam-fragment-store.sqlite")
DEFAULT_DUCKDB_DB = Path("data/haloarchives.duckdb")
DEFAULT_MANIFEST_JSON = Path("data/stage_manifest.json")


@dataclass(frozen=True)
class SourceTable:
    catalog: str | None
    schema: str | None
    name: str


@dataclass
class StageResult:
    run_id: str
    source_catalog: str | None
    source_schema: str | None
    source_table: str
    target_schema: str
    target_table: str
    row_count: int
    column_count: int
    staged_at_utc: str
    duration_ms: int
    status: str
    error: str | None = None


def quote_ident(identifier: str) -> str:
    """
    Safely quote a DuckDB identifier such as a schema, table, or column name.
    """
    return '"' + identifier.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """
    Safely quote a SQL string literal.
    """
    return "'" + value.replace("'", "''") + "'"


def quote_path(path: Path) -> str:
    """
    Safely quote a filesystem path for ATTACH statements.
    """
    return str(path).replace("'", "''")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_run_id(sqlite_db: Path) -> str:
    raw = f"{sqlite_db.resolve()}::{now_utc()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def load_sqlite_extension(con: duckdb.DuckDBPyConnection) -> None:
    """
    DuckDB needs the sqlite extension in order to attach SQLite databases.

    Try LOAD first in case the extension is already installed locally.
    If that fails, install it and then load it.
    """
    try:
        con.execute("LOAD sqlite;")
        logging.debug("Loaded DuckDB sqlite extension")
    except Exception:
        logging.debug("sqlite extension not loaded yet; installing")
        con.execute("INSTALL sqlite;")
        con.execute("LOAD sqlite;")


def connect_duckdb(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(db_path))
    load_sqlite_extension(con)

    return con


def attach_sqlite_source(
    con: duckdb.DuckDBPyConnection,
    sqlite_path: Path,
    source_alias: str,
) -> None:
    if not sqlite_path.exists():
        raise FileNotFoundError(f"Missing SQLite source database: {sqlite_path}")

    con.execute(
        f"""
        ATTACH '{quote_path(sqlite_path)}'
        AS {quote_ident(source_alias)}
        (TYPE sqlite);
        """
    )

    logging.info("Attached SQLite source as %s", source_alias)


def source_table_ref(table: SourceTable) -> str:
    """
    Build a fully qualified table reference.

    Usually attached SQLite tables appear as:
        "halo_src"."main"."table_name"

    Some DuckDB versions / attachment modes may expose them differently,
    so this function supports two-part and three-part references.
    """
    parts = [part for part in [table.catalog, table.schema, table.name] if part]
    return ".".join(quote_ident(part) for part in parts)


def target_table_ref(schema: str, table: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(table)}"


def discover_source_tables(
    con: duckdb.DuckDBPyConnection,
    source_alias: str,
) -> list[SourceTable]:
    """
    Discover source tables from the attached SQLite database.

    Preferred path:
        information_schema.tables where table_catalog = attached alias

    Fallback path:
        information_schema.tables where table_schema = attached alias
    """
    catalog_rows = con.execute(
        """
        SELECT
            table_catalog,
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_catalog = ?
          AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'sqlite_%'
        ORDER BY table_name;
        """,
        [source_alias],
    ).fetchall()

    if catalog_rows:
        return [
            SourceTable(catalog=row[0], schema=row[1], name=row[2])
            for row in catalog_rows
        ]

    schema_rows = con.execute(
        """
        SELECT
            table_schema,
            table_name
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_type = 'BASE TABLE'
          AND table_name NOT LIKE 'sqlite_%'
        ORDER BY table_name;
        """,
        [source_alias],
    ).fetchall()

    if schema_rows:
        return [
            SourceTable(catalog=None, schema=row[0], name=row[1])
            for row in schema_rows
        ]

    return []


def get_source_columns(
    con: duckdb.DuckDBPyConnection,
    source_table: SourceTable,
) -> list[tuple[str, str]]:
    """
    Inspect columns by describing a SELECT from the source relation.

    This avoids relying too much on the exact catalog/schema layout exposed
    by DuckDB for attached SQLite databases.
    """
    rows = con.execute(
        f"""
        DESCRIBE SELECT *
        FROM {source_table_ref(source_table)};
        """
    ).fetchall()

    return [(row[0], row[1]) for row in rows]


def create_schema_if_missing(
    con: duckdb.DuckDBPyConnection,
    schema_name: str,
) -> None:
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(schema_name)};")


def table_exists(
    con: duckdb.DuckDBPyConnection,
    schema_name: str,
    table_name: str,
) -> bool:
    result = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
          AND table_type = 'BASE TABLE';
        """,
        [schema_name, table_name],
    ).fetchone()[0]

    return result > 0


def unique_column_name(existing_columns: list[str], preferred_name: str) -> str:
    """
    Avoid metadata-column collisions with source data.
    """
    existing_lower = {column.lower() for column in existing_columns}

    if preferred_name.lower() not in existing_lower:
        return preferred_name

    counter = 1

    while True:
        candidate = f"{preferred_name}_{counter}"

        if candidate.lower() not in existing_lower:
            return candidate

        counter += 1


def ensure_internal_metadata_tables(
    con: duckdb.DuckDBPyConnection,
    metadata_schema: str,
) -> None:
    create_schema_if_missing(con, metadata_schema)

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ident(metadata_schema)}.stage_audit (
            run_id TEXT,
            source_catalog TEXT,
            source_schema TEXT,
            source_table TEXT,
            target_schema TEXT,
            target_table TEXT,
            row_count BIGINT,
            column_count BIGINT,
            staged_at_utc TIMESTAMP,
            duration_ms BIGINT,
            status TEXT,
            error TEXT
        );
        """
    )

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {quote_ident(metadata_schema)}.source_manifest (
            run_id TEXT,
            source_catalog TEXT,
            source_schema TEXT,
            source_table TEXT,
            column_name TEXT,
            source_data_type TEXT,
            discovered_at_utc TIMESTAMP
        );
        """
    )


def write_source_manifest(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    source_table: SourceTable,
    metadata_schema: str,
    columns: list[tuple[str, str]],
) -> None:
    discovered_at = now_utc()

    for column_name, source_data_type in columns:
        con.execute(
            f"""
            INSERT INTO {quote_ident(metadata_schema)}.source_manifest
            VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            [
                run_id,
                source_table.catalog,
                source_table.schema,
                source_table.name,
                column_name,
                source_data_type,
                discovered_at,
            ],
        )


def write_stage_audit(
    con: duckdb.DuckDBPyConnection,
    metadata_schema: str,
    result: StageResult,
) -> None:
    con.execute(
        f"""
        INSERT INTO {quote_ident(metadata_schema)}.stage_audit
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """,
        [
            result.run_id,
            result.source_catalog,
            result.source_schema,
            result.source_table,
            result.target_schema,
            result.target_table,
            result.row_count,
            result.column_count,
            result.staged_at_utc,
            result.duration_ms,
            result.status,
            result.error,
        ],
    )


def stage_single_table(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    source_table: SourceTable,
    staging_schema: str,
    metadata_schema: str,
    overwrite: bool,
) -> StageResult:
    started = time.perf_counter()
    staged_at = now_utc()

    target_table = source_table.name
    target_ref = target_table_ref(staging_schema, target_table)

    try:
        columns = get_source_columns(con, source_table)
        existing_column_names = [column_name for column_name, _ in columns]

        stage_run_id_column = unique_column_name(
            existing_column_names,
            "_ha_stage_run_id",
        )

        stage_loaded_at_column = unique_column_name(
            existing_column_names + [stage_run_id_column],
            "_ha_stage_loaded_at_utc",
        )

        if not overwrite and table_exists(con, staging_schema, target_table):
            row_count = con.execute(
                f"""
                SELECT COUNT(*)
                FROM {target_ref};
                """
            ).fetchone()[0]

            duration_ms = int((time.perf_counter() - started) * 1000)

            result = StageResult(
                run_id=run_id,
                source_catalog=source_table.catalog,
                source_schema=source_table.schema,
                source_table=source_table.name,
                target_schema=staging_schema,
                target_table=target_table,
                row_count=row_count,
                column_count=len(columns),
                staged_at_utc=staged_at,
                duration_ms=duration_ms,
                status="SKIPPED",
                error="Target table already exists and overwrite is disabled",
            )

            write_stage_audit(con, metadata_schema, result)
            return result

        con.execute("BEGIN TRANSACTION;")

        write_source_manifest(
            con=con,
            run_id=run_id,
            source_table=source_table,
            metadata_schema=metadata_schema,
            columns=columns,
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE {target_ref} AS
            SELECT
                *,
                {quote_literal(run_id)}::TEXT AS {quote_ident(stage_run_id_column)},
                current_timestamp AS {quote_ident(stage_loaded_at_column)}
            FROM {source_table_ref(source_table)};
            """
        )

        row_count = con.execute(
            f"""
            SELECT COUNT(*)
            FROM {target_ref};
            """
        ).fetchone()[0]

        duration_ms = int((time.perf_counter() - started) * 1000)

        result = StageResult(
            run_id=run_id,
            source_catalog=source_table.catalog,
            source_schema=source_table.schema,
            source_table=source_table.name,
            target_schema=staging_schema,
            target_table=target_table,
            row_count=row_count,
            column_count=len(columns),
            staged_at_utc=staged_at,
            duration_ms=duration_ms,
            status="SUCCESS",
        )

        write_stage_audit(con, metadata_schema, result)

        con.execute("COMMIT;")

        return result

    except Exception as exc:
        try:
            con.execute("ROLLBACK;")
        except Exception:
            pass

        duration_ms = int((time.perf_counter() - started) * 1000)

        result = StageResult(
            run_id=run_id,
            source_catalog=source_table.catalog,
            source_schema=source_table.schema,
            source_table=source_table.name,
            target_schema=staging_schema,
            target_table=target_table,
            row_count=0,
            column_count=0,
            staged_at_utc=staged_at,
            duration_ms=duration_ms,
            status="FAILED",
            error=str(exc),
        )

        write_stage_audit(con, metadata_schema, result)

        return result


def create_quality_views(
    con: duckdb.DuckDBPyConnection,
    staging_schema: str,
    metadata_schema: str,
) -> None:
    """
    Create small utility views that make the staged layer feel more warehouse-like.
    """
    create_schema_if_missing(con, staging_schema)

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {quote_ident(staging_schema)}.v_stage_table_inventory AS
        SELECT
            table_catalog,
            table_schema,
            table_name,
            table_type
        FROM information_schema.tables
        WHERE table_schema = {quote_literal(staging_schema)}
        ORDER BY table_name;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {quote_ident(staging_schema)}.v_latest_stage_audit AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY source_table
                    ORDER BY staged_at_utc DESC
                ) AS audit_rank
            FROM {quote_ident(metadata_schema)}.stage_audit
        )
        SELECT
            run_id,
            source_catalog,
            source_schema,
            source_table,
            target_schema,
            target_table,
            row_count,
            column_count,
            staged_at_utc,
            duration_ms,
            status,
            error
        FROM ranked
        WHERE audit_rank = 1
        ORDER BY source_table;
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE VIEW {quote_ident(staging_schema)}.v_source_manifest_latest AS
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY source_table, column_name
                    ORDER BY discovered_at_utc DESC
                ) AS manifest_rank
            FROM {quote_ident(metadata_schema)}.source_manifest
        )
        SELECT
            run_id,
            source_catalog,
            source_schema,
            source_table,
            column_name,
            source_data_type,
            discovered_at_utc
        FROM ranked
        WHERE manifest_rank = 1
        ORDER BY source_table, column_name;
        """
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Stage a HaloArchives-style SQLite source into a DuckDB analytical "
            "landing zone with audit metadata, source manifests, quality views, "
            "transactional table materialization, and JSON run output."
        )
    )

    parser.add_argument(
        "--sqlite-db",
        type=Path,
        default=DEFAULT_SQLITE_DB,
        help=f"Path to the source SQLite database. Default: {DEFAULT_SQLITE_DB}",
    )

    parser.add_argument(
        "--duckdb-db",
        type=Path,
        default=DEFAULT_DUCKDB_DB,
        help=f"Path to the target DuckDB database. Default: {DEFAULT_DUCKDB_DB}",
    )

    parser.add_argument(
        "--source-alias",
        default="halo_src",
        help="Logical alias used when attaching the SQLite source.",
    )

    parser.add_argument(
        "--staging-schema",
        default="staging",
        help="DuckDB schema where staged tables will be materialized.",
    )

    parser.add_argument(
        "--metadata-schema",
        default="_halo_internal",
        help="DuckDB schema where audit and manifest metadata will be stored.",
    )

    parser.add_argument(
        "--table",
        action="append",
        dest="tables",
        help="Stage only a specific source table. Can be provided multiple times.",
    )

    parser.add_argument(
        "--no-overwrite",
        action="store_true",
        help="Skip materializing tables that already exist in the staging schema.",
    )

    parser.add_argument(
        "--manifest-json",
        type=Path,
        default=DEFAULT_MANIFEST_JSON,
        help=f"Write a JSON run manifest to this location. Default: {DEFAULT_MANIFEST_JSON}",
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose debug logging.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    run_id = build_run_id(args.sqlite_db)
    overwrite = not args.no_overwrite

    con: duckdb.DuckDBPyConnection | None = None
    results: list[StageResult] = []

    logging.info("Starting HaloArchives DuckDB staging run")
    logging.info("Run ID: %s", run_id)
    logging.info("Source SQLite: %s", args.sqlite_db)
    logging.info("Target DuckDB: %s", args.duckdb_db)
    logging.info("Overwrite enabled: %s", overwrite)

    try:
        con = connect_duckdb(args.duckdb_db)

        attach_sqlite_source(
            con=con,
            sqlite_path=args.sqlite_db,
            source_alias=args.source_alias,
        )

        create_schema_if_missing(con, args.staging_schema)

        ensure_internal_metadata_tables(
            con=con,
            metadata_schema=args.metadata_schema,
        )

        discovered_tables = discover_source_tables(
            con=con,
            source_alias=args.source_alias,
        )

        if not discovered_tables:
            logging.warning("No source tables discovered")
            tables_to_stage = []
        elif args.tables:
            requested = set(args.tables)
            tables_to_stage = [
                table for table in discovered_tables
                if table.name in requested
            ]

            discovered_names = {table.name for table in discovered_tables}
            missing = sorted(requested - discovered_names)

            if missing:
                logging.warning(
                    "Requested tables not found in source: %s",
                    ", ".join(missing),
                )
        else:
            tables_to_stage = discovered_tables

        logging.info("Discovered %d source tables", len(discovered_tables))
        logging.info("Selected %d tables for staging", len(tables_to_stage))

        for source_table in tables_to_stage:
            logging.info("Staging table: %s", source_table.name)

            result = stage_single_table(
                con=con,
                run_id=run_id,
                source_table=source_table,
                staging_schema=args.staging_schema,
                metadata_schema=args.metadata_schema,
                overwrite=overwrite,
            )

            results.append(result)

            if result.status == "SUCCESS":
                logging.info(
                    "SUCCESS table=%s rows=%s columns=%s duration_ms=%s",
                    result.target_table,
                    result.row_count,
                    result.column_count,
                    result.duration_ms,
                )
            elif result.status == "SKIPPED":
                logging.info(
                    "SKIPPED table=%s reason=%s",
                    result.target_table,
                    result.error,
                )
            else:
                logging.error(
                    "FAILED table=%s error=%s",
                    result.source_table,
                    result.error,
                )

        create_quality_views(
            con=con,
            staging_schema=args.staging_schema,
            metadata_schema=args.metadata_schema,
        )

        args.manifest_json.parent.mkdir(parents=True, exist_ok=True)

        successful_count = len([r for r in results if r.status == "SUCCESS"])
        skipped_count = len([r for r in results if r.status == "SKIPPED"])
        failed_count = len([r for r in results if r.status == "FAILED"])

        manifest = {
            "run_id": run_id,
            "source_sqlite_db": str(args.sqlite_db),
            "target_duckdb_db": str(args.duckdb_db),
            "source_alias": args.source_alias,
            "staging_schema": args.staging_schema,
            "metadata_schema": args.metadata_schema,
            "completed_at_utc": now_utc(),
            "overwrite": overwrite,
            "tables_discovered": len(discovered_tables),
            "tables_selected": len(tables_to_stage),
            "successful_tables": successful_count,
            "skipped_tables": skipped_count,
            "failed_tables": failed_count,
            "results": [asdict(result) for result in results],
        }

        args.manifest_json.write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )

        logging.info("Wrote JSON manifest: %s", args.manifest_json)
        logging.info("Completed HaloArchives DuckDB staging run")

        return 1 if failed_count else 0

    except Exception as exc:
        logging.exception("Fatal staging failure: %s", exc)
        return 2

    finally:
        if con is not None:
            con.close()


if __name__ == "__main__":
    sys.exit(main())
