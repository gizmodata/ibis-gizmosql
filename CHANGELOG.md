# Changelog

All notable changes to ibis-gizmosql will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

#### Driver Upgrade
- Upgraded `adbc-driver-gizmosql` from `0.1.*` to `1.0.*`
- Removed direct `pyarrow` dependency — now bundled transitively via the driver

#### DDL/DML Execution
- Migrated all DDL/DML statements from `cursor.execute(...).fetchall()` to `gizmosql.execute_update(cursor, ...)` — the declarative API that fires statements immediately on the server without requiring a fetch round-trip
- Added `_execute_ddl()` helper method for one-line DDL/DML execution
- Affected methods: `_Settings.__setitem__`, `create_table`, `create_database`, `drop_database`, `_load_extensions`, `attach`, `detach`, `attach_sqlite`, `to_xlsx`, `_create_temp_view`, `_register_in_memory_table`

### Added

#### External Authentication (OAuth/SSO)
- `do_connect()` now accepts `auth_type="external"` for OAuth/SSO authentication flows, along with `oauth_port`, `oauth_timeout`, and `open_browser` parameters
- URL-based connection supports `authType`, `oauthPort`, `oauthTimeout`, and `openBrowser` query parameters

## [0.1.1] - 2026-02-12

### Added

#### Local File Loading via ADBC Bulk Ingest
- `read_csv`, `read_parquet`, `read_json`, and `read_delta` now read files **locally** using an ephemeral DuckDB connection and stream Arrow record batches to GizmoSQL via ADBC bulk ingest
- Enables loading data from the **client's local filesystem** to a remote GizmoSQL server — previously these methods sent file paths as SQL to the server, which only worked for server-accessible paths
- Configurable `batch_size` parameter (default 10,000 rows) on all `read_*` methods to control Arrow record batch size during ingest
- `_register_in_memory_table` now also uses batched ingest via `RecordBatchReader` for large in-memory tables
- New test suite (`tests/test_local_ingest.py`) with 10 tests covering CSV, Parquet, JSON, batching, multi-file, and ibis operations on ingested data

## [0.1.0] - 2026-02-12

### Added

#### Ibis 12 Upgrade & Comprehensive Test Suite
- Upgraded from ibis-framework 11.x to **ibis-framework 12.0.x**
- Brought the full ibis v12 upstream test suite to the GizmoSQL backend: **1664 tests passing** across 24 test modules (aggregation, arrays, binary, client, column, conditionals, dot_sql, export, generic, impure, interactive, join, JSON, map, numeric, param, set_ops, SQL, string, struct, temporal, UUID, window)
- 50 files changed, 1426 insertions

#### TPC-H Support
- Backend now reports `supports_tpch = True`
- Added `load_tpch()` method using DuckDB's built-in `CALL dbgen(sf=0.17)`
- Backend marked as `stateful = True` for correct shared-server test semantics

#### Complex Type Support (ADBC Ingest)
- Full support for ingesting complex/nested Arrow types via ADBC bulk ingest: `LIST`, `STRUCT`, `MAP`, and `ARRAY` types
- `_normalize_arrow_schema` downcasts `LARGE_STRING`, `LARGE_BINARY`, `LARGE_UTF8` for Flight SQL compatibility
- Batched ingest in test fixtures to stay under gRPC's 16 MB message size limit

#### SQL Snapshot Tests
- Added GizmoSQL-specific SQL snapshots for: `test_cte_refs_in_topo_order`, `test_group_by_has_index`, `test_isin_bug`, `test_many_subqueries`, `test_mixed_qualified_and_unqualified_predicates`, `test_order_by_no_deference_literals`, `test_rewrite_context`, `test_sample`, `test_union_aliasing`, `test_temporal_literal_sql`, `test_time_literal_sql`, and others

### Fixed

#### Timezone Handling
- Fixed `SET timezone='UTC'` not taking effect over Flight SQL — `_Settings.__setitem__` now calls `.fetchall()` on the cursor, which is required for DML/DDL side effects over Arrow Flight SQL
- All three timezone tests now pass: `test_arrow_timestamp_with_time_zone`, `test_timestamp_with_timezone_literal[name]`, `test_timestamp_with_timezone_literal[iso]`

#### Connection Compatibility
- Wrapped `python_enable_replacements` setting in try/except — this is a DuckDB Python-specific setting not available on GizmoSQL Flight SQL servers, which previously caused `from_connection` errors

### Changed

- Updated dependencies: `ibis-framework==12.0.*`, `pyarrow==23.0.*`, `numpy==2.4.*`, `pandas==3.0.*`, `rich==14.3.*`
- Added `CLAUDE.md` with project documentation, architecture notes, and release checklist

## [0.0.14] - 2025-12-15

### Changed

- Previous release supporting ibis-framework 11.x
