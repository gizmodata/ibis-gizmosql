# ibis-gizmosql

An [Ibis](https://ibis-project.org/) backend for [GizmoSQL](https://github.com/gizmodata/gizmosql) — DuckDB over Arrow Flight SQL.

## Architecture

- **Backend**: `ibis_gizmosql/__init__.py` — the main `Backend` class, inherits from ibis's SQL backend base classes
- **Compiler**: Uses `DuckDBCompiler` from ibis with a `gizmosql` dialect name
- **Connection**: ADBC Flight SQL driver (`adbc_driver_gizmosql`) over gRPC+TLS to a GizmoSQL server
- **Tests**: `tests/conftest.py` — test configuration with `TestConf` (the ibis `BackendTest` subclass)

## Key Concepts

- **Flight SQL**: All SQL is executed over Arrow Flight SQL. DML/DDL statements **must** have `.fetchall()` called on the cursor for side effects to take place (unlike local DuckDB).
- **ADBC Bulk Ingest**: Used to upload in-memory PyArrow tables to the server (`cur.adbc_ingest()`). This is how `_register_in_memory_table` works.
- **`_Settings.__setitem__`**: Must call `.fetchall()` — otherwise SET commands have no effect over Flight SQL.
- **`python_enable_replacements`**: A DuckDB Python-specific setting not available on GizmoSQL; wrapped in try/except.
- **Timezone handling**: The ibis DuckDB backend sets `SET timezone='UTC'` on connect. This is critical for correct TIMESTAMPTZ behavior.

## Testing

Tests use the ibis v12 upstream test suite. The GizmoSQL server must be running before tests.

```bash
# Run full test suite (parallel)
pytest -x -n auto --dist loadgroup

# Run a specific test file
pytest tests/test_temporal.py -x

# Run with xfail tests to see actual failures
pytest --runxfail -k "test_name"

# Update snapshots
pytest --snapshot-update -k "test_name"
```

The test fixture connects to a GizmoSQL server at `grpc+tls://localhost:31337` (configurable via `GIZMOSQL_URI` env var). The server can be started via Docker or a local compiled binary.

## Development

```bash
# Install in dev mode
pip install -e ".[dev,test]"
```

## Release Checklist

1. Ensure all tests pass: `pytest -x -n auto --dist loadgroup`
2. Update version: `bumpver update --minor` (or `--major` / `--patch`)
   - This updates `pyproject.toml` and `ibis_gizmosql/__init__.py` automatically
   - Creates a git commit and tag
3. Push with tags: `git push && git push --tags`
4. Build and publish: `python -m build && twine upload dist/*`

## File Layout

```
ibis_gizmosql/__init__.py   — Backend implementation (Backend class, compiler, settings)
tests/conftest.py           — Test configuration, xfails, data loaders
tests/snapshots/            — SQL snapshot files for snapshot tests
pyproject.toml              — Package metadata, dependencies, bumpver config
```
