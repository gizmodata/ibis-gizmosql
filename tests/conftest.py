from __future__ import annotations

import contextlib
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

import docker
import pytest
from filelock import FileLock

import ibis
from ibis import util
from ibis.backends.tests.base import BackendTest

if TYPE_CHECKING:
    from ibis.backends import BaseBackend

# ── Constants ──────────────────────────────────────────────────────────────────
GIZMOSQL_PORT = 31337
GIZMOSQL_IMAGE = "gizmodata/gizmosql:latest"
GIZMOSQL_USERNAME = "ibis"
GIZMOSQL_PASSWORD = "ibis_password"
CONTAINER_NAME = "ibis-gizmosql-test"

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "ci" / "ibis-testing-data"
SCHEMA_DIR = PROJECT_ROOT / "ci" / "schema"

# Standard test tables loaded from parquet
PARQUET_TABLES = (
    "functional_alltypes",
    "diamonds",
    "batting",
    "awards_players",
    "astronauts",
)


# ── TestConf ───────────────────────────────────────────────────────────────────
class TestConf(BackendTest):
    # Capability flags (matching DuckDB since GizmoSQL IS DuckDB underneath)
    supports_map = True
    supports_arrays = True
    supports_structs = True
    supports_json = True
    supports_tpch = True
    native_bool = True
    returned_timestamp_unit = "us"
    stateful = True
    driver_supports_multiple_statements = False
    rounding_method = "away_from_zero"

    deps = ("adbc_driver_gizmosql",)

    @classmethod
    def name(cls) -> str:
        return "gizmosql"

    @staticmethod
    def connect(*, tmpdir, worker_id, **kw) -> BaseBackend:
        return ibis.gizmosql.connect(
            host="localhost",
            user=GIZMOSQL_USERNAME,
            password=GIZMOSQL_PASSWORD,
            port=GIZMOSQL_PORT,
            use_encryption=True,
            disable_certificate_verification=True,
        )

    def load_tpch(self) -> None:
        """Load TPC-H data using DuckDB's built-in dbgen."""
        con = self.connection
        with con._safe_raw_sql("CALL dbgen(sf=0.17)") as cur:
            cur.fetchall()

    def _load_data(self, **_: Any) -> None:
        """Load test data into the GizmoSQL server."""
        import pyarrow.parquet as pq

        # 1) Load standard test tables from parquet using ADBC ingest
        # We batch large tables to stay under gRPC's 16MB message size limit.
        parquet_dir = self.data_dir / "parquet"
        for table_name in PARQUET_TABLES:
            parquet_path = parquet_dir / f"{table_name}.parquet"
            table = pq.read_table(parquet_path)
            self._batched_ingest(table_name, table)

        # 2) Load special tables using ADBC ingest
        self._load_special_tables()

    def _batched_ingest(
        self, table_name: str, table, *, batch_rows: int = 5000
    ) -> None:
        """Ingest a PyArrow table via ADBC in batches to avoid gRPC size limits."""
        total_rows = table.num_rows
        if total_rows <= batch_rows:
            with self.connection.con.cursor() as cur:
                cur.adbc_ingest(table_name, table, mode="replace")
            return

        for start in range(0, total_rows, batch_rows):
            chunk = table.slice(start, batch_rows)
            mode = "replace" if start == 0 else "append"
            with self.connection.con.cursor() as cur:
                cur.adbc_ingest(table_name, chunk, mode=mode)

    def _load_special_tables(self) -> None:
        """Load special test tables (array_types, struct, json_t, etc.).

        These tables have complex types (arrays, structs, maps, JSON) that
        use ADBC bulk ingest with properly typed PyArrow tables.
        """
        import pyarrow as pa

        # ── array_types ───────────────────────────────────────────────
        array_types_table = pa.table({
            "x": pa.array(
                [[1, 2, 3], [4, 5], [6, None], [None, 1, None], [2, None, 3], [4, None, None, 5]],
                type=pa.list_(pa.int64()),
            ),
            "y": pa.array(
                [["a", "b", "c"], ["d", "e"], ["f", None], [None, "a", None], ["b", None, "c"], ["d", None, None, "e"]],
                type=pa.list_(pa.string()),
            ),
            "z": pa.array(
                [[1.0, 2.0, 3.0], [4.0, 5.0], [6.0, None], [], None, [4.0, None, None, 5.0]],
                type=pa.list_(pa.float64()),
            ),
            "grouper": pa.array(["a", "a", "a", "b", "b", "c"], type=pa.string()),
            "scalar_column": pa.array([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], type=pa.float64()),
            "multi_dim": pa.array(
                [[[], [1, 2, 3], None], [], [None, [], None], [[1], [2], [], [3, 4, 5]], None, [[1, 2, 3]]],
                type=pa.list_(pa.list_(pa.int64())),
            ),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("array_types", array_types_table, mode="replace")

        # ── struct ────────────────────────────────────────────────────
        struct_table = pa.table({
            "abc": pa.array([
                {"a": 1.0, "b": "banana", "c": 2},
                {"a": 2.0, "b": "apple", "c": 3},
                {"a": 3.0, "b": "orange", "c": 4},
                {"a": None, "b": "banana", "c": 2},
                {"a": 2.0, "b": None, "c": 3},
                None,
                {"a": 3.0, "b": "orange", "c": None},
            ], type=pa.struct([
                ("a", pa.float64()),
                ("b", pa.string()),
                ("c", pa.int64()),
            ]))
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("struct", struct_table, mode="replace")

        # ── map ───────────────────────────────────────────────────────
        map_table = pa.table({
            "idx": pa.array([1, 2], type=pa.int64()),
            "kv": pa.array(
                [
                    [("a", 1), ("b", 2), ("c", 3)],
                    [("d", 4), ("e", 5), ("f", 6)],
                ],
                type=pa.map_(pa.string(), pa.int64()),
            ),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("map", map_table, mode="replace")

        # ── json_t ────────────────────────────────────────────────────
        json_rows = [
            '{"a": [1,2,3,4], "b": 1}',
            '{"a":null,"b":2}',
            '{"a":"foo", "c":null}',
            "null",
            "[42,47,55]",
            "[]",
            '"a"',
            '""',
            '"b"',
            None,
            "true",
            "false",
            "42",
            "37.37",
        ]
        json_table = pa.table({
            "rowid": pa.array(range(1, len(json_rows) + 1), type=pa.int64()),
            "js": pa.array(json_rows, type=pa.string()),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("json_t", json_table, mode="replace")

        # ── win (simple types) ────────────────────────────────────────
        win_table = pa.table({
            "g": pa.array(["a", "a", "a", "a", "a"], type=pa.string()),
            "x": pa.array([0, 1, 2, 3, 4], type=pa.int64()),
            "y": pa.array([3, 2, 0, 1, 1], type=pa.int64()),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("win", win_table, mode="replace")

        # ── topk (simple types) ──────────────────────────────────────
        topk_table = pa.table({
            "x": pa.array([1, 1, None], type=pa.int64()),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("topk", topk_table, mode="replace")

        # ── shops.ice_cream ───────────────────────────────────────────
        with self.connection.con.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS shops")
            cur.fetchall()
        ice_cream_table = pa.table({
            "flavor": pa.array(["vanilla", "chocolate"], type=pa.string()),
            "quantity": pa.array([2, 3], type=pa.int32()),
        })
        with self.connection.con.cursor() as cur:
            cur.adbc_ingest("shops.ice_cream", ice_cream_table, mode="replace")


# ── Docker container management ────────────────────────────────────────────────
def wait_for_container_log(
    container, timeout=60, poll_interval=1,
    ready_message="GizmoSQL server - started",
):
    start_time = time.time()
    while time.time() - start_time < timeout:
        logs = container.logs().decode("utf-8")
        if ready_message in logs:
            return True
        time.sleep(poll_interval)
    raise TimeoutError(
        f"Container did not show '{ready_message}' within {timeout}s."
    )


def _port_is_listening(port: int, host: str = "localhost") -> bool:
    """Check whether something is already listening on the given port."""
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(1)
        return s.connect_ex((host, port)) == 0


@pytest.fixture(scope="session")
def gizmosql_server():
    """Start the GizmoSQL Docker container with test data mounted.

    If a server (local build or another container) is already listening on
    GIZMOSQL_PORT, skip Docker management and use that server instead.
    """
    # If something is already listening (e.g. a local build), just use it.
    if _port_is_listening(GIZMOSQL_PORT):
        yield None
        return

    client = docker.from_env()
    parquet_dir = str(DATA_DIR / "parquet")

    # Reuse an existing container if it's already running
    try:
        container = client.containers.get(CONTAINER_NAME)
        if container.status == "running":
            yield container
            return
        # Container exists but is not running – remove and recreate
        container.remove(force=True)
    except docker.errors.NotFound:
        pass

    container = client.containers.run(
        image=GIZMOSQL_IMAGE,
        name=CONTAINER_NAME,
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{GIZMOSQL_PORT}/tcp": GIZMOSQL_PORT},
        volumes={parquet_dir: {"bind": "/data/parquet", "mode": "ro"}},
        environment={
            "GIZMOSQL_USERNAME": GIZMOSQL_USERNAME,
            "GIZMOSQL_PASSWORD": GIZMOSQL_PASSWORD,
            "TLS_ENABLED": "1",
            "PRINT_QUERIES": "0",
            "DATABASE_FILENAME": ":memory:",
        },
        stdout=True,
        stderr=True,
    )

    wait_for_container_log(container)
    yield container
    container.stop()


# ── Core fixtures ──────────────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def data_dir() -> Path:
    """Override ibis's data_dir to point to our local test data."""
    return DATA_DIR


@pytest.fixture(scope="session")
def backend(gizmosql_server, data_dir, tmp_path_factory, worker_id):
    """Return TestConf instance with data loaded."""
    with TestConf.load_data(data_dir, tmp_path_factory, worker_id) as inst:
        yield inst


@pytest.fixture(scope="session")
def con(backend):
    """GizmoSQL connection instance."""
    return backend.connection


# ── Table fixtures expected by ibis shared tests ───────────────────────────────
@pytest.fixture(scope="session")
def alltypes(backend):
    return backend.functional_alltypes


@pytest.fixture(scope="session")
def df(alltypes):
    return alltypes.execute()


@pytest.fixture(scope="session")
def sorted_df(df):
    return df.sort_values("id").reset_index(drop=True)


@pytest.fixture(scope="session")
def batting(backend):
    return backend.batting


@pytest.fixture(scope="session")
def awards_players(backend):
    return backend.awards_players


@pytest.fixture(scope="session")
def batting_df(batting):
    return batting.execute(limit=None)


@pytest.fixture(scope="session")
def awards_players_df(awards_players):
    return awards_players.execute(limit=None)


@pytest.fixture(scope="session")
def json_t(backend):
    return backend.json_t


@pytest.fixture(scope="session")
def struct(backend):
    return backend.struct


@pytest.fixture(scope="session")
def struct_df(struct):
    return struct.execute()


# ── Temp table/view fixtures ──────────────────────────────────────────────────
@pytest.fixture
def temp_table(con):
    name = util.gen_name("temp_table")
    yield name
    with contextlib.suppress(NotImplementedError):
        con.drop_table(name, force=True)


@pytest.fixture
def temp_table2(con):
    name = util.gen_name("temp_table2")
    yield name
    with contextlib.suppress(NotImplementedError):
        con.drop_table(name, force=True)


@pytest.fixture
def temp_table_orig(con, temp_table):
    name = f"{temp_table}_orig"
    yield name
    with contextlib.suppress(NotImplementedError):
        con.drop_table(name, force=True)


@pytest.fixture
def temp_view(con):
    name = util.gen_name("view")
    yield name
    with contextlib.suppress(NotImplementedError):
        con.drop_view(name, force=True)


@pytest.fixture
def assert_sql(con, snapshot):
    def checker(expr, file_name="out.sql"):
        sql = con.compile(expr, pretty=True)
        snapshot.assert_match(sql, file_name)
    return checker


# ── Fixtures expected by ibis shared tests ────────────────────────────────────
# We override ddl_backend to reuse the main backend instance.
# The ibis pytest_runtest_call hook raises ValueError if both "backend" and
# "ddl_backend" appear in funcargs.  Since tests like test_create_drop_view
# pull in "backend" indirectly (via temp_view → con → backend), we need
# ddl_backend to return the SAME object so we only get one unique name.
@pytest.fixture(scope="session")
def ddl_backend(backend):
    return backend


@pytest.fixture(scope="session")
def ddl_con(ddl_backend):
    return ddl_backend.connection


@pytest.fixture(scope="session")
def con_no_data(con):
    """Connection without test data (reuse main connection for GizmoSQL)."""
    return con


@pytest.fixture(scope="session")
def backend_name():
    """Backend name string for tests that use it directly."""
    return "gizmosql"


@pytest.fixture(scope="session")
def con_create_database(con):
    from ibis.backends import CanCreateDatabase

    if isinstance(con, CanCreateDatabase):
        return con
    else:
        pytest.skip(f"{con.name} backend cannot create schemas")


@pytest.fixture(scope="session")
def con_create_catalog(con):
    from ibis.backends import CanCreateCatalog

    if isinstance(con, CanCreateCatalog):
        return con
    else:
        pytest.skip(f"{con.name} backend cannot create databases")


@pytest.fixture(scope="session")
def con_create_catalog_database(con):
    from ibis.backends import CanCreateCatalog, CanCreateDatabase

    if isinstance(con, CanCreateCatalog) and isinstance(con, CanCreateDatabase):
        return con
    else:
        pytest.skip(f"{con.name} backend cannot create both database and schemas")


# ── Register gizmosql as a DuckDB dialect alias ──────────────────────────────
# GizmoSQL uses the DuckDB compiler, so ibis.to_sql(dialect="gizmosql") should
# resolve to the DuckDB compiler module.
try:
    import ibis.backends.sql.compilers as _compilers

    if not hasattr(_compilers, "gizmosql"):
        _compilers.gizmosql = _compilers.duckdb
except (ImportError, AttributeError):
    pass

# Also register gizmosql in sqlglot's dialect registry so that
# sqlglot.Dialect.get_or_raise("gizmosql") works (used by dot_sql tests).
try:
    from sqlglot.dialects.dialect import Dialect as _SglotDialect
    from sqlglot.dialects.duckdb import DuckDB as _SglotDuckDB

    if "gizmosql" not in _SglotDialect._classes:
        _SglotDialect._classes["gizmosql"] = _SglotDuckDB
except (ImportError, AttributeError):
    pass


# ── Marker processing hook ─────────────────────────────────────────────────────
# Import the ibis marker processing hook so notimpl/notyet/never markers
# are properly converted to xfail at runtime.  We wrap it to also propagate
# DuckDB markers to gizmosql, since GizmoSQL IS DuckDB underneath.
try:
    from ibis.backends.conftest import (
        pytest_runtest_call as _ibis_pytest_runtest_call,
    )

    def _has_duckdb_specific_raises(marker):
        """Check if a marker uses DuckDB-specific exception types.

        DuckDB-native exceptions (from _duckdb module) won't occur
        over Flight SQL, so we shouldn't propagate those markers.
        """
        raises = marker.kwargs.get("raises")
        if raises is None:
            return False
        if not isinstance(raises, tuple):
            raises = (raises,)
        return any(
            getattr(cls, "__module__", "").startswith("duckdb")
            or getattr(cls, "__module__", "").startswith("_duckdb")
            for cls in raises
            if cls is not None
        )

    def pytest_runtest_call(item):
        """Propagate DuckDB notimpl/notyet/never markers to gizmosql.

        Skip markers with DuckDB-specific exception types since those
        won't occur over Flight SQL/ADBC.
        """
        for mark_name in ("notimpl", "notyet", "never"):
            for marker in item.iter_markers(name=mark_name):
                if (
                    "duckdb" in marker.args[0]
                    and "gizmosql" not in marker.args[0]
                    and not _has_duckdb_specific_raises(marker)
                ):
                    marker.args[0].append("gizmosql")

        # Deduplicate backend funcargs: ibis's hook errors when both
        # "backend" and "ddl_backend" are in funcargs (even if they
        # resolve to the same TestConf instance).  Keep only one.
        _backend_keys = [
            k for k in item.funcargs
            if k.endswith(("backend", "backend_name", "backend_no_data"))
        ]
        if len(_backend_keys) > 1:
            # Keep the first, stash and remove the rest
            _stashed = {}
            for k in _backend_keys[1:]:
                _stashed[k] = item.funcargs.pop(k)
            try:
                _ibis_pytest_runtest_call(item)
            finally:
                item.funcargs.update(_stashed)
        else:
            _ibis_pytest_runtest_call(item)

except ImportError:
    pass


# ── Monkey-patch ibis test dictionaries for gizmosql ──────────────────────────
# GizmoSQL is DuckDB underneath, so we mirror DuckDB's entries wherever the
# ibis shared tests use backend-name lookup dicts.
def _patch_dict(d: dict, key: str = "gizmosql", source: str = "duckdb") -> None:
    """Add a gizmosql entry mirroring duckdb if not already present."""
    if key not in d and source in d:
        d[key] = d[source]


# Module-level dicts in test files
try:
    from ibis.backends.tests import test_generic

    _patch_dict(test_generic.NULL_BACKEND_TYPES)
    _patch_dict(test_generic.BOOLEAN_BACKEND_TYPE)
except (ImportError, AttributeError):
    pass

try:
    from ibis.backends.tests import test_binary

    _patch_dict(test_binary.BINARY_BACKEND_TYPES)
except (ImportError, AttributeError):
    pass

try:
    from ibis.backends.tests import test_client

    # Patch catalog/database lookup dicts
    for attr in ("test_catalogs", "test_databases"):
        d = getattr(test_client, attr, None)
        if d is not None:
            _patch_dict(d)
except (ImportError, AttributeError):
    pass

try:
    from ibis.backends.tests import test_temporal

    for attr in ("DATE_BACKEND_TYPES", "TIMESTAMP_BACKEND_TYPES", "TIME_BACKEND_TYPES"):
        d = getattr(test_temporal, attr, None)
        if d is not None:
            _patch_dict(d)
except (ImportError, AttributeError):
    pass

try:
    from ibis.backends.tests import test_uuid

    _patch_dict(getattr(test_uuid, "UUID_BACKEND_TYPE", {}))
except (ImportError, AttributeError):
    pass


# Other backends whose tests we should not run -- we only test gizmosql.
_OTHER_BACKENDS = frozenset({
    "athena", "bigquery", "clickhouse", "databricks", "datafusion", "druid",
    "exasol", "flink", "impala", "materialize", "mssql", "mysql", "oracle",
    "polars", "postgres", "postgresql", "pyspark", "risingwave",
    "singlestoredb", "snowflake", "sqlite", "trino",
})


def pytest_collection_modifyitems(config, items):
    """Patch inline parametrized dicts and skip tests for other backends.

    Many ibis shared tests use inline dicts like ``{..., "duckdb": val, ...}``
    inside ``@pytest.mark.parametrize``. Since gizmosql is DuckDB under the hood,
    we add a ``"gizmosql"`` key mirroring ``"duckdb"`` wherever it's missing.

    We also deselect tests that are explicitly parametrized for other backends
    (e.g. ``test_connect_url[postgres]``), since we only test gizmosql.
    """
    selected = []
    deselected = []

    for item in items:
        # Patch inline dicts
        if hasattr(item, "callspec"):
            for key, val in item.callspec.params.items():
                if isinstance(val, dict) and "duckdb" in val and "gizmosql" not in val:
                    val["gizmosql"] = val["duckdb"]

        # Deselect test_has_operation_no_builtins[gizmosql-*]: it tries
        # importlib.import_module("ibis.backends.gizmosql") which doesn't
        # exist for an out-of-tree backend.
        if (
            item.name.startswith("test_has_operation_no_builtins[gizmosql")
        ):
            deselected.append(item)
            continue

        # Deselect tests parametrized for other backends
        # e.g. test_foo[postgres], test_foo[clickhouse-subquery]
        if hasattr(item, "callspec"):
            params = item.callspec.params
            for val in params.values():
                if isinstance(val, str) and val in _OTHER_BACKENDS:
                    deselected.append(item)
                    break
            else:
                # Also check the test ID for backend names in brackets
                test_id = item.nodeid
                if "[" in test_id:
                    bracket = test_id[test_id.rindex("[") + 1 : test_id.rindex("]")]
                    parts = bracket.split("-")
                    if parts[0] in _OTHER_BACKENDS:
                        deselected.append(item)
                        continue
                selected.append(item)
        else:
            selected.append(item)

    if deselected:
        config.hook.pytest_deselected(items=deselected)
        items[:] = selected

    # ── GizmoSQL-specific xfail markers ──────────────────────────────────
    # Tests that can't pass with GizmoSQL due to known limitations.
    _xfails = {
        # Python UDFs can't run over Flight SQL (requires in-process)
        "test_impure_correlated[udf]": "Python UDFs not supported over FlightSQL",
        "test_chained_selections[udf]": "Python UDFs not supported over FlightSQL",
        "test_impure_uncorrelated_different_id[udf]": "Python UDFs not supported over FlightSQL",
        "test_impure_uncorrelated_same_id[udf]": "Python UDFs not supported over FlightSQL",
        # DuckDB max decimal precision is 38
        "test_decimal_literal[decimal-big]": "DuckDB max DECIMAL precision is 38",
        # PySpark Java-style date format not supported by DuckDB strptime
        "test_string_as_timestamp[pyspark_format]": "PySpark format strings not supported",
        "test_string_as_date[pyspark_format]": "PySpark format strings not supported",
        # Export: to_json not implemented
        "test_to_json": "to_json not implemented for GizmoSQL",
        # Export: partitioned parquet requires specific PyArrow API
        "test_roundtrip_partitioned_parquet": "Partitioned parquet not supported over FlightSQL",
        # Export: FlightSQL doesn't preserve null types (returns int32)
        "test_all_null_table": "FlightSQL returns typed nulls, not pa.null()",
        "test_all_null_column": "FlightSQL returns typed nulls, not pa.null()",
        "test_all_null_scalar": "FlightSQL returns typed nulls, not pa.null()",
        # Export: FlightSQL doesn't preserve NOT NULL constraints
        "test_cast_non_null": "FlightSQL schema doesn't preserve NOT NULL",
        # PyArrow Dataset: materialization guard
        "test_create_table_in_memory[pyarrow dataset]": "PyArrow Dataset materialization not supported",
    }
    for item in items:
        reason = _xfails.get(item.name)
        if reason is not None:
            item.add_marker(pytest.mark.xfail(reason=reason, strict=False))
