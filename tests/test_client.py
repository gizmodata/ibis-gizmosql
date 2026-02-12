from ibis.backends.tests.test_client import *  # noqa: F401,F403


def test_list_catalogs(con):
    """Override: add gizmosql entry (same as duckdb)."""
    result = set(con.list_catalogs())
    assert {"memory"} <= result


def test_list_database_contents(con):
    """Override: add gizmosql entry (same as duckdb)."""
    result = set(con.list_databases())
    assert {"pg_catalog", "main", "information_schema"} <= result
