"""SQLFlite backend."""
from __future__ import annotations

import ast
import contextlib
import os
import urllib
import warnings
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_plus
import re

import duckdb
from adbc_driver_flightsql import dbapi as sqlflite, DatabaseOptions
import pyarrow as pa
import pyarrow_hotfix  # noqa: F401
import sqlglot as sg
import sqlglot.expressions as sge

import ibis
import ibis.common.exceptions as exc
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
from ibis import util
from ibis.backends import CanCreateDatabase, CanCreateSchema, UrlFromPath
from ibis_gizmosql.converter import DuckDBPandasData
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers import DuckDBCompiler
from ibis.backends.sql.compilers.base import STAR, C
from ibis.common.dispatch import lazy_singledispatch
from ibis.expr.operations.udf import InputType
from ibis.util import deprecated

__version__ = "0.0.8"

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, MutableMapping, Sequence
    from urllib.parse import ParseResult

    import pandas as pd
    import polars as pl
    import torch
    from fsspec import AbstractFileSystem

_UDF_INPUT_TYPE_MAPPING = {
    InputType.PYARROW: duckdb.functional.ARROW,
    InputType.PYTHON: duckdb.functional.NATIVE,
}


class _Settings:
    def __init__(self, con: sqlflite.Connection) -> None:
        self.con = con

    def __getitem__(self, key: str) -> Any:
        with self.con.cursor() as cur:
            cur.execute(f"SELECT value FROM duckdb_settings() WHERE name = {key!r}")
            maybe_value = cur.fetchone()
        if maybe_value is not None:
            return maybe_value[0]
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        with self.con.cursor() as cur:
            cur.execute(f"SET {key} = {str(value)!r}")

    def __repr__(self):
        with self.con.cursor() as cur:
            return repr(cur.execute("SELECT * FROM duckdb_settings()").fetchall())


class Backend(SQLBackend, CanCreateDatabase, CanCreateSchema, UrlFromPath):
    name = "sqlflite"
    compiler = DuckDBCompiler()
    dialect = "duckdb"

    def _from_url(self, url: ParseResult, **kwargs):
        """Connect to a backend using a URL `url`.

        Parameters
        ----------
        url
            URL with which to connect to a backend.
        kwargs
            Additional keyword arguments

        Returns
        -------
        BaseBackend
            A backend instance

        """
        database, *schema = url.path[1:].split("/", 1)
        connect_args = {
            "user": url.username,
            "password": unquote_plus(url.password or ""),
            "host": url.hostname,
            "database": database or "",
            "schema": schema[0] if schema else "",
            "port": url.port,
        }

        kwargs.update(connect_args)
        self._convert_kwargs(kwargs)

        if "user" in kwargs and not kwargs["user"]:
            del kwargs["user"]

        if "host" in kwargs and not kwargs["host"]:
            del kwargs["host"]

        if "database" in kwargs and not kwargs["database"]:
            del kwargs["database"]

        if "schema" in kwargs and not kwargs["schema"]:
            del kwargs["schema"]

        if "password" in kwargs and kwargs["password"] is None:
            del kwargs["password"]

        if "port" in kwargs and kwargs["port"] is None:
            del kwargs["port"]

        if "useEncryption" in kwargs:
            kwargs["use_encryption"] = kwargs.pop("useEncryption", "false").lower() == "true"

        if "disableCertificateVerification" in kwargs:
            kwargs["disable_certificate_verification"] = kwargs.pop("disableCertificateVerification",
                                                                    "false").lower() == "true"

        return self.connect(**kwargs)

    @property
    def settings(self) -> _Settings:
        return _Settings(self.con)

    @property
    def current_catalog(self) -> str:
        with self._safe_raw_sql(sg.select(self.compiler.f.current_database())) as cur:
            [(db,)] = cur.fetchall()
        return db

    @property
    def current_database(self) -> str:
        with self._safe_raw_sql(sg.select(self.compiler.f.current_schema())) as cur:
            [(db,)] = cur.fetchall()
        return db

    def raw_sql(self, query: str | sg.Expression, **kwargs: Any) -> Any:
        with contextlib.suppress(AttributeError):
            query = query.sql(dialect=self.dialect)

        cur = self.con.cursor()
        cur.execute(query, **kwargs)

        return cur

    def _to_sqlglot(
            self, expr: ir.Expr, limit: str | None = None, params=None, **_: Any
    ):
        sql = super()._to_sqlglot(expr, limit=limit, params=params)

        return sql

    def create_table(
            self,
            name: str,
            obj: ir.Table
                 | pd.DataFrame
                 | pa.Table
                 | pl.DataFrame
                 | pl.LazyFrame
                 | None = None,
            *,
            schema: ibis.Schema | None = None,
            database: str | None = None,
            temp: bool = False,
            overwrite: bool = False,
    ):
        """Create a table in SQLFlite.

        Parameters
        ----------
        name
            Name of the table to create
        obj
            The data with which to populate the table; optional, but at least
            one of `obj` or `schema` must be specified
        schema
            The schema of the table to create; optional, but at least one of
            `obj` or `schema` must be specified
        database
            The name of the database in which to create the table; if not
            passed, the current database is used.

            For multi-level table hierarchies, you can pass in a dotted string
            path like `"catalog.database"` or a tuple of strings like
            `("catalog", "database")`.
        temp
            Create a temporary table
        overwrite
            If `True`, replace the table if it already exists, otherwise fail
            if the table exists

        """
        table_loc = self._to_sqlglot_table(database)

        if getattr(table_loc, "catalog", False) and temp:
            raise exc.UnsupportedArgumentError(
                "DuckDB can only create temporary tables in the `temp` catalog. "
                "Don't specify a catalog to enable temp table creation."
            )

        catalog = self.current_catalog
        database = self.current_database
        if table_loc is not None:
            catalog = table_loc.catalog or catalog
            database = table_loc.db or database

        if obj is None and schema is None:
            raise ValueError("Either `obj` or `schema` must be specified")

        properties = []

        if temp:
            properties.append(sge.TemporaryProperty())
            catalog = "temp"

        temp_memtable_view = None

        if obj is not None:
            if not isinstance(obj, ir.Expr):
                table = ibis.memtable(obj)
                temp_memtable_view = table.op().name
            else:
                table = obj

            self._run_pre_execute_hooks(table)

            query = self._to_sqlglot(table)
        else:
            query = None

        column_defs = [
            sge.ColumnDef(
                this=sg.to_identifier(colname, quoted=self.compiler.quoted),
                kind=self.compiler.type_mapper.from_ibis(typ),
                constraints=(
                    None
                    if typ.nullable
                    else [sge.ColumnConstraint(kind=sge.NotNullColumnConstraint())]
                ),
            )
            for colname, typ in (schema or table.schema()).items()
        ]

        if overwrite:
            temp_name = util.gen_name("sqlflite_table")
        else:
            temp_name = name

        initial_table = sge.Table(
            this=sg.to_identifier(temp_name, quoted=self.compiler.quoted),
            catalog=sg.to_identifier(catalog, quoted=self.compiler.quoted),
            db=sg.to_identifier(database, quoted=self.compiler.quoted),
        )
        target = sge.Schema(this=initial_table, expressions=column_defs)

        create_stmt = sge.Create(
            kind="TABLE",
            this=target,
            properties=sge.Properties(expressions=properties),
        )

        # This is the same table as initial_table unless overwrite == True
        final_table = sge.Table(
            this=sg.to_identifier(name, quoted=self.compiler.quoted),
            catalog=sg.to_identifier(catalog, quoted=self.compiler.quoted),
            db=sg.to_identifier(database, quoted=self.compiler.quoted),
        )
        with self._safe_raw_sql(create_stmt) as create_table_cur:
            with self.con.cursor() as cur:
                if query is not None:
                    insert_stmt = sge.insert(query, into=initial_table).sql(dialect=self.dialect)
                    cur.execute(insert_stmt)

                if overwrite:
                    cur.execute(
                        sge.Drop(kind="TABLE", this=final_table, exists=True).sql(dialect=self.dialect)
                    )
                    # TODO: This branching should be removed once DuckDB >=0.9.3 is
                    # our lower bound (there's an upstream bug in 0.9.2 that
                    # disallows renaming temp tables)
                    # We should (pending that release) be able to remove the if temp
                    # branch entirely.
                    if temp:
                        cur.execute(
                            sge.Create(
                                kind="TABLE",
                                this=final_table,
                                expression=sg.select(STAR).from_(initial_table),
                                properties=sge.Properties(expressions=properties),
                            ).sql(dialect=self.dialect)
                        )
                        cur.execute(
                            sge.Drop(kind="TABLE", this=initial_table, exists=True).sql(
                                self.name
                            )
                        )
                    else:
                        cur.execute(
                            sge.AlterTable(
                                this=initial_table,
                                actions=[sge.RenameTable(this=final_table)],
                            ).sql(dialect=self.dialect)
                        )

        if temp_memtable_view is not None:
            self.con.unregister(temp_memtable_view)

        return self.table(name, database=(catalog, database))

    def table(
            self, name: str, schema: str | None = None, database: str | None = None
    ) -> ir.Table:
        """Construct a table expression.

        Parameters
        ----------
        name
            Table name
        schema
            [deprecated] Schema name
        database
            Database name

        Returns
        -------
        Table
            Table expression

        """
        table_loc = self._warn_and_create_table_loc(database, schema)

        catalog, database = None, None
        if table_loc is not None:
            catalog = table_loc.catalog or None
            database = table_loc.db or None

        table_schema = self.get_schema(name, catalog=catalog, database=database)

        return ops.DatabaseTable(
            name,
            schema=table_schema,
            source=self,
            namespace=ops.Namespace(catalog=catalog, database=database),
        ).to_expr()

    def get_schema(
            self,
            table_name: str,
            *,
            catalog: str | None = None,
            database: str | None = None,
    ) -> sch.Schema:
        """Compute the schema of a `table`.

        Parameters
        ----------
        table_name
            May **not** be fully qualified. Use `database` if you want to
            qualify the identifier.
        catalog
            Catalog name
        database
            Database name

        Returns
        -------
        sch.Schema
            Ibis schema

        """
        conditions = [sg.column("table_name").eq(sge.convert(table_name))]

        if catalog is not None:
            conditions.append(sg.column("table_catalog").eq(sge.convert(catalog)))

        if database is not None:
            conditions.append(sg.column("table_schema").eq(sge.convert(database)))

        query = (
            sg.select(
                "column_name",
                "data_type",
                sg.column("is_nullable").eq(sge.convert("YES")).as_("nullable"),
            )
            .from_(sg.table("columns", db="information_schema"))
            .where(sg.and_(*conditions))
            .order_by("ordinal_position")
        )

        with self._safe_raw_sql(query) as cur:
            meta = cur.fetch_arrow_table()

        if not meta:
            raise exc.IbisError(f"Table not found: {table_name!r}")

        names = meta["column_name"].to_pylist()
        types = meta["data_type"].to_pylist()
        nullables = meta["nullable"].to_pylist()

        return sch.Schema(
            {
                name: self.compiler.type_mapper.from_string(typ, nullable=nullable)
                for name, typ, nullable in zip(names, types, nullables)
            }
        )

    @contextlib.contextmanager
    def _safe_raw_sql(self, *args, **kwargs):
        cur = self.raw_sql(*args, **kwargs)
        try:
            yield cur
        finally:
            cur.close()

    def list_catalogs(self, like: str | None = None) -> list[str]:
        col = "catalog_name"
        query = sg.select(sge.Distinct(expressions=[sg.column(col)])).from_(
            sg.table("schemata", db="information_schema")
        )
        with self._safe_raw_sql(query) as cur:
            result = cur.fetch_arrow_table()
        dbs = result[col]
        return self._filter_with_like(dbs.to_pylist(), like)

    def list_databases(
            self, like: str | None = None, catalog: str | None = None
    ) -> list[str]:
        col = "schema_name"
        query = sg.select(sge.Distinct(expressions=[sg.column(col)])).from_(
            sg.table("schemata", db="information_schema")
        )

        if catalog is not None:
            query = query.where(sg.column("catalog_name").eq(sge.convert(catalog)))

        with self._safe_raw_sql(query) as cur:
            out = cur.fetch_arrow_table()
        return self._filter_with_like(out[col].to_pylist(), like=like)

    @property
    def version(self) -> str:
        with self._safe_raw_sql("SELECT version()") as cur:
            [(version,)] = cur.fetchall()

        return version

    def do_connect(
            self,
            host: str | None = None,
            user: str | None = None,
            password: str | None = None,
            port: int = 31337,
            database: str | None = None,
            schema: str | None = None,
            use_encryption: bool | None = None,
            disable_certificate_verification: bool | None = None,
            **kwargs: Any,
    ) -> None:
        """Create an Ibis client connected to SQLFlite database.

        Parameters
        ----------
        host
            Hostname
        user
            Username
        password
            Password
        port
            Port number
        database
            Database to connect to
        schema
            SQLFlite schema to use. If `None`, use the default `search_path`.
        use_encryption
            Use encryption via TLS
        disable_certificate_verification
            Disable certificate verification
        kwargs
            Additional keyword arguments to pass to the backend client connection.

        Examples
        --------
        >>> import os
        >>> import getpass
        >>> import ibis
        >>> host = os.environ.get("IBIS_TEST_SQLFLITE_HOST", "localhost")
        >>> user = os.environ.get("IBIS_TEST_SQLFLITE_USER", getpass.getuser())
        >>> password = os.environ.get("IBIS_TEST_SQLFLITE_PASSWORD")
        >>> database = os.environ.get("IBIS_TEST_SQLFLITE_DATABASE", "ibis_testing")
        >>> con = connect(database=database, host=host, user=user, password=password)
        >>> con.list_tables()  # doctest: +ELLIPSIS
        [...]
        >>> t = con.table("functional_alltypes")
        >>> t
        PostgreSQLTable[table]
          name: functional_alltypes
          schema:
            id : int32
            bool_col : boolean
            tinyint_col : int16
            smallint_col : int16
            int_col : int32
            bigint_col : int64
            float_col : float32
            double_col : float64
            date_string_col : string
            string_col : string
            timestamp_col : timestamp
            year : int32
            month : int32

        """
        connection_scheme = "grpc"
        if use_encryption:
            connection_scheme += "+tls"

        db_kwargs = dict(username=user, password=password)
        if use_encryption and disable_certificate_verification is not None:
            db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = str(
                disable_certificate_verification
            ).lower()

        self.con = sqlflite.connect(uri=f"{connection_scheme}://{host}:{port}",
                                    db_kwargs=db_kwargs
                                    )

        vendor_version = self.con.adbc_get_info().get("vendor_version")

        if not re.search(pattern="^duckdb ", string=vendor_version):
            raise exc.UnsupportedBackendType(f"Unsupported SQFLite server backend: '{vendor_version}'")

        # Default timezone, can't be set with `config`
        self.settings["timezone"] = "UTC"

        self._record_batch_readers_consumed = {}

    def create_database(
            self, name: str, catalog: str | None = None, force: bool = False
    ) -> None:
        if catalog is not None:
            raise exc.UnsupportedOperationError(
                "SQLFlite cannot create a database in another catalog."
            )

        name = sg.table(name, catalog=catalog, quoted=self.compiler.quoted)
        with self._safe_raw_sql(sge.Create(this=name, kind="SCHEMA", replace=force)):
            pass

    def drop_database(
            self, name: str, catalog: str | None = None, force: bool = False
    ) -> None:
        if catalog is not None:
            raise exc.UnsupportedOperationError(
                "SQLFlite cannot drop a database in another catalog."
            )

        name = sg.table(name, catalog=catalog, quoted=self.compiler.quoted)
        with self._safe_raw_sql(sge.Drop(this=name, kind="SCHEMA", replace=force)):
            pass

    @deprecated(
        as_of="9.1",
        instead="use the explicit `read_*` method for the filetype you are trying to read, e.g., read_parquet, read_csv, etc.",
    )
    def register(
            self,
            source: str | Path | Any,
            table_name: str | None = None,
            **kwargs: Any,
    ) -> ir.Table:
        """Register a data source as a table in the current database.

        Parameters
        ----------
        source
            The data source(s). May be a path to a file or directory of
            parquet/csv files, an iterable of parquet or CSV files, a pandas
            dataframe, a pyarrow table or dataset, or a postgres URI.
        table_name
            An optional name to use for the created table. This defaults to a
            sequentially generated name.
        **kwargs
            Additional keyword arguments passed to DuckDB loading functions for
            CSV or parquet.  See https://duckdb.org/docs/data/csv and
            https://duckdb.org/docs/data/parquet for more information.

        Returns
        -------
        ir.Table
            The just-registered table

        """

        if isinstance(source, (str, Path)):
            first = str(source)
        elif isinstance(source, (list, tuple)):
            first = source[0]
        else:
            try:
                return self.read_in_memory(source, table_name=table_name, **kwargs)
            except (duckdb.InvalidInputException, NameError):
                self._register_failure()

        if first.startswith(("parquet://", "parq://")) or first.endswith(
                ("parq", "parquet")
        ):
            return self.read_parquet(source, table_name=table_name, **kwargs)
        elif first.startswith(
                ("csv://", "csv.gz://", "txt://", "txt.gz://")
        ) or first.endswith(("csv", "csv.gz", "tsv", "tsv.gz", "txt", "txt.gz")):
            return self.read_csv(source, table_name=table_name, **kwargs)
        elif first.startswith(("postgres://", "postgresql://")):
            return self.read_postgres(source, table_name=table_name, **kwargs)
        elif first.startswith("sqlite://"):
            return self.read_sqlite(
                first[len("sqlite://"):], table_name=table_name, **kwargs
            )
        else:
            self._register_failure()  # noqa: RET503

    def _register_failure(self):
        import inspect

        msg = ", ".join(
            name for name, _ in inspect.getmembers(self) if name.startswith("read_")
        )
        raise ValueError(
            f"Cannot infer appropriate read function for input, "
            f"please call one of {msg} directly"
        )

    @util.experimental
    def read_json(
            self,
            source_list: str | list[str] | tuple[str],
            table_name: str | None = None,
            **kwargs,
    ) -> ir.Table:
        """Read newline-delimited JSON into an ibis table.

        ::: {.callout-note}
        ## This feature requires duckdb>=0.7.0
        :::

        Parameters
        ----------
        source_list
            File or list of files
        table_name
            Optional table name
        **kwargs
            Additional keyword arguments passed to DuckDB's `read_json_auto` function

        Returns
        -------
        Table
            An ibis table expression

        """
        if not table_name:
            table_name = util.gen_name("read_json")

        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(
                self.compiler.f.read_json_auto(
                    util.normalize_filenames(source_list), *options
                )
            ),
        )

        return self.table(table_name)

    def read_csv(
            self,
            source_list: str | list[str] | tuple[str],
            table_name: str | None = None,
            **kwargs: Any,
    ) -> ir.Table:
        """Register a CSV file as a table in the current database.

        Parameters
        ----------
        source_list
            The data source(s). May be a path to a file or directory of CSV files, or an
            iterable of CSV files.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        **kwargs
            Additional keyword arguments passed to DuckDB loading function.
            See https://duckdb.org/docs/data/csv for more information.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        source_list = util.normalize_filenames(source_list)

        if not table_name:
            table_name = util.gen_name("read_csv")

        kwargs.setdefault("header", True)
        kwargs["auto_detect"] = kwargs.pop("auto_detect", "columns" not in kwargs)
        # TODO: clean this up
        # We want to _usually_ quote arguments but if we quote `columns` it messes
        # up DuckDB's struct parsing.
        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]

        if (columns := kwargs.pop("columns", None)) is not None:
            options.append(
                sg.to_identifier("columns").eq(
                    sge.Struct(
                        expressions=[
                            sge.PropertyEQ(
                                this=sge.convert(key), expression=sge.convert(value)
                            )
                            for key, value in columns.items()
                        ]
                    )
                )
            )

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.read_csv(source_list, *options)),
        )

        return self.table(table_name)

    def read_parquet(
            self,
            source_list: str | Iterable[str],
            table_name: str | None = None,
            **kwargs: Any,
    ) -> ir.Table:
        """Register a parquet file as a table in the current database.

        Parameters
        ----------
        source_list
            The data source(s). May be a path to a file, an iterable of files,
            or directory of parquet files.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        **kwargs
            Additional keyword arguments passed to DuckDB loading function.
            See https://duckdb.org/docs/data/parquet for more information.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        table_name = table_name or util.gen_name("read_parquet")

        # Default to using the native duckdb parquet reader
        # If that fails because of auth issues, fall back to ingesting via
        # pyarrow dataset
        try:
            self._read_parquet_duckdb_native(source_list, table_name, **kwargs)
        except duckdb.IOException:
            self._read_parquet_pyarrow_dataset(source_list, table_name, **kwargs)

        return self.table(table_name)

    def _read_parquet_duckdb_native(
            self, source_list: str | Iterable[str], table_name: str, **kwargs: Any
    ) -> None:
        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.read_parquet(source_list, *options)),
        )

    def _read_parquet_pyarrow_dataset(
            self, source_list: str | Iterable[str], table_name: str, **kwargs: Any
    ) -> None:
        import pyarrow.dataset as ds

        dataset = ds.dataset(list(map(ds.dataset, source_list)), **kwargs)
        # We don't create a view since DuckDB special cases Arrow Datasets
        # so if we also create a view we end up with both a "lazy table"
        # and a view with the same name
        self.con.register(table_name, dataset)
        # DuckDB normally auto-detects Arrow Datasets that are defined
        # in local variables but the `dataset` variable won't be local
        # by the time we execute against this so we register it
        # explicitly.

    def read_in_memory(
            # TODO: deprecate this in favor of `create_table`
            self,
            source: pd.DataFrame
                    | pa.Table
                    | pa.RecordBatchReader
                    | pl.DataFrame
                    | pl.LazyFrame,
            table_name: str | None = None,
    ) -> ir.Table:
        """Register an in-memory table object in the current database.

        Supported objects include pandas DataFrame, a Polars
        DataFrame/LazyFrame, or a PyArrow Table or RecordBatchReader.

        Parameters
        ----------
        source
            The data source.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.

        Returns
        -------
        ir.Table
            The just-registered table

        """
        table_name = table_name or util.gen_name("read_in_memory")
        _read_in_memory(source, table_name, self)
        return self.table(table_name)

    def read_delta(
            self, source_table: str, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        """Register a Delta Lake table as a table in the current database.

        Parameters
        ----------
        source_table
            The data source. Must be a directory containing a Delta Lake table.
        table_name
            An optional name to use for the created table. This defaults to a
            generated name.
        kwargs
            Additional keyword arguments passed to the `delta` extension's
            `delta_scan` function.

        Returns
        -------
        ir.Table
            The just-registered table.

        """
        source_table = util.normalize_filenames(source_table)[0]

        table_name = table_name or util.gen_name("read_delta")

        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.delta_scan(source_table, *options)),
        )

        return self.table(table_name)

    def list_tables(
            self,
            like: str | None = None,
            database: tuple[str, str] | str | None = None,
            schema: str | None = None,
    ) -> list[str]:
        """List tables and views.

        ::: {.callout-note}
        ## Ibis does not use the word `schema` to refer to database hierarchy.

        A collection of tables is referred to as a `database`.
        A collection of `database` is referred to as a `catalog`.

        These terms are mapped onto the corresponding features in each
        backend (where available), regardless of whether the backend itself
        uses the same terminology.
        :::

        Parameters
        ----------
        like
            Regex to filter by table/view name.
        database
            Database location. If not passed, uses the current database.

            By default uses the current `database` (`self.current_database`) and
            `catalog` (`self.current_catalog`).

            To specify a table in a separate catalog, you can pass in the
            catalog and database as a string `"catalog.database"`, or as a tuple of
            strings `("catalog", "database")`.
        schema
            [deprecated] Schema name. If not passed, uses the current schema.

        Returns
        -------
        list[str]
            List of table and view names.

        Examples
        --------
        >>> import ibis
        >>> con = ibis.duckdb.connect()
        >>> foo = con.create_table("foo", schema=ibis.schema(dict(a="int")))
        >>> con.list_tables()
        ['foo']
        >>> bar = con.create_view("bar", foo)
        >>> con.list_tables()
        ['bar', 'foo']
        >>> con.create_database("my_database")
        >>> con.list_tables(database="my_database")
        []
        >>> with con.begin() as c:
        ...     c.exec_driver_sql("CREATE TABLE my_database.baz (a INTEGER)")  # doctest: +ELLIPSIS
        <...>
        >>> con.list_tables(database="my_database")
        ['baz']

        """
        table_loc = self._warn_and_create_table_loc(database, schema)

        catalog = self.current_catalog
        database = self.current_database
        if table_loc is not None:
            catalog = table_loc.catalog or catalog
            database = table_loc.db or database

        col = "table_name"
        sql = (
            sg.select(col)
            .from_(sg.table("tables", db="information_schema"))
            .distinct()
            .where(
                C.table_catalog.isin(sge.convert(catalog), sge.convert("temp")),
                C.table_schema.eq(sge.convert(database)),
            )
            .sql(self.dialect)
        )
        with self._safe_raw_sql(sql) as cur:
            out = cur.fetch_arrow_table()

        return self._filter_with_like(out[col].to_pylist(), like)

    def read_postgres(
            self, uri: str, *, table_name: str | None = None, database: str = "public"
    ) -> ir.Table:
        """Register a table from a postgres instance into a DuckDB table.

        ::: {.callout-note}
        ## Ibis does not use the word `schema` to refer to database hierarchy.

        A collection of `table` is referred to as a `database`.
        A collection of `database` is referred to as a `catalog`.

        These terms are mapped onto the corresponding features in each
        backend (where available), regardless of whether the backend itself
        uses the same terminology.
        :::

        Parameters
        ----------
        uri
            A postgres URI of the form `postgres://user:password@host:port`
        table_name
            The table to read
        database
            PostgreSQL database (schema) where `table_name` resides

        Returns
        -------
        ir.Table
            The just-registered table.

        """
        if table_name is None:
            raise ValueError(
                "`table_name` is required when registering a postgres table"
            )
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(
                self.compiler.f.postgres_scan_pushdown(uri, database, table_name)
            ),
        )

        return self.table(table_name)

    def read_mysql(
            self,
            uri: str,
            *,
            catalog: str,
            table_name: str | None = None,
    ) -> ir.Table:
        """Register a table from a MySQL instance into a DuckDB table.

        Parameters
        ----------
        uri
            A mysql URI of the form `mysql://user:password@host:port/database`
        catalog
            User-defined alias given to the MySQL database that is being attached
            to DuckDB
        table_name
            The table to read

        Returns
        -------
        ir.Table
            The just-registered table.
        """

        parsed = urllib.parse.urlparse(uri)

        if table_name is None:
            raise ValueError("`table_name` is required when registering a mysql table")

        database = parsed.path.strip("/")

        query_con = f"""ATTACH 'host={parsed.hostname} user={parsed.username} password={parsed.password} port={parsed.port} database={database}' AS {catalog} (TYPE mysql)"""

        with self._safe_raw_sql(query_con) as cur:
            cur.fetchall()

        return self.table(table_name, database=(catalog, database))

    def attach(
            self, path: str | Path, name: str | None = None, read_only: bool = False
    ) -> None:
        """Attach another DuckDB database to the current DuckDB session.

        Parameters
        ----------
        path
            Path to the database to attach.
        name
            Name to attach the database as. Defaults to the basename of `path`.
        read_only
            Whether to attach the database as read-only.

        """
        code = f"ATTACH '{path}'"

        if name is not None:
            name = sg.to_identifier(name).sql(dialect=self.dialect)
            code += f" AS {name}"

        if read_only:
            code += " (READ_ONLY)"

        with self._safe_raw_sql(code) as cur:
            cur.fetchall()

    def detach(self, name: str) -> None:
        """Detach a database from the current DuckDB session.

        Parameters
        ----------
        name
            The name of the database to detach.

        """
        name = sg.to_identifier(name).sql(dialect=self.dialect)

        with self._safe_raw_sql(f"DETACH {name}") as cur:
            cur.fetchall()

    def register_filesystem(self, filesystem: AbstractFileSystem):
        """Register an `fsspec` filesystem object with DuckDB.

        This allow a user to read from any `fsspec` compatible filesystem using
        `read_csv`, `read_parquet`, `read_json`, etc.


        ::: {.callout-note}
        Creating an `fsspec` filesystem requires that the corresponding
        backend-specific `fsspec` helper library is installed.

        e.g. to connect to Google Cloud Storage, `gcsfs` must be installed.
        :::

        Parameters
        ----------
        filesystem
            The fsspec filesystem object to register with DuckDB.
            See https://duckdb.org/docs/guides/python/filesystems for details.

        Examples
        --------
        >>> import ibis
        >>> import fsspec
        >>> gcs = fsspec.filesystem("gcs")
        >>> con = ibis.duckdb.connect()
        >>> con.register_filesystem(gcs)
        >>> t = con.read_csv(
        ...     "gcs://ibis-examples/data/band_members.csv.gz",
        ...     table_name="band_members",
        ... )
        DatabaseTable: band_members
          name string
          band string

        """
        self.con.register_filesystem(filesystem)

    def _run_pre_execute_hooks(self, expr: ir.Expr) -> None:
        # Warn for any tables depending on RecordBatchReaders that have already
        # started being consumed.
        for t in expr.op().find(ops.PhysicalTable):
            started = self._record_batch_readers_consumed.get(t.name)
            if started is True:
                warnings.warn(
                    f"Table {t.name!r} is backed by a `pyarrow.RecordBatchReader` "
                    "that has already been partially consumed. This may lead to "
                    "unexpected results. Either recreate the table from a new "
                    "`pyarrow.RecordBatchReader`, or use `Table.cache()`/"
                    "`con.create_table()` to consume and store the results in "
                    "the backend to reuse later."
                )
            elif started is False:
                self._record_batch_readers_consumed[t.name] = True

        super()._run_pre_execute_hooks(expr)

    def _to_pyarrow_table(
            self,
            expr: ir.Expr,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
    ) -> pa.Table:
        """Preprocess the expr, and return a ``pyarrow.Table`` object.
        """
        self._run_pre_execute_hooks(expr)
        table_expr = expr.as_table()
        sql = self.compile(table_expr, limit=limit, params=params)
        with self._safe_raw_sql(sql) as cur:
            return cur.fetch_arrow_table()

    def to_pyarrow_batches(
            self,
            expr: ir.Expr,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
            chunk_size: int = 1_000_000,
            **_: Any,
    ) -> pa.ipc.RecordBatchReader:
        """Return a stream of record batches.

        The returned `RecordBatchReader` contains a cursor with an unbounded lifetime.

        For analytics use cases this is usually nothing to fret about. In some cases you
        may need to explicit release the cursor.

        Parameters
        ----------
        expr
            Ibis expression
        params
            Bound parameters
        limit
            Limit the result to this number of rows
        chunk_size
            ::: {.callout-warning}
            ## DuckDB returns 1024 size batches regardless of what argument is passed.
            :::

        """
        self._run_pre_execute_hooks(expr)
        table = expr.as_table()
        sql = self.compile(table, limit=limit, params=params)

        def batch_producer(cur):
            yield from cur.fetch_record_batch(rows_per_batch=chunk_size)

        result = self.raw_sql(sql)
        return pa.ipc.RecordBatchReader.from_batches(
            expr.as_table().schema().to_pyarrow(), batch_producer(result)
        )

    def to_pyarrow(
            self,
            expr: ir.Expr,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
            **_: Any,
    ) -> pa.Table:
        return self._to_pyarrow_table(expr, params=params, limit=limit)

    def execute(
            self,
            expr: ir.Expr,
            params: Mapping | None = None,
            limit: str | None = "default",
            **_: Any,
    ) -> Any:
        """Execute an expression."""
        import pandas as pd
        import pyarrow.types as pat

        table = self._to_pyarrow_table(expr, params=params, limit=limit)

        df = pd.DataFrame(
            {
                name: (
                    col.to_pylist()
                    if (
                            pat.is_nested(col.type)
                            or
                            # pyarrow / duckdb type null literals columns as int32?
                            # but calling `to_pylist()` will render it as None
                            col.null_count
                    )
                    else col.to_pandas()
                )
                for name, col in zip(table.column_names, table.columns)
            }
        )
        df = DuckDBPandasData.convert_table(df, expr.as_table().schema())
        return expr.__pandas_result__(df)

    @util.experimental
    def to_torch(
            self,
            expr: ir.Expr,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
            **kwargs: Any,
    ) -> dict[str, torch.Tensor]:
        """Execute an expression and return results as a dictionary of torch tensors.

        Parameters
        ----------
        expr
            Ibis expression to execute.
        params
            Parameters to substitute into the expression.
        limit
            An integer to effect a specific row limit. A value of `None` means no limit.
        kwargs
            Keyword arguments passed into the backend's `to_torch` implementation.

        Returns
        -------
        dict[str, torch.Tensor]
            A dictionary of torch tensors, keyed by column name.

        """
        return self._to_pyarrow_table(expr, params=params, limit=limit).torch()

    @util.experimental
    def to_parquet(
            self,
            expr: ir.Table,
            path: str | Path,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            **kwargs: Any,
    ) -> None:
        """Write the results of executing the given expression to a parquet file.

        This method is eager and will execute the associated expression
        immediately.

        Parameters
        ----------
        expr
            The ibis expression to execute and persist to parquet.
        path
            The data source. A string or Path to the parquet file.
        params
            Mapping of scalar parameter expressions to value.
        **kwargs
            DuckDB Parquet writer arguments. See
            https://duckdb.org/docs/data/parquet#writing-to-parquet-files for
            details

        Examples
        --------
        Write out an expression to a single parquet file.

        >>> import ibis
        >>> penguins = ibis.examples.penguins.fetch()
        >>> con = ibis.get_backend(penguins)
        >>> con.to_parquet(penguins, "/tmp/penguins.parquet")

        Write out an expression to a hive-partitioned parquet file.

        >>> import tempfile
        >>> penguins = ibis.examples.penguins.fetch()
        >>> con = ibis.get_backend(penguins)

        Partition on a single column.

        >>> con.to_parquet(penguins, tempfile.mkdtemp(), partition_by="year")

        Partition on multiple columns.

        >>> con.to_parquet(penguins, tempfile.mkdtemp(), partition_by=("year", "island"))

        """
        self._run_pre_execute_hooks(expr)
        query = self.compile(expr, params=params)
        args = ["FORMAT 'parquet'", *(f"{k.upper()} {v!r}" for k, v in kwargs.items())]
        copy_cmd = f"COPY ({query}) TO {str(path)!r} ({', '.join(args)})"
        with self._safe_raw_sql(copy_cmd) as cur:
            cur.fetchall()

    @util.experimental
    def to_csv(
            self,
            expr: ir.Table,
            path: str | Path,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            header: bool = True,
            **kwargs: Any,
    ) -> None:
        """Write the results of executing the given expression to a CSV file.

        This method is eager and will execute the associated expression
        immediately.

        Parameters
        ----------
        expr
            The ibis expression to execute and persist to CSV.
        path
            The data source. A string or Path to the CSV file.
        params
            Mapping of scalar parameter expressions to value.
        header
            Whether to write the column names as the first line of the CSV file.
        **kwargs
            DuckDB CSV writer arguments. https://duckdb.org/docs/data/csv/overview.html#parameters

        """
        self._run_pre_execute_hooks(expr)
        query = self.compile(expr, params=params)
        args = [
            "FORMAT 'csv'",
            f"HEADER {int(header)}",
            *(f"{k.upper()} {v!r}" for k, v in kwargs.items()),
        ]
        copy_cmd = f"COPY ({query}) TO {str(path)!r} ({', '.join(args)})"
        with self._safe_raw_sql(copy_cmd) as cur:
            cur.fetchall()

    def _get_schema_using_query(self, query: str) -> sch.Schema:
        with self._safe_raw_sql(f"DESCRIBE {query}") as cur:
            rows = cur.fetch_arrow_table()

        rows = rows.to_pydict()

        type_mapper = self.compiler.type_mapper
        return sch.Schema(
            {
                name: type_mapper.from_string(typ, nullable=null == "YES")
                for name, typ, null in zip(
                rows["column_name"], rows["column_type"], rows["null"]
            )
            }
        )

    def _get_temp_view_definition(self, name: str, definition: str) -> str:
        return sge.Create(
            this=sg.to_identifier(name, quoted=self.compiler.quoted),
            kind="VIEW",
            expression=definition,
            replace=True,
            properties=sge.Properties(expressions=[sge.TemporaryProperty()]),
        )

    def _create_temp_view(self, table_name, source):
        with self._safe_raw_sql(self._get_temp_view_definition(table_name, source)) as cur:
            cur.fetchall()


@lazy_singledispatch
def _read_in_memory(source: Any, table_name: str, _conn: Backend, **kwargs: Any):
    raise NotImplementedError(
        f"The `{_conn.name}` backend currently does not support "
        f"reading data of {type(source)!r}"
    )
