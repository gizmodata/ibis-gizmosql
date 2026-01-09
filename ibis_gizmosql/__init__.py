"""GizmoSQL backend."""

from __future__ import annotations

import ast
import contextlib
import re
import urllib
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote_plus

import ibis
import ibis.backends.sql.compilers as sc
import ibis.common.exceptions as exc
import ibis.expr.operations as ops
import ibis.expr.schema as sch
import ibis.expr.types as ir
import sqlglot as sg
import sqlglot.expressions as sge
from adbc_driver_flightsql import dbapi as gizmosql, DatabaseOptions
from ibis import util
from ibis.backends import (
    CanCreateDatabase,
    DirectExampleLoader,
    HasCurrentCatalog,
    HasCurrentDatabase,
    UrlFromPath,
)
from ibis.backends.sql import SQLBackend
from ibis.backends.sql.compilers.base import STAR, C
from ibis.common.dispatch import lazy_singledispatch
from packaging.version import parse as vparse

from ibis_gizmosql.converter import DuckDBPandasData

__version__ = "0.0.14"

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, MutableMapping, Sequence
    from urllib.parse import ParseResult

    import pandas as pd
    import polars as pl
    import pyarrow as pa
    import pyarrow_hotfix  # noqa: F401
    import torch
    from fsspec import AbstractFileSystem

    from ibis.expr.schema import SchemaLike


class _Settings:
    def __init__(self, con: gizmosql.Connection) -> None:
        self.con = con

    def __getitem__(self, key: str) -> Any:
        with self.con.cursor() as cur:
            cur.execute(f"SELECT value FROM duckdb_settings() WHERE name = {key!r}")
            maybe_value = cur.fetchone()
        if maybe_value is not None:
            return maybe_value[0]
        raise KeyError(key)

    def __setitem__(self, key, value):
        with self.con.cursor() as cur:
            cur.execute(f"SET {key} = {str(value)!r}")

    def __repr__(self):
        with self.con.cursor() as cur:
            return repr(cur.execute("SELECT * FROM duckdb_settings()").fetchall())


class Backend(
    SQLBackend,
    CanCreateDatabase,
    HasCurrentCatalog,
    HasCurrentDatabase,
    UrlFromPath,
    DirectExampleLoader,
):
    name = "gizmosql"
    compiler = sc.duckdb.compiler

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

    def create_table(
            self,
            name: str,
            /,
            obj: ir.Table
                 | pd.DataFrame
                 | pa.Table
                 | pl.DataFrame
                 | pl.LazyFrame
                 | None = None,
            *,
            schema: SchemaLike | None = None,
            database: str | None = None,
            temp: bool = False,
            overwrite: bool = False,
    ):
        """Create a table in GizmoSQL.

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

        catalog = table_loc.catalog or self.current_catalog
        database = table_loc.db or self.current_database

        if obj is None and schema is None:
            raise ValueError("Either `obj` or `schema` must be specified")

        quoted = self.compiler.quoted
        dialect = self.dialect

        properties = []

        if temp:
            properties.append(sge.TemporaryProperty())
            catalog = "temp"
            database = "main"

        if obj is not None:
            if not isinstance(obj, ir.Expr):
                table = ibis.memtable(obj)
            else:
                table = obj

            query = self.compiler.to_sqlglot(table)
        else:
            query = None

        if schema is None:
            schema = table.schema()
        else:
            schema = ibis.schema(schema)

        if null_fields := schema.null_fields:
            raise exc.IbisTypeError(
                "GizmoSQL / DuckDB does not support creating tables with NULL typed columns. "
                "Ensure that every column has non-NULL type. "
                f"NULL columns: {null_fields}"
            )

        if overwrite:
            temp_name = util.gen_name("gizmosql_table")
        else:
            temp_name = name

        initial_table = sg.table(temp_name, catalog=catalog, db=database, quoted=quoted)
        target = sge.Schema(this=initial_table, expressions=schema.to_sqlglot(dialect))

        create_stmt = sge.Create(
            kind="TABLE",
            this=target,
            properties=sge.Properties(expressions=properties),
        )

        # This is the same table as initial_table unless overwrite == True
        final_table = sg.table(name, catalog=catalog, db=database, quoted=quoted)
        with self._safe_raw_sql(create_stmt) as create_table_cur:
            with self.con.cursor() as cur:
                if query is not None:
                    insert_stmt = sge.insert(
                        query, into=initial_table, columns=table.columns
                    ).sql(dialect)
                    cur.execute(insert_stmt).fetchall()

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
                            sge.Drop(kind="TABLE", this=initial_table, exists=True).sql(dialect=self.dialect)
                        )
                    else:
                        cur.execute(
                            sge.AlterTable(
                                this=initial_table,
                                actions=[sge.RenameTable(this=final_table)],
                            ).sql(dialect=self.dialect)
                        )

        return self.table(name, database=(catalog, database))

    def table(self, name: str, /, *, database: str | None = None) -> ir.Table:
        table_loc = self._to_sqlglot_table(database)

        # TODO: set these to better defaults
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
        query = sge.Describe(
            this=sg.table(
                table_name, db=database, catalog=catalog, quoted=self.compiler.quoted
            )
        ).sql(self.dialect)

        with self._safe_raw_sql(query) as cur:
            meta = cur.fetch_arrow_table()

        names = meta["column_name"].to_pylist()
        types = meta["column_type"].to_pylist()
        nullables = meta["null"].to_pylist()

        type_mapper = self.compiler.type_mapper
        return sch.Schema(
            {
                name: type_mapper.from_string(typ, nullable=null == "YES")
                for name, typ, null in zip(names, types, nullables)
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
            self, *, like: str | None = None, catalog: str | None = None
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

    @staticmethod
    def _convert_kwargs(kwargs: MutableMapping) -> None:
        read_only = str(kwargs.pop("read_only", "False")).capitalize()
        try:
            kwargs["read_only"] = ast.literal_eval(read_only)
        except ValueError as e:
            raise ValueError(
                f"invalid value passed to ast.literal_eval: {read_only!r}"
            ) from e

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
        """Create an Ibis client connected to GizmoSQL database.

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
            GizmoSQL schema to use. If `None`, use the default `search_path`.
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
        >>> host = os.environ.get("IBIS_TEST_GIZMOSQL_HOST", "localhost")
        >>> user = os.environ.get("IBIS_TEST_GIZMOSQL_USER", getpass.getuser())
        >>> password = os.environ.get("IBIS_TEST_GIZMOSQL_PASSWORD")
        >>> database = os.environ.get("IBIS_TEST_GIZMOSQL_DATABASE", "ibis_testing")
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

        self.con = gizmosql.connect(uri=f"{connection_scheme}://{host}:{port}",
                                    db_kwargs=db_kwargs
                                    )

        vendor_version = self.con.adbc_get_info().get("vendor_version")

        if not re.search(pattern="^duckdb ", string=vendor_version):
            raise exc.UnsupportedBackendType(f"Unsupported GizmoSQL server backend: '{vendor_version}'")

        # Default timezone, can't be set with `config`
        self.settings["timezone"] = "UTC"

        self._record_batch_readers_consumed = {}

    @util.experimental
    @classmethod
    def from_connection(
            cls,
            con: gizmosql.Connection,
            /,
            *,
            extensions: Sequence[str] | None = None,
    ) -> Backend:
        """Create an Ibis client from an existing connection to a DuckDB database.

        Parameters
        ----------
        con
            An existing connection to a GizmoSQL database.
        extensions
            A list of duckdb extensions to install/load upon connection.
        """
        new_backend = cls(extensions=extensions)
        new_backend._can_reconnect = False
        new_backend.con = con
        new_backend._post_connect(extensions)
        return new_backend

    def _post_connect(self, extensions: Sequence[str] | None = None) -> None:
        # Load any pre-specified extensions
        if extensions is not None:
            self._load_extensions(extensions)

        # Default timezone, can't be set with `config`
        self.settings["timezone"] = "UTC"

        # setting this to false disables magic variables-as-tables discovery,
        # hopefully eliminating large classes of bugs
        if vparse(self.version) > vparse("1"):
            self.settings["python_enable_replacements"] = False

        self._record_batch_readers_consumed = {}

    def _load_extensions(
            self, extensions: list[str], force_install: bool = False
    ) -> None:
        f = self.compiler.f
        query = (
            sg.select(
                f.anon.unnest(
                    f.list_intersect(
                        f.list_append(C.aliases, C.extension_name),
                        f.list_value(*extensions),
                    )
                ),
                C.installed,
                C.loaded,
            )
            .from_(f.duckdb_extensions())
            .where(sg.not_(C.installed & C.loaded))
        )
        with self._safe_raw_sql(query) as cur:
            if not (not_installed_or_loaded := cur.fetchall()):
                return

            commands = [
                "FORCE " * force_install + f"INSTALL '{extension}'"
                for extension, installed, _ in not_installed_or_loaded
                if not installed
            ]
            commands.extend(
                f"LOAD '{extension}'"
                for extension, _, loaded in not_installed_or_loaded
                if not loaded
            )
            for command in commands:
                cur.execute(command)

    def load_extension(self, extension: str, force_install: bool = False) -> None:
        """Install and load a duckdb extension by name or path.

        Parameters
        ----------
        extension
            The extension name or path.
        force_install
            Force reinstallation of the extension.

        """
        self._load_extensions([extension], force_install=force_install)

    def create_database(
            self, name: str, /, *, catalog: str | None = None, force: bool = False
    ) -> None:
        if catalog is not None:
            raise exc.UnsupportedOperationError(
                "GizmoSQL cannot create a database in another catalog."
            )

        name = sg.table(name, catalog=catalog, quoted=self.compiler.quoted)
        with self._safe_raw_sql(sge.Create(this=name, kind="SCHEMA", replace=force)):
            pass

    def drop_database(
            self, name: str, /, *, catalog: str | None = None, force: bool = False
    ) -> None:
        if catalog is not None and catalog != self.current_catalog:
            raise exc.UnsupportedOperationError(
                "GizmoSQL cannot drop a database in another catalog."
            )

        name = sg.table(name, catalog=catalog, quoted=self.compiler.quoted)
        with self._safe_raw_sql(sge.Drop(this=name, kind="SCHEMA", replace=force)):
            pass

    @util.experimental
    def read_json(
            self,
            paths: str | list[str] | tuple[str],
            /,
            *,
            table_name: str | None = None,
            columns: Mapping[str, str] | None = None,
            **kwargs,
    ) -> ir.Table:
        """Read newline-delimited JSON into an ibis table.

        ::: {.callout-note}
        ## This feature requires duckdb>=0.7.0
        :::

        Parameters
        ----------
        paths
            File or list of files
        table_name
            Optional table name
        columns
            Optional mapping from string column name to duckdb type string.
        **kwargs
            Additional keyword arguments passed to DuckDB's `read_json_auto` function.

            See https://duckdb.org/docs/data/json/overview.html#json-loading
            for parameters and more information about reading JSON.

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

        if columns:
            options.append(
                sg.to_identifier("columns").eq(
                    sge.Struct.from_arg_list(
                        [
                            sge.PropertyEQ(
                                this=sg.to_identifier(key),
                                expression=sge.convert(value),
                            )
                            for key, value in columns.items()
                        ]
                    )
                )
            )

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(
                self.compiler.f.read_json_auto(
                    util.normalize_filenames(paths), *options
                )
            ),
        )

        return self.table(table_name)

    def read_csv(
            self,
            paths: str | list[str] | tuple[str],
            /,
            *,
            table_name: str | None = None,
            columns: Mapping[str, str | dt.DataType] | None = None,
            types: Mapping[str, str | dt.DataType] | None = None,
            **kwargs: Any,
    ) -> ir.Table:
        """Register a CSV file as a table in the current database.

        Parameters
        ----------
        paths
            The data source(s). May be a path to a file or directory of CSV
            files, or an iterable of CSV files.
        table_name
            An optional name to use for the created table. This defaults to a
            sequentially generated name.
        columns
            An optional mapping of **all** column names to their types.
        types
            An optional mapping of a **subset** of column names to their types.
        **kwargs
            Additional keyword arguments passed to DuckDB loading function. See
            https://duckdb.org/docs/data/csv for more information.

        Returns
        -------
        ir.Table
            The just-registered table

        Examples
        --------
        Generate some data

        >>> import tempfile
        >>> data = b'''
        ... lat,lon,geom
        ... 1.0,2.0,POINT (1 2)
        ... 2.0,3.0,POINT (2 3)
        ... '''
        >>> with tempfile.NamedTemporaryFile(delete=False) as f:
        ...     nbytes = f.write(data)

        Import Ibis

        >>> import ibis
        >>> from ibis import _
        >>> ibis.options.interactive = True
        >>> con = ibis.duckdb.connect()

        Read the raw CSV file

        >>> t = con.read_csv(f.name)
        >>> t
        ┏━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━┓
        ┃ lat     ┃ lon     ┃ geom        ┃
        ┡━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━┩
        │ float64 │ float64 │ string      │
        ├─────────┼─────────┼─────────────┤
        │     1.0 │     2.0 │ POINT (1 2) │
        │     2.0 │     3.0 │ POINT (2 3) │
        └─────────┴─────────┴─────────────┘

        Load the `spatial` extension and read the CSV file again, using
        specific column types

        >>> con.load_extension("spatial")
        >>> t = con.read_csv(f.name, types={"geom": "geometry"})
        >>> t
        ┏━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━┓
        ┃ lat     ┃ lon     ┃ geom                 ┃
        ┡━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━┩
        │ float64 │ float64 │ geospatial:geometry  │
        ├─────────┼─────────┼──────────────────────┤
        │     1.0 │     2.0 │ <POINT (1 2)>        │
        │     2.0 │     3.0 │ <POINT (2 3)>        │
        └─────────┴─────────┴──────────────────────┘
        """
        paths = util.normalize_filenames(paths)

        if not table_name:
            table_name = util.gen_name("read_csv")

        # auto_detect and columns collide, so we set auto_detect=True
        # unless COLUMNS has been specified
        if any(source.startswith(("http://", "https://", "s3://")) for source in paths):
            self._load_extensions(["httpfs"])

        kwargs.setdefault("header", True)
        kwargs["auto_detect"] = kwargs.pop("auto_detect", columns is None)
        # TODO: clean this up
        # We want to _usually_ quote arguments but if we quote `columns` it messes
        # up DuckDB's struct parsing.
        options = [C[key].eq(sge.convert(val)) for key, val in kwargs.items()]

        def make_struct_argument(obj: Mapping[str, str | dt.DataType]) -> sge.Struct:
            expressions = []
            geospatial = False
            dialect = self.compiler.dialect
            possible_geospatial_types = (
                sge.DataType.Type.GEOGRAPHY,
                sge.DataType.Type.GEOMETRY,
            )

            for name, typ in obj.items():
                sgtype = sg.parse_one(typ, read=dialect, into=sge.DataType)
                geospatial |= sgtype.this in possible_geospatial_types
                prop = sge.PropertyEQ(
                    this=sge.to_identifier(name),
                    expression=sge.convert(sgtype.sql(dialect)),
                )
                expressions.append(prop)

            if geospatial:
                self._load_extensions(["spatial"])
            return sge.Struct(expressions=expressions)

        if columns is not None:
            options.append(C.columns.eq(make_struct_argument(columns)))

        if types is not None:
            options.append(C.types.eq(make_struct_argument(types)))

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.read_csv(paths, *options)),
        )

        return self.table(table_name)

    def read_parquet(
            self,
            paths: str | Path | Iterable[str | Path],
            /,
            *,
            table_name: str | None = None,
            **kwargs: Any,
    ) -> ir.Table:
        """Register a parquet file as a table in the current database.

        Parameters
        ----------
        paths
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
        paths = util.normalize_filenames(paths)

        table_name = table_name or util.gen_name("read_parquet")

        if any(path.startswith(("http://", "https://", "s3://")) for path in paths):
            self._load_extensions(["httpfs"])

        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.read_parquet(paths, *options)),
        )
        return self.table(table_name)

    def read_delta(
            self, path: str | Path, /, *, table_name: str | None = None, **kwargs: Any
    ) -> ir.Table:
        """Register a Delta Lake table as a table in the current database.

        Parameters
        ----------
        path
            The data source. Must be a directory containing a Delta Lake table.
        table_name
            An optional name to use for the created table. This defaults to
            a sequentially generated name.
        kwargs
            Additional keyword arguments passed to deltalake.DeltaTable.

        Returns
        -------
        ir.Table
            The just-registered table.
        """
        (path,) = util.normalize_filenames(path)

        extensions = ["delta"]
        if path.startswith(("http://", "https://", "s3://")):
            extensions.append("httpfs")

        table_name = table_name or util.gen_name("read_delta")

        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]

        self._load_extensions(extensions)

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.delta_scan(path, *options)),
        )
        return self.table(table_name)

    def list_tables(
            self, *, like: str | None = None, database: tuple[str, str] | str | None = None
    ) -> list[str]:
        table_loc = self._to_sqlglot_table(database)

        catalog = table_loc.catalog or self.current_catalog
        database = table_loc.db or self.current_database

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
            self, uri: str, /, *, table_name: str | None = None, database: str = "public"
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
        self._load_extensions(["postgres_scanner"])

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
            /,
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

        self._load_extensions(["mysql"])

        database = parsed.path.strip("/")

        query_con = f"""ATTACH 'host={parsed.hostname} user={parsed.username} password={parsed.password} port={parsed.port} database={database}' AS {catalog} (TYPE mysql)"""

        with self._safe_raw_sql(query_con):
            pass

        return self.table(table_name, database=(catalog, database))

    def read_sqlite(
            self, path: str | Path, /, *, table_name: str | None = None
    ) -> ir.Table:
        """Register a table from a SQLite database into a DuckDB table.

        Parameters
        ----------
        path
            The path to the SQLite database
        table_name
            The table to read

        Returns
        -------
        ir.Table
            The just-registered table.

        Examples
        --------
        >>> import ibis
        >>> import sqlite3
        >>> ibis.options.interactive = True
        >>> con = sqlite3.connect("/tmp/sqlite.db")
        >>> with con:
        ...     con.execute("DROP TABLE IF EXISTS t")  # doctest: +ELLIPSIS
        ...     con.execute("CREATE TABLE t (a INT, b TEXT)")  # doctest: +ELLIPSIS
        ...     con.execute(
        ...         "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
        ...     )  # doctest: +ELLIPSIS
        <...>
        >>> con.close()
        >>> con = ibis.connect("duckdb://")
        >>> t = con.read_sqlite("/tmp/sqlite.db", table_name="t")
        >>> t
        ┏━━━━━━━┳━━━━━━━━┓
        ┃ a     ┃ b      ┃
        ┡━━━━━━━╇━━━━━━━━┩
        │ int64 │ string │
        ├───────┼────────┤
        │     1 │ a      │
        │     2 │ b      │
        │     3 │ c      │
        └───────┴────────┘

        """

        if table_name is None:
            raise ValueError("`table_name` is required when registering a sqlite table")
        self._load_extensions(["sqlite"])

        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(
                self.compiler.f.sqlite_scan(
                    sg.to_identifier(str(path), quoted=True), table_name
                )
            ),
        )

        return self.table(table_name)

    @util.experimental
    def read_xlsx(
            self,
            path: str | Path,
            /,
            *,
            sheet: str | None = None,
            range: str | None = None,
            **kwargs,
    ) -> ir.Table:
        """Read an Excel file into a DuckDB table. This requires duckdb>=1.2.0.

        Parameters
        ----------
        path
            The path to the Excel file.
        sheet
            The name of the sheet to read, eg 'Sheet3'.
        range
            The range of cells to read, eg 'A5:Z'.
        kwargs
            Additional args passed to the backend's read function.

        Returns
        -------
        ir.Table
            The just-registered table.

        See Also
        --------
        [DuckDB's `excel` extension docs for reading](https://duckdb.org/docs/stable/extensions/excel.html#reading-xlsx-files)

        Examples
        --------
        >>> import os
        >>> import ibis
        >>> t = ibis.memtable({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        >>> con = ibis.duckdb.connect()
        >>> con.to_xlsx(t, "/tmp/test.xlsx", header=True)
        >>> assert os.path.exists("/tmp/test.xlsx")
        >>> t = con.read_xlsx("/tmp/test.xlsx")
        >>> t.columns
        ('a', 'b')
        """
        table_name = util.gen_name("read_xlsx")

        if sheet:
            kwargs["sheet"] = sheet

        if range:
            kwargs["range"] = range

        options = [
            sg.to_identifier(key).eq(sge.convert(val)) for key, val in kwargs.items()
        ]

        self.load_extension("excel")
        self._create_temp_view(
            table_name,
            sg.select(STAR).from_(self.compiler.f.read_xlsx(str(path), *options)),
        )
        return self.table(table_name)

    def to_xlsx(
            self,
            expr: ir.Table,
            /,
            path: str | Path,
            *,
            sheet: str = "Sheet1",
            header: bool = False,
            params: Mapping[ir.Scalar, Any] | None = None,
            **kwargs: Any,
    ):
        """Write a table to an Excel file.

        Parameters
        ----------
        expr
            Ibis table expression to write to an excel file.
        path
            Excel output path.
        sheet
            The name of the sheet to write to, eg 'Sheet3'.
        header
            Whether to include the column names as the first row.
        params
            Additional Ibis expression parameters to pass to the backend's
            write function.
        kwargs
            Additional arguments passed to the backend's write function.

        Notes
        -----
        Requires DuckDB >= 1.2.0.

        See Also
        --------
        [DuckDB's `excel` extension docs for writing](https://duckdb.org/docs/stable/extensions/excel.html#writing-xlsx-files)

        Examples
        --------
        >>> import os
        >>> import ibis
        >>> t = ibis.memtable({"a": [1, 2, 3], "b": ["a", "b", "c"]})
        >>> con = ibis.duckdb.connect()
        >>> con.to_xlsx(t, "/tmp/test.xlsx")
        >>> os.path.exists("/tmp/test.xlsx")
        True
        """
        query = self.compile(expr, params=params)
        kwargs["sheet"] = sheet
        kwargs["header"] = header
        args = ["FORMAT 'xlsx'"]
        args.extend(f"{k.upper()} {v!r}" for k, v in kwargs.items())
        copy_cmd = f"COPY ({query}) TO {str(path)!r} ({', '.join(args)})"
        self.load_extension("excel")
        self.con.execute(copy_cmd).fetchall()

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
        name = sg.to_identifier(name).sql(self.name)
        self.con.execute(f"DETACH {name}").fetchall()

    def attach_sqlite(
            self, path: str | Path, overwrite: bool = False, all_varchar: bool = False
    ) -> None:
        """Attach a SQLite database to the current DuckDB session.

        Parameters
        ----------
        path
            The path to the SQLite database.
        overwrite
            Allow overwriting any tables or views that already exist in your current
            session with the contents of the SQLite database.
        all_varchar
            Set all SQLite columns to type `VARCHAR` to avoid type errors on ingestion.

        Examples
        --------
        >>> import ibis
        >>> import sqlite3
        >>> con = sqlite3.connect("/tmp/attach_sqlite.db")
        >>> with con:
        ...     con.execute("DROP TABLE IF EXISTS t")  # doctest: +ELLIPSIS
        ...     con.execute("CREATE TABLE t (a INT, b TEXT)")  # doctest: +ELLIPSIS
        ...     con.execute(
        ...         "INSERT INTO t VALUES (1, 'a'), (2, 'b'), (3, 'c')"
        ...     )  # doctest: +ELLIPSIS
        <...>
        >>> con.close()
        >>> con = ibis.connect("duckdb://")
        >>> con.list_tables()
        []
        >>> con.attach_sqlite("/tmp/attach_sqlite.db")
        >>> con.list_tables()
        ['t']

        """
        self.load_extension("sqlite")
        with self._safe_raw_sql(f"SET GLOBAL sqlite_all_varchar={all_varchar}") as cur:
            cur.execute(
                f"CALL sqlite_attach('{path}', overwrite={overwrite})"
            ).fetchall()

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
        >>> ibis.options.interactive = True
        >>> gcs = fsspec.filesystem("gcs")
        >>> con = ibis.duckdb.connect()
        >>> con.register_filesystem(gcs)
        >>> t = con.read_csv(
        ...     "gcs://ibis-examples/data/band_members.csv.gz",
        ...     table_name="band_members",
        ... )
        >>> t
        ┏━━━━━━━━┳━━━━━━━━━┓
        ┃ name   ┃ band    ┃
        ┡━━━━━━━━╇━━━━━━━━━┩
        │ string │ string  │
        ├────────┼─────────┤
        │ Mick   │ Stones  │
        │ John   │ Beatles │
        │ Paul   │ Beatles │
        └────────┴─────────┘
        """
        self.con.register_filesystem(filesystem)

    def _to_pyarrow_table(
            self,
            expr: ir.Expr,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
    ) -> pa.Table:
        """Preprocess the expr, and return a ``pyarrow.Table`` object.
        """
        table_expr = expr.as_table()
        sql = self.compile(table_expr, limit=limit, params=params)
        with self._safe_raw_sql(sql) as cur:
            return cur.fetch_arrow_table()

    def to_pyarrow_batches(
            self,
            expr: ir.Expr,
            /,
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
            The number of rows to fetch per batch
        """
        import pyarrow as pa
        import pyarrow_hotfix  # noqa: F401

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
            /,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
            **kwargs: Any,
    ) -> pa.Table:
        return self._to_pyarrow_table(expr, params=params, limit=limit)

    def execute(
            self,
            expr: ir.Expr,
            /,
            *,
            params: Mapping[ir.Scalar, Any] | None = None,
            limit: int | str | None = None,
            **kwargs: Any,
    ) -> pd.DataFrame | pd.Series | Any:
        """Execute an expression."""
        import pandas as pd
        import pyarrow.types as pat
        import pyarrow_hotfix  # noqa: F401

        table = self._to_pyarrow_table(expr, params=params, limit=limit)

        df = pd.DataFrame(
            {
                name: (
                    col.to_pylist()
                    if (
                            pat.is_nested(col.type)
                            or pat.is_dictionary(col.type)
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
            /,
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
            /,
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
            https://duckdb.org/docs/data/parquet/overview.html#writing-to-parquet-files
            for details.

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
        query = self.compile(expr, params=params)
        args = ["FORMAT 'parquet'", *(f"{k.upper()} {v!r}" for k, v in kwargs.items())]
        copy_cmd = f"COPY ({query}) TO {str(path)!r} ({', '.join(args)})"
        with self._safe_raw_sql(copy_cmd):
            pass

    @util.experimental
    def to_csv(
            self,
            expr: ir.Table,
            /,
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
        kwargs
            DuckDB CSV writer arguments. https://duckdb.org/docs/data/csv/overview.html#parameters
        """
        query = self.compile(expr, params=params)
        args = [
            "FORMAT 'csv'",
            f"HEADER {int(header)}",
            *(f"{k.upper()} {v!r}" for k, v in kwargs.items()),
        ]
        copy_cmd = f"COPY ({query}) TO {str(path)!r} ({', '.join(args)})"
        with self._safe_raw_sql(copy_cmd) as cur:
            pass

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

    def _register_in_memory_table(self, op: ops.InMemoryTable) -> None:
        raise exc.UnsupportedOperationError(
            "GizmoSQL does not yet support registering in-memory tables (coming soon)."
        )

    def _finalize_memtable(self, name: str) -> None:
        raise exc.UnsupportedOperationError(
            "GizmoSQL does not yet support registering in-memory tables (coming soon)."
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
        with self._safe_raw_sql(self._get_temp_view_definition(table_name, source)):
            pass


@lazy_singledispatch
def _read_in_memory(source: Any, table_name: str, _conn: Backend, **_: Any):
    raise NotImplementedError(
        f"The `{_conn.name}` backend currently does not support "
        f"reading data of {type(source)!r}"
    )


@_read_in_memory.register("polars.DataFrame")
@_read_in_memory.register("polars.LazyFrame")
@_read_in_memory.register("pyarrow.Table")
@_read_in_memory.register("pandas.DataFrame")
@_read_in_memory.register("pyarrow.dataset.Dataset")
def _default(source, table_name, _conn, **_: Any):
    _conn.con.register(table_name, source)


@_read_in_memory.register("pyarrow.RecordBatchReader")
def _pyarrow_rbr(source, table_name, _conn, **_: Any):
    _conn.con.register(table_name, source)
    # Ensure the reader isn't marked as started, in case the name is
    # being overwritten.
    _conn._record_batch_readers_consumed[table_name] = False
