# ibis-gizmosql
An [Ibis](https://ibis-project.org) back-end for [GizmoSQL](https://gizmodata.com/gizmosql)

[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Fibis--gizmosql-blue.svg?logo=Github">](https://github.com/gizmodata/ibis-gizmosql)
[<img src="https://img.shields.io/badge/GitHub-gizmodata%2Fgizmosql--public-blue.svg?logo=Github">](https://github.com/gizmodata/gizmosql-public)
[![ibis-gizmosql-ci](https://github.com/gizmodata/ibis-gizmosql/actions/workflows/ci.yml/badge.svg)](https://github.com/gizmodata/ibis-gizmosql/actions/workflows/ci.yml)
[![Supported Python Versions](https://img.shields.io/pypi/pyversions/ibis-gizmosql)](https://pypi.org/project/ibis-gizmosql/)
[![PyPI version](https://badge.fury.io/py/ibis-gizmosql.svg)](https://badge.fury.io/py/ibis-gizmosql)
[![PyPI Downloads](https://img.shields.io/pypi/dm/ibis-gizmosql.svg)](https://pypi.org/project/ibis-gizmosql/)

# Setup (to run locally)

## Install Python package
You can install `ibis-gizmosql` from PyPi or from source.

### Option 1 - from PyPi
```shell
# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

pip install ibis-gizmosql
```

### Option 2 - from source - for development
```shell
git clone https://github.com/gizmodata/ibis-gizmosql

cd ibis-gizmosql

# Create the virtual environment
python3 -m venv .venv

# Activate the virtual environment
. .venv/bin/activate

# Upgrade pip, setuptools, and wheel
pip install --upgrade pip setuptools wheel

# Install the Ibis GizmoSQL back-end - in editable mode with client and dev dependencies
pip install --editable .[dev,test]
```

### Note
For the following commands - if you running from source and using `--editable` mode (for development purposes) - you will need to set the PYTHONPATH environment variable as follows:
```shell
export PYTHONPATH=$(pwd)/ibis_gizmosql
```

### Usage
In this example - we'll start a GizmoSQL server with the DuckDB back-end in Docker, and connect to it from Python using Ibis.

First - start the GizmoSQL server - which by default mounts an empty in-memory database.  We use the `INIT_SQL_COMMANDS` env var in the `docker` command to create a very small TPC-H database upon startup:

```bash
docker run --name gizmosql \
           --detach \
           --rm \
           --tty \
           --init \
           --publish 31337:31337 \
           --env TLS_ENABLED="1" \
           --env GIZMOSQL_USERNAME="gizmosql_user" \
           --env GIZMOSQL_PASSWORD="gizmosql_password" \
           --env PRINT_QUERIES="1" \
           --env DATABASE_FILENAME=":memory:" \
           --env INIT_SQL_COMMANDS="CALL dbgen(sf=0.01);" \
           --pull always \
           gizmodata/gizmosql:latest
```

> [!IMPORTANT]
> The GizmoSQL server must be started with the DuckDB (default) back-end.  The SQLite back-end is not supported.

Next - connect to the GizmoSQL server from Python using Ibis by running this Python code:
```python
import os
import ibis
from ibis import _

# Kwarg connection example
con = ibis.gizmosql.connect(host="localhost",
                            user=os.getenv("GIZMOSQL_USERNAME", "gizmosql_user"),
                            password=os.getenv("GIZMOSQL_PASSWORD", "gizmosql_password"),
                            port=31337,
                            use_encryption=True,
                            disable_certificate_verification=True
                            )

# URL connection example
# con = ibis.connect("gizmosql://gizmosql_user:gizmosql_password@localhost:31337?disableCertificateVerification=True&useEncryption=True")

print(con.tables)

# assign the LINEITEM table to variable t (an Ibis table object)
t = con.table('lineitem')

# use the Ibis dataframe API to run TPC-H query 1
results = (t.filter(_.l_shipdate.cast('date') <= ibis.date('1998-12-01') + ibis.interval(days=90))
       .mutate(discount_price=_.l_extendedprice * (1 - _.l_discount))
       .mutate(charge=_.discount_price * (1 + _.l_tax))
       .group_by([_.l_returnflag,
                  _.l_linestatus
                  ]
                 )
       .aggregate(
            sum_qty=_.l_quantity.sum(),
            sum_base_price=_.l_extendedprice.sum(),
            sum_disc_price=_.discount_price.sum(),
            sum_charge=_.charge.sum(),
            avg_qty=_.l_quantity.mean(),
            avg_price=_.l_extendedprice.mean(),
            avg_disc=_.l_discount.mean(),
            count_order=_.count()
        )
       .order_by([_.l_returnflag,
                  _.l_linestatus
                  ]
                 )
       )

print(results.execute())
```

You should see output:
```text
  l_returnflag l_linestatus    sum_qty sum_base_price sum_disc_price     sum_charge avg_qty avg_price avg_disc  count_order
0            A            F  380456.00   532348211.65   505822441.49   526165934.00   25.58  35785.71     0.05        14876
1            N            F    8971.00    12384801.37    11798257.21    12282485.06   25.78  35588.51     0.05          348
2            N            O  765251.00  1072862302.10  1019517788.99  1060424708.62   25.47  35703.76     0.05        30049
3            R            F  381449.00   534594445.35   507996454.41   528524219.36   25.60  35874.01     0.05        14902
```

### Handy development commands

#### Version management

##### Bump the version of the application - (you must have installed from source with the [dev] extras)
```bash
bumpver update --patch
```
