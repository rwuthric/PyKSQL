# PyKSQL
[![Lint with Flake8](https://github.com/rwuthric/PyKSQL/actions/workflows/lint.yml/badge.svg)](https://github.com/rwuthric/PyKSQL/actions/workflows/lint.yml)

Simple Python client to ksqlDB

This code is inspired from the work of [@sauljabin](https://github.com/sauljabin) taken from [here](https://github.com/sauljabin/kayak/tree/main/kayak/ksql).

## Usage
To connect to a running `ksqlDB` server (in this example running on `localhost` on port `8088`):
```
from pyksql.ksql import KSQL
ksql = KSQL("http://localhost:8088")
```
General information about the `ksqlDB` server can be obtained like so:
```
print("info", ksql.info())
```
To list the Kafka topics, defined streams and tables:
```
print("topics", ksql.topics())
print("streams", ksql.streams())
print("tables", ksql.tables())
```
To send a query to the `ksqlDB` server:
```
asyncio.run(
        ksql.query(
            "SELECT * FROM services;",
            on_init=print,
            on_new_row=print,
            on_close=lambda: print("No more entries"),
        )
    )
```
To stream the content of a ksql TABLE `services` to a [pandas](https://pandas.pydata.org/) [DataFrame](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html):
```
df = asyncio.run(
    ksql.query_to_dataframe("SELECT * FROM services;")
)
```
To run queries in a `Jupyter` NoteBook use:
```
await ksql.query(
    "SELECT * FROM services;",
    on_init=print,
    on_new_row=print,
    on_close=lambda: print("No more entries"),
)
```
and
```
df = await ksql.query_to_dataframe(SELECT * FROM services;)
```

## Installation
Create a local clone of this repository and in the root directory of the cloned code run:
```
pip install .
```
or install it directly from GitHub:
```
pip install git+https://github.com/rwuthric/PyKSQL.git
```

## Uninstall
Use
```
pip uninstall pyksql
```
