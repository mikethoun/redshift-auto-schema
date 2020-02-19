# Redshift Auto Schema

Redshift Auto Schema is a Python library that takes a delimited flat file or parquet file as input, parses it, and provides a variety of functions that allow for the creation and validation of tables within Amazon Redshift. For each field, the appropriate Redshift data type is inferred from the contents of the file.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install Redshift Auto Schema.

```bash
pip install redshift-auto-schema
```

## Usage

```python
from redshift_auto_schema import RedshiftAutoSchema
import psycopg2 as pg

redshift_conn = pg.connect()

new_table = RedshiftAutoSchema(file='sample_file.parquet',
                               schema='test_schema',
                               table='test_table',
                               conn=redshift_conn)

if not new_table.check_table_existence():
    ddl = new_table.generate_table_ddl()

    with redshift_conn as conn:
    	with conn.cursor() as cur:
        	cur.execute(ddl)
```

## Methods

|NAME|DESCRIPTION|
|---|---|
|**get_column_list**|Returns column list based on header of file.|
|**check_schema_existence**|Checks Redshift for the existence of a schema.|
|**check_table_existence**|Checks Redshift for the existence of a table.|
|**generate_schema_ddl**|Returns a SQL statement that creates a Redshift schema.|
|**generate_schema_permissions**|Returns a SQL statement that grants schema usage to the default group.|
|**generate_table_ddl**|Returns a SQL statement that creates a Redshift table.|
|**generate_column_ddl**|Returns SQL statement(s) that adds missing column(s) a Redshift table.|
|**generate_table_permissions**|Returns a SQL statement that grants table read access to the default group.|
|**evaluate_table_ddl_diffs**|Returns a dataframe containing differences between generated and existing table DDL.|

## Contributing
Pull requests are welcome.

## License
[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)