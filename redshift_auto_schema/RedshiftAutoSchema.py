"""
Copyright 2019 Mike Thoun

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import psycopg2 as pg
import pandas as pd
import numpy as np
import re
from datetime import datetime
from dateutil import parser
from typing import List


class RedshiftAutoSchema():

    """RedshiftAutoSchema takes a delimited flat file or parquet file as input and infers the appropriate Redshift data type for each column.
    This class provides functions that allow for the automatic generation and validation of table schemas (and basic permissioning) in Redshift.

    Attributes:
        schema (str): Schema of the new Redshift table.
        table (str): Name of the new Redshift table.
        file (str): Path to delimited flat file or parquet file.
        export_date_field (bool, optional): Flag indicating whether export_date column should be added to new table.
        dist_key (str, optional): Name of column that should be the distribution key. If no column is specified, it will default to DISTSTYLE EVEN.
        sort_key (str, optional): Name of columns that should be the sort key (separated by commas).
        delimiter (str, optional): Flat file delimiter. Defaults to '|'.
        quotechar (str, optional): Flat file quote character. Defaults to '"'.
        encoding (str, optional): Flat file encoding. Defaults to None.
        conn (pg.extensions.connection, optional): Redshift connection (psycopg2).
        default_group (str, optional): Default group/role for readonly table access. Defaults to 'reporting_role'.
        file_df (pd.core.frame.DataFrame): Pandas dataframe with column naming using "_" only
        column (List[str]): Optional list of column names
    """

    def __init__(self,
                 schema: str,
                 table: str,
                 file: str = None,
                 export_field_name: str = None,
                 export_field_type: str = None,
                 primary_key: str = None,
                 dist_key: str = None,
                 sort_key: str = None,
                 delimiter: str = '|',
                 quotechar: str = '"',
                 encoding: str = None,
                 conn: pg.extensions.connection = None,
                 default_group: str = 'dbreader',
                 file_df: pd.core.frame.DataFrame = None,
                 columns: List[str] = None) -> None:
        assert file or not file_df.empty
        self.file = file
        self.schema = schema
        self.table = table
        self.export_field_name = export_field_name
        self.export_field_type = export_field_type
        self.primary_key = primary_key
        self.dist_key = dist_key
        self.sort_key = sort_key
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.conn = conn
        self.default_group = default_group
        self.metadata = None
        self.columns = columns
        self.diff = None
        self.file_df = file_df

    def get_column_list(self) -> list:
        """Returns column list based on header of file.
        """
        if self.columns is None:
            if self.file_df is None:
                self._load_file(self.file, True)

            self.columns = [col for col in self.file_df.columns]

        return self.columns

    def check_schema_existence(self) -> bool:
        """Checks Redshift for the existence of a schema.

        Returns:
            bool: True if the schema exists, False otherwise

        Raises:
            Exception: This function requires a Redshift connection be passed into class conn parameter.
        """
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_namespace WHERE nspname = '{self.schema}';")
                return True if cur.rowcount != 0 else False
        else:
            raise Exception("Conn must be set to a valid Redshift connection.")

    def check_table_existence(self) -> bool:
        """Checks Redshift for the existence of a table.

        Returns:
            bool: True if the table exists, False otherwise

        Raises:
            Exception: This function requires a Redshift connection be passed into class conn parameter.
        """
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_tables WHERE schemaname = '{self.schema}' AND tablename = '{self.table}' UNION SELECT 1 FROM pg_views WHERE schemaname = '{self.schema}' AND viewname = '{self.table}' LIMIT 1;")
                return True if cur.rowcount != 0 else False
        else:
            raise Exception("Conn must be set to a valid Redshift connection.")

    def generate_schema_ddl(self) -> str:
        """Returns a SQL statement that creates a Redshift schema.

        Returns:
            str: Schema DDL
        """
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema};"

    def generate_schema_permissions(self) -> str:
        """Returns a SQL statement that grants schema usage to the default group.

        Returns:
            str: Schema permissions DDL
        """
        return f"GRANT USAGE ON SCHEMA {self.schema} TO GROUP {self.default_group};"

    def generate_table_ddl(self) -> str:
        """Returns a SQL statement that creates a Redshift table.

        Returns:
            str: Table DDL
        """
        if self.metadata is None:
            self._generate_table_metadata()
            if self.metadata is None:
                return None

        metadata = self.metadata.copy()
        metadata.loc[metadata.proposed_type == 'notype', 'proposed_type'] = 'varchar(256)'
        metadata['index'][0] = '"' + str(metadata['index'][0]) + '"'
        metadata['index'][1:] = ', "' + metadata['index'][1:].astype(str) + '"'
        columns = re.sub(' +', ' ', metadata[['index', 'proposed_type']].to_string(header=False, index=False))
        ddl = f"CREATE TABLE {self.schema}.{self.table} (\n{columns}\n"

        if self.export_field_name and self.export_field_type:
            ddl += f" , {self.export_field_name} {self.export_field_type}\n"

        if self.primary_key:
            ddl += f" , PRIMARY KEY ({self.primary_key})\n"

        ddl += ')\n'

        if self.dist_key:
            ddl += f"DISTKEY ({self.dist_key})\n"
        else:
            ddl += f"DISTSTYLE EVEN\n"

        if self.sort_key:
            ddl += f"SORTKEY ({self.sort_key})\n"

        return ddl

    def generate_column_ddl(self) -> str:
        """Returns a SQL statement that adds missing column(s) to a table.

        Returns:
            str: Column DDL
        """
        if self.diff is None:
            self.evaluate_table_ddl_diffs()

        if 'reason' not in self.diff.columns:
            return None
        elif not self.diff[self.diff.reason == 'MISSING'].empty:
            missing = self.diff[['field', 'proposed_type']][self.diff.reason == 'MISSING']
            return re.sub(' +', ' ', (f'ALTER TABLE {self.schema}.{self.table} ADD COLUMN ' + missing['field'] + ' ' + missing['proposed_type'] + ';').to_string(header=False, index=False))
        else:
            return None

    def generate_table_permissions(self) -> str:
        """Returns a SQL statement that grants table read access to the default group.

        Returns:
            str: Table permissions DDL
        """
        return f"GRANT SELECT ON {self.schema}.{self.table} TO GROUP {self.default_group};"

    def evaluate_table_ddl_diffs(self) -> pd.core.frame.DataFrame:
        """If table exists in Redshift, returns a dataframe containing differences between proposed and existing table DDL. Return an empty dataframe if there are no diffs.

        Returns:
            pd.core.frame.DataFrame: Dataframe with diffs (empty dataframe with no diffs)
        """
        if self.conn is None:
            raise Exception("Conn must be set to a valid Redshift connection.")

        if self.metadata is None:
            self._generate_table_metadata()

        proposed_df = self.metadata.copy()
        deployed_df = pd.read_sql(f"""SELECT "column_name" AS index, "udt_name" || CASE WHEN character_maximum_length IS NOT NULL THEN '(' || CAST(character_maximum_length AS VARCHAR) || ')' ELSE '' END AS deployed_type
                                      FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = '{self.table}' ORDER BY ordinal_position;""", con=self.conn)
        combined_df = pd.merge(proposed_df, deployed_df, how='outer', on='index')
        combined_df['reason'] = combined_df.apply(lambda x: 'TYPE MISMATCH' if (self._classify_type(x['proposed_type']) != self._classify_type(x['deployed_type'])) else np.NaN, axis=1)
        combined_df.loc[combined_df.proposed_type.notnull() & combined_df.deployed_type.isnull(), 'reason'] = 'MISSING'
        combined_df.loc[combined_df.proposed_type.isnull() & combined_df.deployed_type.notnull(), 'reason'] = 'DEPRECATED'
        combined_df.rename(columns={'index': 'field'}, inplace=True)
        combined_df = combined_df[combined_df.proposed_type != 'notype']
        combined_df = combined_df[['field', 'proposed_type', 'deployed_type', 'reason']].copy()
        self.diff = combined_df[combined_df['reason'].notnull()]
        return self.diff

    def _load_file(self, path: str, low_memory: bool = False) -> None:
        if 'parquet' in self.file.lower():
            self.file_df = pd.read_parquet(self.file)
        else:
            self.file_df = pd.read_csv(self.file, sep=self.delimiter, quotechar=self.quotechar, encoding=self.encoding, low_memory=low_memory)

        self.file_df.columns = self.file_df.columns.str.replace(".", "_")

    def _generate_table_metadata(self) -> None:
        """Generates metadata based on contents of file.
        """
        pd.set_option("display.max_colwidth", 10000)

        if self.file_df is None:
            self._load_file(self.file, False)

        if self.file_df.empty:
            self.metadata = None
            return

        if self.columns is None:
            self.columns = [col for col in self.file_df.columns]
        else:
            self.file_df.columns = self.columns

        metadata = self.file_df.dtypes.to_frame('pandas_type')
        metadata.reset_index(level=0, inplace=True)
        metadata['proposed_type'] = ''
        metadata['proposed_type'] = metadata.apply(lambda col: self._evaluate_type(col, identifier=True) if str(col[0]).endswith('_id') else self._evaluate_type(col), axis=1)
        self.metadata = metadata

    def _classify_type(self, datatype: str) -> int:
        """Classifies data types and their aliases for the purposes of comparison.

        Returns:
            int: Value for the data type set.
        """
        datatype = str(datatype).lower().strip()
        if datatype in ('smallint', 'int2'):
            return 1
        elif datatype in ('integer', 'int', 'int4'):
            return 2
        elif datatype in ('bigint', 'int8'):
            return 3
        elif datatype in ('decimal', 'numeric'):
            return 4
        elif datatype in ('real', 'float'):
            return 5
        elif datatype in ('double precision', 'float8', 'float'):
            return 6
        elif datatype in ('boolean', 'bool'):
            return 7
        elif datatype in ('char', 'character', 'nchar', 'bpchar'):
            return 8
        elif datatype in ('varchar', 'varchar(256)', 'character varying', 'character varying(256)', 'nvarchar', 'nvarchar(256)', 'text'):
            return 9
        elif datatype in ('varchar(65535)', 'character varying(65535)', 'nvarchar(65535)'):
            return 10
        elif datatype in ('date'):
            return 11
        elif datatype in ('timestamp', 'timestamp without time zone'):
            return 12
        elif datatype in ('timestamptz', 'timestamp with time zone'):
            return 13
        else:
            return 0

    def _evaluate_type(self, metadata: pd.core.series.Series, identifier: bool = False) -> str:
        """Takes table column metadata as input and infers a Redshift data type from the data.

        Args:
            metadata (pd.core.series.Series): Core

        Returns:
            str: Redshift data type
        """
        name = str(metadata[0])
        column = self.file_df[name]

        if column.isnull().all():
            return 'notype'
        else:
            column = column[column.notnull()]

            if all(str(x).lower() in ["true", "false", "t", "f", "0", "1"] for x in column.unique()) and identifier is False:
                return 'bool'
            else:
                try:
                    column.astype(float)
                    try:
                        if any(column.astype(str).str.contains('.')):
                            return 'float8'
                        else:
                            if column.max() <= 2147483647 and column.min() >= -2147483648:
                                return 'int4'
                            else:
                                return 'int8'
                    except TypeError:
                        return 'float8'
                except (TypeError, ValueError, OverflowError):
                    try:
                        date_parse = pd.to_datetime(column, infer_datetime_format=True)
                        if not all(parser.parse(str(x), default=datetime(1900, 1, 1)) == parser.parse(str(x)) for x in column.unique()):
                            return 'varchar(256)'
                        elif all(date_parse == date_parse.dt.normalize()):
                            return 'date'
                        else:
                            return 'timestamp'
                    except (TypeError, ValueError, OverflowError):
                        if column.astype(str).map(len).max() <= 240:
                            return 'varchar(256)'
                        else:
                            return 'varchar(65535)'
