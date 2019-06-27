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


class RedshiftAutoSchema():

    """RedshiftAutoSchema takes a delimited flat file or parquet file as input and infers the appropriate Redshift data type for each column.
    This class provides functions that allow for the automatic generation and validation of table schemas (and basic permissioning) in Redshift.

    Attributes:
        file (str): Path to delimited flat file or parquet file.
        schema (str): Schema of the new Redshift table.
        table (str): Name of the new Redshift table.
        export_date_field (bool, optional): Flag indicating whether export_date column should be added to new table.
        dist_key (str, optional): Name of column that should be the distribution key. If no column is specified, it will default to DISTSTYLE EVEN.
        sort_key (str, optional): Name of columns that should be the sort key (separated by commas).
        delimiter (str, optional): Flat file delimiter. Defaults to '|'.
        quotechar (str, optional): Flat file quote character. Defaults to '"'.
        encoding (str, optional): Flat file encoding. Defaults to None.
        conn (pg.extensions.connection, optional): Redshift connection (psycopg2).
        default_group (str, optional): Default group/role for readonly table access. Defaults to 'reporting_role'.
    """

    def __init__(self,
                 file: str,
                 schema: str,
                 table: str,
                 export_date_field: bool = False,
                 dist_key: str = None,
                 sort_key: str = None,
                 delimiter: str = '|',
                 quotechar: str = '"',
                 encoding: str = None,
                 conn: pg.extensions.connection = None,
                 default_group: str = 'reporting_role') -> None:
        self.file = file
        self.schema = schema
        self.table = table
        self.export_date_field = export_date_field
        self.dist_key = dist_key
        self.sort_key = sort_key
        self.delimiter = delimiter
        self.quotechar = quotechar
        self.encoding = encoding
        self.conn = conn
        self.default_group = default_group
        self.metadata = None

    def check_schema_existence(self) -> bool:
        """Checks Redshift for the existence of a schema.

        Returns:
            bool: True if the schema exists, False otherwise

        Raises:
            Exception: This function requires a Redshift connection be passed into class conn parameter.
        """
        if self.conn:
            with self.conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_tables WHERE schemaname = '{self.schema}' LIMIT 1;")
                return True if cur.fetchall() else False
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
                return True if cur.fetchall() else False
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
            self._generate_table_metadata_from_file()

        metadata = self.metadata.copy()
        metadata.loc[metadata.proposed_type == 'NULL FIELD', 'proposed_type'] = 'CHARACTER VARYING(256)'
        metadata['index'][1:] = ', ' + metadata['index'][1:].astype(str)
        columns = re.sub(' +', ' ', metadata[['index', 'proposed_type']].to_string(header=False, index=False))
        export_date = f" , export_date DATE DEFAULT GETDATE()\n" if self.export_date_field else ""

        ddl = f"CREATE TABLE {self.schema}.{self.table} (\n{columns}\n{export_date})\n"

        if self.dist_key:
            ddl += f"DISTKEY ({self.dist_key})\n"
        else:
            ddl += f"DISTSTYLE EVEN\n"

        if self.sort_key:
            ddl += f"SORTKEY ({self.sort_key})\n"

        return ddl

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
            self._generate_table_metadata_from_file()

        proposed_df = self.metadata.copy()
        deployed_df = pd.read_sql(f"""SELECT "column_name" AS index, "data_type" || CASE WHEN character_maximum_length IS NOT NULL THEN '(' || CAST(character_maximum_length AS VARCHAR) || ')' ELSE '' END AS deployed_type
                                      FROM information_schema.columns WHERE table_schema = '{self.schema}' AND table_name = '{self.table}' ORDER BY ordinal_position;""", con=self.conn)
        combined_df = pd.merge(proposed_df, deployed_df, how='outer', on='index')
        combined_df['reason'] = combined_df.apply(lambda x: 'DATA TYPE MISMATCH' if (self._classify_type(x['proposed_type']) != self._classify_type(x['deployed_type'])) else np.NaN, axis=1)
        combined_df.loc[combined_df.proposed_type.notnull() & combined_df.deployed_type.isnull(), 'reason'] = 'FIELD IN FILE, NOT IN REDSHIFT'
        combined_df.loc[combined_df.proposed_type.isnull() & combined_df.deployed_type.notnull(), 'reason'] = 'FIELD IN REDSHIFT, NOT IN FILE'
        combined_df.rename(columns={'index': 'field'}, inplace=True)
        combined_df = combined_df[combined_df.proposed_type != 'NULL FIELD']
        combined_df = combined_df[['field', 'proposed_type', 'deployed_type', 'reason']].copy()
        return combined_df[combined_df['reason'].notnull()]

    def _generate_table_metadata_from_file(self) -> None:
        """Generates metadata based on contents of file.
        """
        if 'parquet' in self.file.lower():
            self.file_df = pd.read_parquet(self.file)
        else:
            self.file_df = pd.read_csv(self.file, sep=self.delimiter, quotechar=self.quotechar, encoding=self.encoding, low_memory=False)

        metadata = self.file_df.dtypes.to_frame('pandas_type')
        metadata.reset_index(level=0, inplace=True)
        metadata['proposed_type'] = ''
        metadata['proposed_type'] = metadata.apply(lambda row: self._evaluate_type(row), axis=1)
        self.metadata = metadata

    def _classify_type(self, datatype: str) -> int:
        """Classifies data types and their aliases for the purposes of comparison.

        Returns:
            int: Value for the data type set.
        """
        datatype = str(datatype).upper().strip()
        if datatype in ('SMALLINT', 'INT2'):
            return 1
        elif datatype in ('INTEGER', 'INT', 'INT4'):
            return 2
        elif datatype in ('BIGINT', 'INT8'):
            return 3
        elif datatype in ('DECIMAL', 'NUMERIC'):
            return 4
        elif datatype in ('REAL', 'FLOAT'):
            return 5
        elif datatype in ('DOUBLE PRECISION', 'FLOAT8', 'FLOAT'):
            return 6
        elif datatype in ('BOOLEAN', 'BOOL'):
            return 7
        elif datatype in ('CHAR', 'CHARACTER', 'NCHAR', 'BPCHAR'):
            return 8
        elif datatype in ('VARCHAR', 'VARCHAR(256)', 'CHARACTER VARYING', 'CHARACTER VARYING(256)', 'NVARCHAR', 'NVARCHAR(256)', 'TEXT'):
            return 9
        elif datatype in ('VARCHAR(65535)', 'CHARACTER VARYING(65535)', 'NVARCHAR(65535)'):
            return 10
        elif datatype in ('DATE'):
            return 11
        elif datatype in ('TIMESTAMP', 'TIMESTAMP WITHOUT TIME ZONE'):
            return 12
        elif datatype in ('TIMESTAMPTZ', 'TIMESTAMP WITH TIME ZONE'):
            return 13
        else:
            return 0

    def _evaluate_type(self, metadata: pd.core.series.Series) -> str:
        """Takes table column metadata as input and infers a Redshift data type from the data.

        Args:
            metadata (pd.core.series.Series): Core

        Returns:
            str: Redshift data type
        """
        name = str(metadata[0])

        try:
            if not self.file_df[name].isnull().all():
                try:
                    self.file_df[name].astype(float)
                    try:
                        if np.array_equal(self.file_df[name].notnull().astype(float), self.file_df[name].notnull().astype(int)):
                            if all(value in [0, 1] for value in self.file_df[name].unique()):
                                return 'BOOLEAN'
                            elif self.file_df[name].max() <= 2147483647 and self.file_df[name].min() >= -2147483648:
                                return 'INTEGER'
                            else:
                                return 'BIGINT'
                        else:
                            return 'DOUBLE PRECISION'
                    except TypeError:
                        return 'DOUBLE PRECISION'
                except (ValueError, OverflowError):
                    if all(str(value).lower() in ["true", "false", "t", "f", "0", "1"] for value in self.file_df[name].unique()):
                        return 'BOOLEAN'
                    else:
                        try:
                            values = pd.to_datetime(self.file_df[name], infer_datetime_format=True)
                            if (values == values.dt.normalize()):
                                return 'DATE'
                            else:
                                return 'TIMESTAMP WITHOUT TIME ZONE'
                        except (ValueError, OverflowError):
                            if self.file_df[name].astype(str).map(len).max() <= 256:
                                return 'CHARACTER VARYING(256)'
                            else:
                                return 'CHARACTER VARYING(65535)'
            else:
                return 'NULL FIELD'
        except KeyError:
            return 'NULL FIELD'
