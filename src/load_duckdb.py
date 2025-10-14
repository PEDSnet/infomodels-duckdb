from typing import List
from src.util import get_csv_header, get_table_count
from src.config import CONFIG, LOGGER
from duckdb import DuckDBPyConnection
import duckdb
from src.data_model import DataModel

def init_duckdb_logging_schema(con: DuckDBPyConnection, run_id: str, run_config: dict, logging_schema = 'logging') -> DuckDBPyConnection:
    con.execute(f"""
    CREATE SCHEMA IF NOT EXISTS {logging_schema};
    CREATE TABLE IF NOT EXISTS {logging_schema}.dq (
        run_id VARCHAR,
        log_time TIMESTAMP,
        check_type VARCHAR,
        status VARCHAR,
        file_name VARCHAR,
        table_name VARCHAR,
        column_name VARCHAR,
        violation_pct FLOAT,
        threshold VARCHAR,
        message VARCHAR,
        extra_info VARCHAR
    );
    CREATE TABLE IF NOT EXISTS {logging_schema}.process (
        run_id VARCHAR,
        process_name VARCHAR,
        status VARCHAR,
        start_time TIMESTAMP,
        end_time TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS {logging_schema}.run (
        run_id VARCHAR,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        config STRING
    );
    """)
    # insert a new run
    con.execute(f"""INSERT INTO {logging_schema}.run (run_id, start_time, config) VALUES ('{run_id}', current_localtimestamp(), ?);""", (str(run_config),))
    return con

def create_duckdb_tables(data_model: DataModel, con: DuckDBPyConnection, skip_tables: List = [], recreate: bool = False):
    ddl_dict = data_model.to_duckdb_ddl()
    tables = set(ddl_dict.keys()) - set(skip_tables)
    sql = ''
    for t in tables:
        if recreate:
            sql += f'DROP TABLE IF EXISTS {t};\n'
        sql += ddl_dict[t] + ';\n' 
    con.execute(sql)
    LOGGER.info(f"empty table(s) created -- {tables}")
    return con


def load_csv_to_duckdb(csv_path: str, con: DuckDBPyConnection, table_name: str, accept_additional_col: bool = True):
    """
    Loads a CSV file into a DuckDB table. Any additional column in csv will be added to database

    Parameters:
    - csv_path: str, path to the CSV file.
    - con: DuckDBPyConnection, a duckdb connection
    - table_name: str, the name of the table to create/load into.
    - accept_additional_col: bool, if True, add additional columns in csv to duckdb. If False, will throw an error if addtional col in csv

    Returns:
    - duckdb.Connection object connected to the database.
    """
    csv_header = [item.lower() for item in get_csv_header(csv_path)]
    duckdb_columns = con.execute(f'DESCRIBE {table_name}').df()['column_name'].tolist()
    # if csv has more columns than duckdb
    if (set(csv_header) - set(duckdb_columns)):
        if accept_additional_col:
            for col in set(csv_header) - set(duckdb_columns):
                con.execute(f'ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR;')
                LOGGER.warning(f"column {col} exists in csv, but not in duckdb ddl. Added '{col} VARCHAR' to duckdb. ")
        else:
            raise ValueError(f"CSV file has additional columns {set(csv_header) - set(duckdb_columns)} not in duckdb table {table_name} and accept_additional_col is set to False.")
    count_before_load = get_table_count(con, table_name)
    LOGGER.info(f"Loading {csv_path} to {table_name}...")
    copy_sql = f"""COPY {table_name} ({', '.join(csv_header)}) FROM '{csv_path}' ({CONFIG['duckdb']['copy_options'] + ', AUTO_DETECT false'});"""
    LOGGER.debug(f"Executing SQL: {copy_sql}")
    try:
        con.execute(copy_sql)
    except Exception as e:
        LOGGER.error(f"Fail to load CSV to DuckDB: table={table_name}, csv={csv_path}")
        raise
    count_after_load = get_table_count(con, table_name)
    LOGGER.info(f"Loaded {count_after_load - count_before_load} rows into {table_name}.")
    return con

