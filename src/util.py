import csv
from typing import List, Dict
from src.config import CONFIG
import os

# Optional CDM tables that are not required for submission
# These tables will still be loaded if present in the submission files,
# but are not included in any checks
OPTIONAL_TABLES = [
    'cohort_definition', 
    'concept', 
    'concept_ancestor', 
    'concept_class', 
    'concept_relationship', 
    'concept_synonym', 
    'condition_era', 
    'domain', 
    'dose_era', 
    'drug_era', 
    'drug_strength', 
    'observation_period', 
    'relationship', 
    'source_to_concept_map', 
    'vocabulary'
]

def get_csv_header(file_path: str, **kwargs) -> List[str]:
    """
    Get header column list from a csv file.

    Args:
        file_path (str): path to csv file.

    Raises:
        ValueError: If files is empty.

    Returns:
        list[str]: A list of strings, each representing a column name. 
    """
    with open(file_path) as csvfile:
        reader = csv.reader(csvfile, kwargs)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"CSV file: {file_path} is empty.")
        else:
            return([x.lower() for x in header])

def get_table_count(
        con, 
        table_name: str, 
        schema: str = None
    ) -> int:
    """
    Get the count of rows in a DuckDB table.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to count rows in.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        int: The number of rows in the specified table.
    """
    if schema:
        sql = f"SELECT COUNT(*) FROM {schema}.{table_name}"
    else:
        sql = f"SELECT COUNT(*) FROM {table_name}"
    count = con.execute(sql).fetchone()[0]
    assert isinstance(count, int), f"Count for table {table_name} is not an integer: {count}"
    return count

def table_exists(con, table_name: str, schema: str = None) -> bool:
    """
    Check if a table exists in the DuckDB database.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to check.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    if schema:
        sql = f"""
            f'SHOW TABLES FROM "{schema}";'
        """
    else:
        sql = f"""
            SHOW TABLES;
        """
    all_tables = [item[0] for item in con.execute(sql).fetchall()]
    return table_name in all_tables

def column_exists(con, table_name: str, column_name: str, schema: str = None) -> bool:
    """
    Check if a column exists in a DuckDB table.

    Args:
        con (DuckDBPyConnection): A DuckDB connection object.
        table_name (str): The name of the table to check.
        column_name (str): The name of the column to check.
        schema (str): The schema where the table is located. Default is None, will use connection schema.

    Returns:
        bool: True if the column exists in the specified table, False otherwise.
    """
    if not table_exists(con, table_name, schema):
        return False
    if schema:
        sql = f"""
           DESCRIBE "{schema}"."{table_name}";
        """
    else:
        sql = f"""
            DESCRIBE "{table_name}";
        """
    all_columns = [item[0] for item in con.execute(sql).fetchall()]
    return column_name in all_columns