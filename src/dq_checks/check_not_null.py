from src.dq_checks.check_result import CheckResult
from src.config import LOGGER
from duckdb import DuckDBPyConnection
from typing import Optional
from src.util import get_table_count, table_exists, column_exists

def check_not_null_violation(
    con: DuckDBPyConnection,
    table_name: str,
    column_name: str,
    threshold: Optional[dict[str, float] ]= {'PASS': 0.0}
) -> CheckResult:
    """
    Check for NOT NULL constraint violations in the specified table and column.

    Parameters:
    - con: DuckDBPyConnection, a duckdb connection.
    - table_name: str, the name of the table to check.
    - column_name: str, the column in the table that should not contain NULL values.

    Returns:
    - CheckResult: Result of the NOT NULL check.
    """
    # check if table and column exist
    if not table_exists(con, table_name):
        result = CheckResult(
            check_type='not_null_violation',
            table_name=table_name,
            column_name=column_name,
            status='SKIPPED',
            troubleshooting_message=f'Table {table_name} does not exist in the database.'
        )
        result.log(LOGGER)
        return result
    elif not column_exists(con, table_name, column_name):
        result = CheckResult(
            check_type='not_null_violation',
            table_name=table_name,
            column_name=column_name,
            status='SKIPPED',
            troubleshooting_message=f'Column {column_name} does not exist in table {table_name}.'
        )
        result.log(LOGGER)
        return result
    
    # check for NOT NULL violations
    check_query = f"""
        SELECT COUNT(*)
        FROM "{table_name}"
        WHERE "{column_name}" IS NULL;
    """
    
    LOGGER.debug(f"Executing NOT NULL check query: {check_query}")
    violation_count = con.execute(check_query).fetchone()[0]
    if violation_count > 0:
        total_count = get_table_count(con, table_name)
        violation_pct = violation_count / total_count
        result = CheckResult(
            check_type='not_null_violation',
            status=None,  # Let CheckResult infer the status based on threshold and violation_pct
            table_name=table_name,
            column_name=column_name,
            violation_pct=violation_pct,
            threshold=threshold,
            troubleshooting_message = f'The column "{column_name}" in table "{table_name}" has {violation_count} NULL values out of {total_count} rows ({violation_pct:.2%}). Please ensure this column does not contain NULL values.'
        )
    else:
        result = CheckResult(
            check_type='not_null_violation',
            status='PASS',
            table_name=table_name,
            column_name=column_name
        )
    result.log(LOGGER)
    return result
