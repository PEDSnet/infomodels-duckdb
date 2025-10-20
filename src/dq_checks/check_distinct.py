from src.dq_checks.check_result import CheckResult
from src.util import get_table_count, table_exists, column_exists
from src.config import LOGGER
from duckdb import DuckDBPyConnection
from typing import Optional
from src.util import get_threshold

def check_distinct_violation(
    con: DuckDBPyConnection,
    table_name: str,
    column_names: tuple[str] | str,
    threshold: Optional[dict[str, float] ]= None
) -> CheckResult:
    """
    Check for DISTINCT constraint violations in the specified table and column.

    Parameters:
    - con: DuckDBPyConnection, a duckdb connection.
    - table_name: str, the name of the table to check.
    - column_name: str, the column in the table that should contain distinct values.

    Returns:
    - CheckResult: Result of the DISTINCT check.
    """
    if isinstance(column_names, list):
        column_names = tuple(column_names, )
    if threshold is None:
        threshold = get_threshold('distinct_violation', table_name=table_name, column_name=column_names[0])
    # check if table and column exist
    if not table_exists(con, table_name):
        result = CheckResult(
            check_type='distinct_violation',
            table_name=table_name,
            column_name=column_names,
            status='SKIPPED',
            troubleshooting_message=f'Table {table_name} does not exist in the database.'
        )
        result.log(LOGGER, duckdb_conn=con)
        return result
    for column_name in column_names:
        if not column_exists(con, table_name, column_name):
            result = CheckResult(
                check_type='distinct_violation',
                table_name=table_name,
                column_name=column_name,
                status='SKIPPED',
                troubleshooting_message=f'Column {column_name} does not exist in table {table_name}.'
            )
            result.log(LOGGER, duckdb_conn=con)
            return result
    
    # check for DISTINCT violations
    check_query = f"""
        SELECT COUNT(*) AS total_count,
               COUNT(DISTINCT ( {', '.join(['"' + col + '"' for col in column_names])}) ) AS distinct_count
        FROM "{table_name}";
    """
    sample_query = f"""
        SELECT "{column_name}" AS value_count
        GROUP BY {', '.join(['"' + col + '"' for col in column_names])}
        FROM "{table_name}"
        HAVING COUNT(*) > 1
        LIMIT 10;
    """
    
    LOGGER.debug(f"Executing DISTINCT check query: {check_query}")
    total_count, distinct_count = con.execute(check_query).fetchone()
    violation_count = total_count - distinct_count
    if violation_count > 0:
        LOGGER.debug(f"Executing sample query for DISTINCT violations: {sample_query}")
        con.execute(sample_query)
        sample_violations_str = ', '.join([str(row[0]) for row in con.fetchall()])
        violation_pct = 1.0 * violation_count / total_count
        result = CheckResult(
            check_type='distinct_violation',
            status=None,  # Let CheckResult infer the status based on threshold and violation_pct
            table_name=table_name,
            column_name=column_name,
            violation_count=violation_count,
            violation_pct=violation_pct,
            threshold=threshold,
            troubleshooting_message = f'The column "{column_name}" in table "{table_name}" has {violation_count} non-distinct values out of {total_count} total values ({violation_pct:.2%}). Sample non-distinct values: {sample_violations_str}. Please ensure this column contains only distinct values.',
        )
    else:
        result = CheckResult(
            check_type='distinct_violation',
            status='PASS',
            table_name=table_name,
            column_name=column_name,
        )
    result.log(LOGGER, duckdb_conn=con)
    return result