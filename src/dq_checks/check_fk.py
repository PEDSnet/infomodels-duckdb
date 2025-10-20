
from src.dq_checks.check_result import CheckResult
from src.util import get_table_count, table_exists, column_exists, get_threshold
from src.config import LOGGER
from duckdb import DuckDBPyConnection
from typing import Optional

def check_fk_violation(
    con: DuckDBPyConnection,
    main_table: str,
    main_column: str,
    reference_table: str,
    reference_column: str,
    threshold: Optional[dict[str, float] ]= None
) -> CheckResult:
    """
    Check for foreign key violations in the specified tables and columns.

    Parameters:
    - con: DuckDBPyConnection, a duckdb connection.
    - main_table: str, the name of the main table to check.
    - main_column: str, the column in the main table that should reference the reference table.
    - reference_table: str, the name of the reference table.
    - reference_column: str, the column in the reference table that is referenced by the main column.

    Returns:
    - dict: A dictionary with keys 'status' and 'message'.
    """
    if threshold is None:
        threshold = get_threshold('foreign_key_violation', table_name=main_table, column_name=main_column)
    # check if main_column in main_table exists, reference_column in reference_table exists
    if not table_exists(con, main_table):
        result = CheckResult(
            check_type='foreign_key_violation',
            table_name=main_table,
            column_name=main_column,
            status='SKIPPED',
            troubleshooting_message=f'Main table {main_table} does not exist in the database.',
            reference_table=reference_table,
            reference_column=reference_column
        )
    elif not table_exists(con, reference_table):
        result = CheckResult(
            check_type='foreign_key_violation',
            table_name=main_table,
            column_name=main_column,
            status='SKIPPED',
            troubleshooting_message=f'Reference table {reference_table} does not exist in the database.',
            reference_table=reference_table,
            reference_column=reference_column
        )
    elif not column_exists(con, main_table, main_column):
        result = CheckResult(
            check_type='foreign_key_violation',
            table_name=main_table,
            column_name=main_column,
            status='SKIPPED',
            troubleshooting_message=f'Main column {main_column} does not exist in the main table {main_table}.',
            reference_table=reference_table,
            reference_column=reference_column
        )
    elif not column_exists(con, reference_table, reference_column):
        result = CheckResult(
            check_type='foreign_key_violation',
            table_name=main_table,
            column_name=main_column,
            status='SKIPPED',
            troubleshooting_message=f'Reference column {reference_column} does not exist in the reference table {reference_table}.',
            reference_table=reference_table,
            reference_column=reference_column
        )
    else:
        # Check for foreign key violations
        check_query = f"""
            SELECT COUNT(*)
            FROM "{main_table}" AS m
            LEFT JOIN "{reference_table}" AS r
                ON m."{main_column}" = r."{reference_column}" 
            WHERE  
                r."{reference_column}" IS NULL AND
                m."{main_column}" IS NOT NULL;
        """
        sample_query = f"""
            SELECT DISTINCT m."{main_column}"
            FROM "{main_table}" AS m
            LEFT JOIN "{reference_table}" AS r
                ON m."{main_column}" = r."{reference_column}" 
            WHERE  
                r."{reference_column}" IS NULL AND
                m."{main_column}" IS NOT NULL
            LIMIT 5;
            """
        LOGGER.debug(f"Executing foreign key check query: {check_query}")
        violation_count = con.execute(check_query).fetchone()[0]    
        if violation_count > 0:
            LOGGER.debug(f"Executing sample query for foreign key violations: {sample_query}")
            con.execute(sample_query)
            sample_violations_str = ', '.join([str(row[0]) for row in con.fetchall()])

            total_count = get_table_count(con, main_table)
            result = CheckResult(
                check_type='foreign_key_violation',
                table_name=main_table,
                column_name=main_column,
                violation_pct= 1.0 * violation_count / total_count,
                threshold = threshold,
                troubleshooting_message=f'Found {violation_count} foreign key violations in {main_table}.{main_column} referencing {reference_table}.{reference_column}. Total rows in {main_table}: {total_count}.\nSample violating values: {sample_violations_str}',
                reference_table=reference_table,
                reference_column=reference_column
            )
        else:
            result = CheckResult(
                check_type='foreign_key_violation',
                table_name=main_table,
                column_name=main_column,
                status='PASS',
                reference_table=reference_table,
                reference_column=reference_column
            )
            
    result.log(LOGGER, duckdb_conn=con)
    return result
