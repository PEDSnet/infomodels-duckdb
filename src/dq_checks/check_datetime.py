from src.dq_checks.check_result import CheckResult
from src.util import table_exists, column_exists, get_threshold
from src.config import LOGGER
from duckdb import DuckDBPyConnection
from typing import Optional


def check_datetime_concordance(
    con: DuckDBPyConnection,
    table_name: str,
    date_column: str,
    datetime_column: str,
    threshold: Optional[dict[str, float]] = None,
) -> CheckResult:
    """
    Verify that DATE and DATETIME columns are concordant.

    Example:
        birth_date == CAST(birth_datetime AS DATE)

    A violation occurs when both columns contain values but the date portion of the datetime does not match the date column.

    Parameters:
        - con: DuckDBPyConnection, a duckdb connection.
        - table_name: str, the name of the table to check.
        - date_column: str, the date column in the table that should be checked against the datetime
        - datetime_column: str, the datetime column in the table that should be checked against the date
    
    Returns:
        - CheckResult: Result of the DATETIME check.
    """

    if threshold is None:
        threshold = get_threshold("datetime_concordance", table_name=table_name, column_name=date_column,)

    # check table exists
    if not table_exists(con, table_name):
        result = CheckResult(
            check_type="datetime_concordance",
            table_name=table_name,
            column_name=f"{date_column}|{datetime_column}",
            status="SKIPPED",
            troubleshooting_message=f"Table {table_name} does not exist in the database.",
        )
        result.log(LOGGER, duckdb_conn=con)
        return result

    # check columns exist
    for col in [date_column, datetime_column]:
        if not column_exists(con, table_name, col):
            result = CheckResult(
                check_type="datetime_concordance",
                table_name=table_name,
                column_name=col,
                status="SKIPPED",
                troubleshooting_message=f'Column "{col}" does not exist in table "{table_name}".',
            )
            result.log(LOGGER, duckdb_conn=con)
            return result

    check_query = f"""
        SELECT
            COUNT(*) AS total_count,
            COUNT(*) FILTER (
                WHERE "{date_column}" IS NOT NULL
                  AND "{datetime_column}" IS NOT NULL
                  AND "{date_column}" != CAST("{datetime_column}" AS DATE)
            ) AS violation_count,
            MIN("{datetime_column}") AS min_datetime,
            MAX("{datetime_column}") AS max_datetime
        FROM "{table_name}";
    """

    sample_query = f"""
        SELECT
            "{date_column}",
            "{datetime_column}"
        FROM "{table_name}"
        WHERE "{date_column}" IS NOT NULL
          AND "{datetime_column}" IS NOT NULL
          AND "{date_column}" != CAST("{datetime_column}" AS DATE)
        LIMIT 10;
    """

    LOGGER.debug(
        f"Executing datetime concordance check query: {check_query}"
    )

    total_count, violation_count, min_dt, max_dt = (
        con.execute(check_query).fetchone()
    )

    if violation_count > 0:
        LOGGER.debug(
            f"Executing sample query for datetime concordance violations: {sample_query}"
        )

        sample_rows = con.execute(sample_query).fetchall()

        sample_str = "; ".join(
            [f"{r[0]} != {r[1]}" for r in sample_rows]
        )

        violation_pct = (
            violation_count / total_count if total_count > 0 else 0
        )

        result = CheckResult(
            check_type="datetime_concordance",
            status=None,  # inferred by threshold
            table_name=table_name,
            column_name=f"{date_column}|{datetime_column}",
            violation_count=violation_count,
            violation_pct=violation_pct,
            threshold=threshold,
            troubleshooting_message=(
                f'Column "{date_column}" does not match the date portion of "{datetime_column}" for {violation_count} rows out of {total_count} ({violation_pct:.2%}). '
                f"Min datetime: {min_dt}. "
                f"Max datetime: {max_dt}. "
                f"Sample mismatches: {sample_str}"
            ),
        )
    else:
        result = CheckResult(
            check_type="datetime_concordance",
            status="PASS",
            table_name=table_name,
            column_name=f"{date_column}|{datetime_column}",
        )

    result.log(LOGGER, duckdb_conn=con)
    return result