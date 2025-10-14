from src.util import get_csv_header
from src.dq_checks.check_result import CheckResult
from collections import Counter
from src.config import LOGGER
from src.data_model import DataModel

def check_duplicated_column_in_csv(file_path: str, table_name: str, duckdb_conn = None) -> CheckResult:
    """
    Check if the CSV file has duplicated columns in its header.

    Parameters:
        file_path (str): Path to the CSV file.
        table_name (str): Name of the CDM table to check against.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header has duplicated columns.
    """
    csv_header = get_csv_header(file_path)
    if len(csv_header) > len(set(csv_header)):
        duplicated_columns = [item for item, count in Counter(csv_header).items() if count > 1]
        result = CheckResult(
            check_type = 'csv header duplication',
            status = 'FAIL',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(duplicated_columns),
            troubleshooting_message = 'The file will not be loaded. Please remove duplicated column(s) in file. '
        )
    else:
        result = CheckResult(
            check_type = 'csv header duplication',
            status = 'PASS',
            file_name=file_path,
            table_name = table_name,
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_extra_column_in_csv(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None) -> CheckResult:
    """
    Check if the CSV file has extra columns that are not defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the CSV file.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header has extra columns.
    """
    csv_header = get_csv_header(file_path)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in csv
    extra_csv_column = set(csv_header) - set(cdm_columns)
    if len(extra_csv_column) > 0:
        result = CheckResult(
            check_type = 'extra column in csv header',
            status = 'WARN',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(extra_csv_column)
        )
    else:
        result = CheckResult(
            check_type = 'extra column in csv header',
            status = 'PASS',
            table_name = table_name
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result

def check_missing_column_in_csv(file_path: str, data_model: DataModel, table_name: str, duckdb_conn = None) -> CheckResult:
    """
    Check if the CSV file has all the required columns defined in the CDM table definition.

    Parameters:
        file_path (str): Path to the CSV file.
        data_model (DataModel): DataModel object containing CDM table definitions.
        table_name (str): Name of the CDM table to check against.

    Returns:
        CheckResult: Result of the check, indicating whether the CSV header is missing any required columns.
    """
    csv_header = get_csv_header(file_path)
    cdm_columns = data_model.all_column_names_in_table(table_name)
    # check extra header in csv
    missing_csv_column = set(cdm_columns) - set(csv_header)
    if len(missing_csv_column) > 0:
        result = CheckResult(
            check_type = 'missing column in csv header',
            status = 'FAIL',
            file_name = file_path,
            table_name = table_name,
            column_name = tuple(missing_csv_column)
        )
    else:
        result = CheckResult(
            check_type = 'missing column in csv header',
            status = 'PASS',
            table_name = table_name,
        )
    result.log(LOGGER, duckdb_conn=duckdb_conn)
    return result