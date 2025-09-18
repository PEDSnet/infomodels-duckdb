from __future__ import annotations
import os
from typing import Tuple, List, Set
from src.dq_checks.check_result import CheckResult
from src.config import LOGGER
from src.util import OPTIONAL_TABLES


def check_missing_submission_file(
        file_dir: str, 
        cdm_tables_expected: Tuple[str, ...],
        file_extension: str = '.csv'
    ) -> CheckResult:
    """
    Check if the directory misses any expected CSV files for each CDM table.

    Parameters:
        file_dir (str): Path to the directory containing submission files.
        cdm_tables_expected (Tuple[str, ...]): Tuple of expected CDM table names (without file extension).
        file_extension (str): extension of file. Default to .csv

    Returns:
        CheckResult
    """
    LOGGER.info("Running DQ check: check_missing_submission_file. "
                "Params: file_dir={file_dir}, cdm_tables_expected={cdm_tables_expected}, file_extension={file_extension}")
    check_type = 'missing_submission_file'
    table_names_from_files = [os.path.splitext(f)[0] for f in os.listdir(file_dir) if f.endswith(file_extension)]
    missing_tables = set(cdm_tables_expected) - set(table_names_from_files) - set(OPTIONAL_TABLES)
    if len(missing_tables) > 0:
        result = CheckResult(
            check_type = check_type,
            status = 'FAIL',
            table_name = tuple(missing_tables),
            troubleshooting_message = f'Cannot find submission file(s) for above table(s) in dir: {file_dir}'
        )
    else:
        result = CheckResult(
            check_type = check_type,
            status = 'PASS'
        )
    result.log(LOGGER)
    return(result)

def check_extra_submission_file(
        file_dir: str, 
        cdm_tables_expected: Tuple[str, ...],
        file_extension: str = '.csv'
    ) -> CheckResult:
    """
    Check if the directory contains extra CSV files for each CDM table.

    Parameters:
        file_dir (str): Path to the directory containing submission files.
        cdm_tables_expected (Tuple[str, ...]): Tuple of expected CDM table names (without file extension).
        file_extension (str): extension of file. Default to .csv

    Returns:
        CheckResult
    """
    LOGGER.info("Running DQ check: check_extra_submission_file. "
                f"Params: file_dir={file_dir}, cdm_tables_expected={cdm_tables_expected}, file_extension={file_extension}")
    check_type = 'extra_submission_file'
    filenames_on_dir = [f for f in os.listdir(file_dir) if f.endswith(file_extension)]
    filenames_from_tables = [ t+file_extension for t in cdm_tables_expected]
    extra_files = set(filenames_on_dir) - set(filenames_from_tables)
    if len(extra_files) > 0:
        result = CheckResult(
            check_type = check_type,
            status = 'WARN',
            file_name = tuple(extra_files),
            troubleshooting_message = "Extra file(s) in directory. These files will not be loaded and submitted. Pleased make sure there's no config issue."
        )
    else:
        result = CheckResult(
            check_type = check_type,
            status = 'PASS'
        )
    result.log(LOGGER)
    return(result)
