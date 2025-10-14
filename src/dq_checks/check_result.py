from typing import Literal, Tuple, List, Optional
import logging

class CheckResult:
    """
    Represents the result of a data quality check.

    Parameters:
        check_type (str): The type of check performed.
        status (Optional[StatusLiteral]): The status of the check ('PASS', 'WARN', or 'FAIL'). If not provided, it will be inferred based on violation_pct and threshold.
        file_name (Optional[str or tuple[str, ...]]): The file name(s) associated with the check.
            Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
        table_name (Optional[str or tuple[str, ...]]): The table name(s) associated with the check.
            Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
        column_name (Optional[str or tuple[str, ...]]): The column name(s) associated with the check.
            Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
        violation_pct (Optional[float]): The percentage of violations found. If not provided, the status must be provided.
        threshold (Optional[dict]): Thresholds for determining check status. Keys are status names ('PASS', 'WARN', 'FAIL') and values are the upper bounds for those statuses.
            If not provided, defaults to {'PASS': 0.0}, meaning any violation will result in a 'FAIL' status.
        troubleshooting_message (Optional[str]): Message to help troubleshoot failures.

    Notes:
        For file_name, table_name, and column_name, you may provide either a string or a tuple of strings.
        All values are stored internally as tuples for consistency.
    """

    dq_fail: List['CheckResult'] = []  # Class variable to store all failed checks
    dq_warn: List['CheckResult'] = []  # Class variable to store all warning checks
    dq_skip: List['CheckResult'] = []  # Class variable to store all skipped checks
    
    success_count: int = 0  # Class variable to count successful checks

    ALLOWED_STATUS: Tuple[str, ...] = ("PASS", "WARN", "FAIL", "SKIPPED")
    StatusLiteral = Literal["PASS", "WARN", "FAIL", "SKIPPED"]

    STATUS_ANSI_COLOR = {
        "PASS": "\033[92m", # GREEN
        "WARN": "\033[93m", # YELLOW
        "FAIL": "\033[91m", # RED
        "SKIPPED": "\033[94m" # BLUE
    }

    def __init__(
        self,
        check_type: str, 
        status: Optional[StatusLiteral] = None,
        file_name: Optional[tuple[str,...] | str]= None,
        table_name: Optional[tuple[str,...] | str]= None,
        column_name: Optional[tuple[str,...] | str]= None,
        violation_pct: Optional[float] = None,
        threshold: Optional[dict] = None, # define the upperbound of a status. Anything else will be FAIL status
        troubleshooting_message: Optional[str] = None,
        **kwargs
    ):
        """
        Initialize a CheckResult instance.
        Parameters:
            check_type (str): The type of check performed.
            status (Optional[StatusLiteral]): The status of the check ('PASS', 'WARN', or 'FAIL', 'SKIPPED'). If not provided, it will be inferred based on violation_pct and threshold.
            file_name (Optional[str or tuple[str, ...]]): The file name(s) associated with the check.
                Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
            table_name (Optional[str or tuple[str, ...]]): The table name(s) associated with the check.
                Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
            column_name (Optional[str or tuple[str, ...]]): The column name(s) associated with the check.
                Accepts either a string or a tuple of strings. If a string is provided, it is automatically converted to a tuple internally.
            violation_pct (Optional[float]): The percentage of violations found. If not provided, the status must be provided.
            threshold (Optional[dict]): Thresholds for determining check status. Keys are status names ('PASS', 'WARN', 'or 'FAIL') and values are the upper bounds for those statuses.
                If not provided, defaults to {'PASS': 0.0}, meaning any violation will result in a 'FAIL' status.
            troubleshooting_message (Optional[str]): Message to help troubleshoot failures. 
        """
        # Convert single strings to tuples for consistency
        if isinstance(file_name, str):
            file_name = (file_name,)
        if isinstance(table_name, str):
            table_name = (table_name,)
        if isinstance(column_name, str):
            column_name = (column_name,)
        
        self.check_type = check_type
        self.file_name = file_name
        self.table_name = table_name
        self.column_name = column_name
        self.violation_pct = violation_pct
        self.threshold = threshold or {'PASS': 0.0}  
        self.troubleshooting_message = troubleshooting_message
        self.kwargs = kwargs

        if status:
            self.status = status
        else:
            if threshold is not None and violation_pct is not None:
                self.infer_status()
            else:
                raise TypeError(
                    "Status not provided and cannot infer status because either "
                    "violation_pct or threshold is not provided."
                )
        if self.status == 'FAIL':
            CheckResult.dq_fail.append(self)
        elif self.status == 'WARN':
            CheckResult.dq_warn.append(self)
        elif self.status == 'SKIPPED':
            CheckResult.dq_skip.append(self)
        elif self.status == 'PASS':
            CheckResult.success_count += 1


    def __bool__(self):
        """
        Returns True if the status is 'PASS', otherwise False.
        """
        return self.status == 'PASS'

    def __str__(self):
        """
        Returns a string representation of the CheckResult instance.
        """
        status_str = f"[{self.status}] Check: {self.check_type}. "
        
        location_str = ""
        if self.column_name:
            location_str += f"Column: {self.column_name}; "
        if self.table_name:
            location_str += f"Table: {self.table_name}; "
        if self.file_name:
            location_str += f"File: {self.file_name}; "
        
        violation_pct_str = ""
        if self.violation_pct:
            violation_pct_str += f"(violation: {self.violation_pct:.2%}) "
        
        if self.status == 'PASS' or not self.troubleshooting_message:
            troubleshooting_message_str = ''
        else:
            troubleshooting_message_str = self.troubleshooting_message
        
        kwargs_str = '; '.join([f"{k}: {v}" for k, v in self.kwargs.items()]) if self.kwargs else ''

        return self.STATUS_ANSI_COLOR[self.status] + status_str + location_str + kwargs_str + violation_pct_str + troubleshooting_message_str + "\033[0m"
        
    def infer_status(self):
        """
        Infer the status based on the violation percentage and the defined thresholds.
        """
        for threshold_status_name in self.threshold.keys():
            if threshold_status_name not in self.ALLOWED_STATUS:
                raise ValueError(
                    f"Invalid status in threshold: {threshold_status_name}. Expected one of: {self.ALLOWED_STATUS}"
                ) 
        sorted_threshold_items = sorted(self.threshold.items(), key=lambda item: item[1])
        for (status, cutoff) in sorted_threshold_items:
            if self.violation_pct <= cutoff:
                self.status = status
                return
        self.status = 'FAIL'

    def log(self, logger: logging.Logger, level_str: str = 'DQ'):
        """
        Log the CheckResult using the provided logger at the specified log level. 
        Parameters:
            logger (logging.Logger): The logger to use for logging.
            level_str (str): The log level as a string ('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'DQ').
                'DQ' is a custom level for data quality logs, mapped to INFO level.
        """
        if level_str not in logging._nameToLevel.keys():
            raise ValueError(
                f"Invalid log level: {level_str}. Expected one of: {list(logging._nameToLevel.keys()) + ['DQ']}"
            )
        
        log_level = logging._nameToLevel[level_str]
        logger.log(log_level, self.__str__())

    def summary(
            logger: Optional[logging.Logger] = None, 
            level_str: Optional[logging.Logger] = 'DQ'
        ) -> str:
        """
        Print a summary of all CheckResult instances.
        """
        total_checks = CheckResult.success_count + len(CheckResult.dq_fail) + len(CheckResult.dq_warn) + len(CheckResult.dq_skip)
        summary_lines = [
            "Data Quality Check Summary:",
            "---------------------------------",
            f"Total checks: {total_checks}",
            f"  PASS: {CheckResult.success_count}",
            f"  WARN: {len(CheckResult.dq_warn)}",
            f"  FAIL: {len(CheckResult.dq_fail)}",
            f"  SKIPPED: {len(CheckResult.dq_skip)}",
            "---------------------------------",
            "Failed DQ Checks:"
        ] + [
            check_result.__str__() for check_result in CheckResult.dq_fail
        ] + [
            'Warning DQ Checks:'
        ] + [
            check_result.__str__() for check_result in CheckResult.dq_warn
        ]
        summary_text = "\n".join(summary_lines)
        if logger:
            if level_str not in logging._nameToLevel.keys():
                raise ValueError(
                    f"Invalid log level: {level_str}. Expected one of: {list(logging._nameToLevel.keys()) + ['DQ']}"
                )
            log_level = logging._nameToLevel[level_str]
            logger.log(log_level, summary_text)
        return summary_text
        

