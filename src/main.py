from src.config import CONFIG, LOGGER
from src.dq_checks.check_result import CheckResult
from src.load_duckdb import create_duckdb_tables, load_csv_to_duckdb, init_duckdb_logging_schema
from src.data_model import DataModel
from src.constants import OPTIONAL_TABLES
from src.dq_checks.check_file_completeness import check_missing_submission_file, check_extra_submission_file
from src.dq_checks.check_header import check_duplicated_column_in_csv, check_extra_column_in_csv, check_missing_column_in_csv
from src.dq_checks.check_fk import check_fk_violation
from src.dq_checks.check_not_null import check_not_null_violation
from src.dq_checks.check_distinct import check_distinct_violation
import duckdb
import os
import fnmatch
from datetime import datetime


def main():
    run_id = datetime.now().strftime("%Y-%m-%d_%H:%M:%S.%f")
    CheckResult.run_id = run_id
    with duckdb.connect(CONFIG['duckdb']['path']) as con:
        init_duckdb_logging_schema(con, run_id, CONFIG)
        LOGGER.info(f"Run ID: {run_id}.\nRunning with config: " + str(CONFIG))  
        # get data models
        LOGGER.info(f"Loading data models with config: {CONFIG['data-models']}")
        data_model = DataModel(**CONFIG['data-models'])
        #data_models_dict = data_model.data
        LOGGER.info("Data models loaded successfully. ")

        skip_check_tables = list(OPTIONAL_TABLES)
        skip_check_columns = dict() # a dict of {table_name: (column_name, ...)}
        skip_duckdb_load_table_patterns = list(CONFIG['duckdb'].get('skip_load', []))
        skip_duckdb_load_tables = [table for table in data_model.all_table_names() if any(fnmatch.fnmatch(table, pattern) for pattern in skip_duckdb_load_table_patterns)]
        LOGGER.debug(f"Tables to skip loading into DuckDB from config: {skip_duckdb_load_tables}")

        # Initialize DuckDB database
        LOGGER.info("Initializing DuckDB database.")
        create_duckdb_tables(data_model, con, skip_tables = skip_duckdb_load_tables, recreate = True)
        LOGGER.info("DuckDB tables created successfully.")

        # check submission files completeness
        submission_dir = CONFIG['submission_files']['dir']
        submission_file_extension = CONFIG['submission_files'].get('file_extension', '.csv')

        LOGGER.debug("Checking submission files completeness.")
        required_cdm_tables = tuple(set(data_model.all_table_names()) - set(OPTIONAL_TABLES) - set(skip_duckdb_load_tables))
        check_result_missing_submission_file = check_missing_submission_file(
            file_dir = submission_dir,
            cdm_tables_expected = required_cdm_tables,
            file_extension = submission_file_extension
        )
        if check_result_missing_submission_file.status != 'PASS':
            # skip checks for missing tables
            skip_check_tables.extend(check_result_missing_submission_file.table_name)

        LOGGER.debug("Checking for extra submission files.")
        check_result_extra_submission_file = check_extra_submission_file(
            file_dir = submission_dir,
            cdm_tables_expected = data_model.all_table_names(),
            file_extension = submission_file_extension
        )

        # check header issues
        for table_name in data_model.all_table_names():
            # check if file exists for the table
            file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
            if not os.path.isfile(file_path):
                LOGGER.debug(f"No submission file found for table {table_name}. Skipping header checks.")
                continue
            LOGGER.debug(f"Checking header for table: {table_name}, file: {file_path}")
            # check duplicated columns in csv
            check_result_duplicated_column = check_duplicated_column_in_csv(file_path, table_name)
            if check_result_duplicated_column.status != 'PASS':
                # if the csv has duplicated columns, don't load the table to duckdb
                skip_duckdb_load_tables.append(table_name)
                skip_check_tables.append(table_name)

            # check extra columns in csv
            check_result_extra_column = check_extra_column_in_csv(file_path, data_model, table_name)

            # check missing columns in csv
            check_result_missing_column = check_missing_column_in_csv(file_path, data_model, table_name)
            if check_result_missing_column.status != 'PASS':
                skip_check_columns.get(table_name, tuple()) + check_result_missing_column.column_name
        
        # Loading csv files to DuckDB
        for table_name in data_model.all_table_names():
            file_path = f"{submission_dir}/{table_name}{submission_file_extension}"
            if not os.path.isfile(file_path):
                LOGGER.debug(f"No submission file found for table {table_name}. Skipping DuckDB load.")
                continue
            if table_name in skip_duckdb_load_tables:
                LOGGER.debug(f"Skipping loading {table_name} to DuckDB as it is in the skip list.")
                continue
            LOGGER.debug(f"Loading {file_path} into DuckDB table {table_name}.")
            load_csv_to_duckdb(csv_path=file_path, con=con, table_name=table_name, accept_additional_col=True)

        LOGGER.info("All submission files loaded into DuckDB successfully.")
        
        # Check foreign key violations
        LOGGER.info("Checking foreign key violations.") 
        for fk_definition in data_model.data['schema']['constraints']['foreign_keys']:
            main_table = fk_definition['source_table']
            main_column = fk_definition['source_field']
            reference_table = fk_definition['target_table']
            reference_column = fk_definition['target_field']
            if main_table in skip_check_tables:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as main table is in the skip list.")
                continue
            if reference_table in skip_check_tables or reference_table in skip_check_tables:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as reference table is in the skip list.")
                continue
            if main_table in skip_check_columns.keys() and main_column in skip_check_columns[main_table]:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as the main column is in the skip list.")
                continue
            if reference_table in skip_check_columns.keys() and reference_column in skip_check_columns[reference_table]:
                LOGGER.debug(f"Skipping foreign key check for {main_table}.{main_column} referencing {reference_table}.{reference_column} as the reference column is in the skip list.")
                continue
            check_result_fk = check_fk_violation(
                con=con,
                main_table=main_table,
                main_column=main_column,
                reference_table=reference_table,
                reference_column=reference_column,
            )
            LOGGER.debug(f"Foreign Key Check Finished.")
        
        # Check Not Null violations
        for not_null_definition in data_model.data['schema']['constraints']['not_null']:
            table_name = not_null_definition['table']
            column_name = not_null_definition['field']

            if table_name in skip_check_tables:
                LOGGER.debug(f"Skipping Not Null check for {table_name}.{column_name} as table is in the skip list.")
                continue
            if table_name in skip_check_columns.keys() and column_name in skip_check_columns[table_name]:
                LOGGER.debug(f"Skipping Not Null check for {table_name}.{column_name} as column is in the skip list.")
                continue
            check_result_not_null = check_not_null_violation(
                con=con,
                table_name=table_name,
                column_name=column_name,
            )
            LOGGER.debug(f"Not Null Check Finished.")

        # Check Distinct violations
        for distinct_definition in data_model.data['schema']['constraints']['uniques']:
            table_name = distinct_definition['table']
            column_name = distinct_definition['field']

            if table_name in skip_check_tables:
                LOGGER.debug(f"Skipping Distinct check for {table_name}.{column_name} as table is in the skip list.")
                continue
            if table_name in skip_check_columns.keys() and column_name in skip_check_columns[table_name]:
                LOGGER.debug(f"Skipping Distinct check for {table_name}.{column_name} as column is in the skip list.")
                continue
            check_result_distinct = check_distinct_violation(
                con=con,
                table_name=table_name,
                column_names=column_name,
            )
            LOGGER.debug(f"Distinct Check Finished.")
        
        # Check PK violations
        # PK is a combination of NOT NULL and DISTINCT
        for pk_definition in data_model.data['schema']['constraints']['primary_keys']:
            table_name = pk_definition['table']
            column_names = tuple(pk_definition['fields'])  # list of columns in the primary key

            if table_name in skip_check_tables:
                LOGGER.debug(f"Skipping Primary Key check for {table_name}({', '.join(column_names)}) as table is in the skip list.")
                continue
            if table_name in skip_check_columns.keys() and any(col in skip_check_columns[table_name] for col in column_names):
                LOGGER.debug(f"Skipping Primary Key check for {table_name}({', '.join(column_names)}) as one or more columns are in the skip list.")
                continue
            # check not null for each column in the primary key
            for column_name in column_names:
                check_result_pk_not_null = check_not_null_violation(
                    con=con,
                    table_name=table_name,
                    column_name=column_name,
                )
                LOGGER.debug(f"Primary Key Not Null Check Finished for {table_name}.{column_name}.")
            # check distinct for the combination of columns in the primary key
            check_result_pk_distinct = check_distinct_violation(
                con=con,
                table_name=table_name,
                column_names=column_names
            )
            LOGGER.debug(f"Primary Key Distinct Check Finished for {table_name}({', '.join(column_names)}).")
            LOGGER.debug(f"Primary Key Check Finished for {table_name}.{column_names}.")

        
        # Summarize DQ results
        CheckResult.summary(LOGGER)




if __name__ == '__main__':
    main()
