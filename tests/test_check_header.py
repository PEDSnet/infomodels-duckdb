from src.config import CONFIG
from src.util import get_csv_header

test_header = ['person_id','gender_concept_id','year_of_birth','month_of_birth','day_of_birth','race_concept_id','ethnicity_concept_id','location_id','provider_id','care_site_id','person_source_value','gender_source_value','gender_source_concept_id','race_source_value','race_source_concept_id','ethnicity_source_value','ethnicity_source_concept_id','extra_col']


def test_get_csv_header():
    assert get_csv_header('tests/data/cdm/base/person.csv') == test_header


def test_get_csv_header_pipe_delimited():
    original_copy_options = CONFIG['duckdb']['copy_options']
    try:
        # delimiter specified in duckdb copy options with different spellings
        for copy_options in ("FORMAT CSV, HEADER, DELIM '|', ESCAPE '\"'",
                             "FORMAT CSV, HEADER, DELIMITER '|'",
                             "FORMAT CSV, HEADER, SEP '|'",
                             "FORMAT CSV, HEADER, SEPARATOR '|'"):
            CONFIG['duckdb']['copy_options'] = copy_options
            assert get_csv_header('tests/data/cdm/base/person_pipe.csv') == test_header
    finally:
        CONFIG['duckdb']['copy_options'] = original_copy_options



