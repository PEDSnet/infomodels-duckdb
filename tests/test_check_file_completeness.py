from src.dq_checks.check_file_completeness import check_extra_submission_file, check_missing_submission_file

file_dir = 'tests/data/cdm/base'

def test_check_missing_file():
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('care_site', 'person', 'visit_occurrence'))
    assert result.status == 'PASS'
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('person1', 'person', 'visit_occurrence'))
    assert result.status == 'FAIL'
    assert type(result.table_name) == tuple
    assert len(result.table_name) == 1
    assert result.table_name[0] == 'person1' 
    result = check_missing_submission_file(file_dir, cdm_tables_expected=('person1', 'person2', 'person', 'visit_occurrence'))
    assert result.status == 'FAIL'
    assert type(result.table_name) == tuple
    assert len(result.table_name) == 2
    assert set(result.table_name) == set(['person1', 'person2'])

def test_check_extra_file():
    result = check_extra_submission_file(file_dir, cdm_tables_expected=('person', ))
    assert result.status == 'WARN'
    assert set(result.file_name) == set(['observation.csv', 'visit_occurrence.csv', 'location.csv', 'provider.csv', 'observation_period.csv', 'drug_exposure.csv', 'condition_occurrence.csv', 'fact_relationship.csv', 'procedure_occurrence.csv', 'care_site.csv'])