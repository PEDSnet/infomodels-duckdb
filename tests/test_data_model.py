from src.data_model import DataModel
import pytest
import json
import duckdb

model = 'pedsnet'
version = '5.7.0'
json_file_path = 'tests/data/data_model/pedsnet_v57_data_model.json'

with open(json_file_path) as f:
    data_model_dict = json.load(f)

@pytest.fixture(scope='module')
def _data_model_from_json():
    data_model = DataModel(mode='json', name=model, version=version, file_path=json_file_path)
    return data_model   

# Test initialization with JSON file
def test_data_model_init_json(_data_model_from_json):
    data_model = _data_model_from_json
    assert data_model.mode == 'json'
    assert data_model.source == json_file_path
    assert data_model.name == model
    assert data_model.version == version
    assert data_model.data == data_model_dict
    assert len(data_model.data['tables']) > 0

# Test initialization with data-models-service
def test_data_model_init_service():
    data_model = DataModel(mode='data-models-service', name=model, version=version)
    assert data_model.mode == 'data-models-service'
    assert data_model.source == 'https://data-models-service.research.chop.edu/schemata/pedsnet/5.7.0?format=json'
    assert data_model.name == model
    assert data_model.version == version
    assert data_model.data['model'] == model
    assert data_model.data['version'] == version
    assert set(data_model.data.keys()) == set(['model', 'schema', 'tables', 'version'])
    assert len(data_model.data['tables']) > 0

# Test intialization with invalid mode
def test_data_model_init_invalid_mode():
    with pytest.raises(ValueError):
        DataModel(mode='some_mode', name=model, version=version)

# Test all_table_names method
def test_all_table_names(_data_model_from_json):
    data_model = _data_model_from_json
    table_names = data_model.all_table_names()
    assert isinstance(table_names, list)
    assert len(table_names) == len(data_model.data['tables'])
    assert all(isinstance(name, str) for name in table_names)
    assert len(table_names) > 0
    assert 'person' in table_names
    assert set(table_names) == set([item['name'] for item in data_model_dict['tables']])

# Check all_column_names_in_table method
def test_all_column_names_in_table(_data_model_from_json):
    data_model = _data_model_from_json
    table_name = 'person'
    column_names = data_model.all_column_names_in_table(table_name)
    expected_columns_names = ['birth_date', 'birth_datetime', 'care_site_id', 'day_of_birth', 'ethnicity_concept_id', 'ethnicity_source_concept_id', 'ethnicity_source_value', 'gender_concept_id', 'gender_source_concept_id', 'gender_source_value', 'language_concept_id', 'language_source_concept_id', 'language_source_value', 'location_id', 'month_of_birth', 'person_id', 'person_source_value', 'pn_gestational_age', 'provider_id', 'race_concept_id', 'race_source_concept_id', 'race_source_value', 'year_of_birth']
    assert isinstance(column_names, list)
    assert len(column_names) > 0
    assert all(isinstance(name, str) for name in column_names)
    assert set(column_names) == set(expected_columns_names)

# Test to_duckdb_ddl method
def test_to_duckdb_ddl(_data_model_from_json):
    data_model = _data_model_from_json
    ddl_dict = data_model.to_duckdb_ddl()
    assert isinstance(ddl_dict, dict)
    assert set(ddl_dict.keys()) == set(data_model.all_table_names())
    for table_name, ddl in ddl_dict.items():
        assert isinstance(ddl, str)
        assert ddl.strip().startswith(f"CREATE TABLE {table_name}")
    # initialze a duckdb in-memory database and execute the DDL statements
    with duckdb.connect(database=':memory:') as conn:
        for ddl in ddl_dict.values():
            conn.execute(ddl)
        # check if tables are created
        existing_tables = conn.execute("SHOW TABLES").fetchall()
        existing_table_names = [item[0] for item in existing_tables]
        assert set(existing_table_names) == set(_data_model_from_json.all_table_names())
        # check if columns are created correctly for a sample table
        sample_table = 'person'
        expected_columns_names = ['birth_date', 'birth_datetime', 'care_site_id', 'day_of_birth', 'ethnicity_concept_id', 'ethnicity_source_concept_id', 'ethnicity_source_value', 'gender_concept_id', 'gender_source_concept_id', 'gender_source_value', 'language_concept_id', 'language_source_concept_id', 'language_source_value', 'location_id', 'month_of_birth', 'person_id', 'person_source_value', 'pn_gestational_age', 'provider_id', 'race_concept_id', 'race_source_concept_id', 'race_source_value', 'year_of_birth']
        columns_info = conn.execute(f"PRAGMA table_info('{sample_table}')").fetchall()
        existing_column_names = [item[1] for item in columns_info]
        assert set(existing_column_names) == set(expected_columns_names)
        # check data types for a sample table
        sample_table = 'drug_exposure'
        conn.execute(f"PRAGMA table_info('{sample_table}')")
        columns_info = conn.execute(f"PRAGMA table_info('{sample_table}')").fetchall()
        existing_columns_types = {item[1]: item[2] for item in columns_info}
        assert existing_columns_types['drug_exposure_id'] == 'BIGINT'
        assert existing_columns_types['person_id'] == 'BIGINT'
        assert existing_columns_types['drug_concept_id'] == 'INTEGER'
        assert existing_columns_types['drug_exposure_start_date'] == 'DATE'
        assert existing_columns_types['drug_exposure_start_datetime'] == 'TIMESTAMP'
        assert existing_columns_types['quantity'] == 'DECIMAL(20,5)'
        assert existing_columns_types['route_source_value'] == 'VARCHAR'


