from src.util import get_threshold


def test_get_threshold():
    assert get_threshold(check_type='foreign_key_violation', table_name='person', column_name = 'person_id') == 0.01
    assert get_threshold(check_type='foreign_key_violation', table_name='visit_occurrence_id', column_name = 'some_other_column') == 0.05