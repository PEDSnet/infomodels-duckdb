# Optional CDM tables that are not required for submission
# These tables will still be loaded if present in the submission files,
# but are not included in any checks
OPTIONAL_TABLES = [
    'cohort_definition', 
    'concept', 
    'concept_ancestor', 
    'concept_class', 
    'concept_relationship', 
    'concept_synonym', 
    'condition_era', 
    'domain', 
    'dose_era', 
    'drug_era', 
    'drug_strength', 
    'observation_period', 
    'relationship', 
    'source_to_concept_map', 
    'vocabulary'
]

# Thresholds for data quality (DQ) checks.
#
# Each key represents a specific DQ check type (e.g., "foreign_key_violation").
# The corresponding value is a list of threshold rules.
#
# Each rule is a dictionary that can include any combination of the following optional keys:
#   - table_name: Table pattern (supports Unix-style wildcards, e.g., '*'). Defaults to '*'.
#   - column_name: Column pattern (supports wildcards). Defaults to '*'.
#   - threshold: Allowed violation rate before triggering a warning/failure.
#
# Rules are evaluated in order; later matching rules override earlier ones.
# If a key (table_name or column_name) is omitted, it defaults to '*', matching any value.

DQ_THRESHOLDS = {
    "foreign_key_violation": [
        {
            "table_name": "*",
            "column_name": "*",
            "threshold": {'PASS': 0.0, 'WARN': 0.05,},
        },
        {
            "table_name": "*",
            "column_name": "person_id",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
        {
            "table_name": "*",
            "column_name": "visit_occurrence_id",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
        {
            "table_name": "*",
            "column_name": "provider_id",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
        {
            "table_name": "*",
            "column_name": "care_site_id",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
    ],
    "not_null_violation": [
        {
            "table_name": "*",
            "column_name": "*",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
    ],
    "distinct_violation": [
        {
            "table_name": "*",
            "column_name": "*",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
    ],
}
