# Optional CDM tables that are not required for submission
# These tables will still be loaded if present in the submission files,
# but are not included in any checks
OPTIONAL_TABLES = [
    'cohort_definition', 
    'concept', 
    'concept_ancestor', 
    'concept_class', 
    'concept_metadata',
    'concept_relationship', 
    'concept_relationship_metadata',
    'concept_synonym', 
    'condition_era', 
    'domain', 
    'dose_era', 
    'drug_era', 
    'drug_strength', 
    'observation_period', 
    'pack_content',
    'relationship', 
    'source_to_concept_map', 
    'vocabulary',
    'lab_site_mapping'
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
    "datetime_concordance": [
        {
            "table_name": "*",
            "column_name": "*",
            "threshold": {'PASS': 0.0, 'WARN': 0.01,},
        },
    ],
}


# Assess date/datetime concordance in raw data
# Each entry: date : datetime pairs
DATE_DATETIME_COLUMNS= {
    'person' : {
        'birth_date' : 'birth_datetime'
    },

    'death' : {
        'death_date' : 'death_datetime'
    },

    'visit_occurrence' : {
        'visit_start_date' : 'visit_start_datetime',
        'visit_end_date' : 'visit_end_datetime'
    },

    'condition_occurrence' : {
        'condition_start_date' : 'condition_start_datetime',
        'condition_end_date' : 'condition_end_datetime'
    },

    'procedure_occurrence' :  {
        'procedure_date' : 'procedure_datetime',
        'procedure_end_date' : 'procedure_end_datetime'
    },

    'observation' : {
        'observation_date' : 'observation_datetime'
    },

    'drug_exposure' : {
        'drug_exposure_start_date' : 'drug_exposure_start_datetime',
        'drug_exposure_end_date' : 'drug_exposure_end_datetime',
        'drug_exposure_order_date' : 'drug_exposure_order_datetime'
    },

    'measurement' : {
        'measurement_date' : 'measurement_datetime',
        'measurement_order_date' : 'measurement_order_datetime',
        'measurement_result_date' : 'measurement_result_datetime'
    },

    'immunization' : {
        'imm_exp_date' : 'imm_exp_datetime',
        'imm_recorded_date' : 'imm_recorded_datetime',
        'immunization_date' : 'immunization_datetime'
    },

    'device_exposure' : {
        'device_exposure_start_date' : 'device_exposure_start_datetime',
        'device_exposure_end_date' : 'device_exposure_end_datetime'
    },

    'location_history' : {
        'start_date' : 'start_datetime',
        'end_date' : 'end_datetime'
    }

}