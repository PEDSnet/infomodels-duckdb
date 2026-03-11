from src.dq_checks.check_result import CheckResult
from duckdb import DuckDBPyConnection
from src.util import table_exists, column_exists
from src.config import LOGGER

FACT_RELATIONSHIP_DOMAIN_CONCEPT_ID_TO_CDM_MAPPING = {
    27: {"table_name": "observation", "column_name": "observation_id"},
    21: {"table_name": "measurement", "column_name": "measurement_id"},
    8: {"table_name": "visit_occurrence", "column_name": "visit_occurrence_id"},
    13: {"table_name": "drug_exposure", "column_name": "drug_exposure_id"},
    17: {"table_name": "device_exposure", "column_name": "device_exposure_id"},
    19: {"table_name": "condition_occurrence", "column_name": "condition_occurrence_id"},
    56: {"table_name": "person", "column_name": "person_id"},
    10: {"table_name": "procedure_occurrence", "column_name": "procedure_occurrence_id"},
}

def check_fact_relationship(
    con: DuckDBPyConnection,
    skip_tables: list = None
) -> CheckResult:
    """
    Check fact_ids in fact_relationship table exist in the corresponding domain tables.
    """
    if not table_exists(con, 'fact_relationship'):
        result = CheckResult(
            check_type='fact_relationship_violation',
            status='SKIPPED',
            troubleshooting_message=f'Table fact_relationship does not exist in the database.'
        )
        result.log(LOGGER, duckdb_conn=con)
        return result
    if not column_exists(con, 'fact_relationship', 'domain_concept_id_1') or not column_exists(con, 'fact_relationship', 'domain_concept_id_2') or not column_exists(con, 'fact_relationship', 'fact_id_1') or not column_exists(con, 'fact_relationship', 'fact_id_2'):
        result = CheckResult(
            check_type='fact_relationship_violation',
            status='SKIPPED',
            troubleshooting_message=f'One or more required columns (domain_concept_id_1, domain_concept_id_2, fact_id_1, fact_id_2) do not exist in fact_relationship table.'
        )
        result.log(LOGGER, duckdb_conn=con)
        return result
    for domain_concept_id, mapping in FACT_RELATIONSHIP_DOMAIN_CONCEPT_ID_TO_CDM_MAPPING.items():
        if skip_tables and mapping["table_name"] in skip_tables:
            continue
        table_name = mapping["table_name"]
        column_name = mapping["column_name"]
        if not column_exists(con, table_name, column_name):
            result = CheckResult(
                check_type='fact_relationship_violation',
                table_name=table_name,
                column_name=column_name,
                status='SKIPPED',
                troubleshooting_message=f'Column {column_name} does not exist in table {table_name}.'
            )
            result.log(LOGGER, duckdb_conn=con)
            continue
        fact1_total_count_query = f"""
            SELECT COUNT(*) AS total_count
            FROM fact_relationship
            WHERE domain_concept_id_1 = {domain_concept_id}
        """
        fact2_total_count_query = f"""
            SELECT COUNT(*) AS total_count
            FROM fact_relationship
            WHERE domain_concept_id_2 = {domain_concept_id}
        """
        total_fact_count = con.execute(fact1_total_count_query).fetchone()[0] + con.execute(fact2_total_count_query).fetchone()[0]
        if total_fact_count == 0:
            result = CheckResult(
                check_type='fact_relationship_violation',
                status='SKIPPED',
                table_name=table_name,
                column_name=column_name,
                troubleshooting_message=f'No records in fact_relationship table with domain_concept_id_1 or domain_concept_id_2 = {domain_concept_id}.'
            )
            result.log(LOGGER, duckdb_conn=con)
            continue
        
        check_count_fact1_query = f"""
            SELECT COUNT(*) AS missing_count
            FROM fact_relationship f
            LEFT JOIN
            {table_name} t
            ON 
                f.fact_id_1 = t.{column_name}
            WHERE f.domain_concept_id_1 = {domain_concept_id}
            AND t.{column_name} IS NULL
        """
        check_count_fact2_query = f"""
            SELECT COUNT(*) AS missing_count
            FROM fact_relationship f
            LEFT JOIN
            {table_name} t
            ON 
                f.fact_id_2 = t.{column_name}
            WHERE f.domain_concept_id_2 = {domain_concept_id}
            AND t.{column_name} IS NULL
        """
        fact1_bad_count = con.execute(check_count_fact1_query).fetchone()[0]
        fact2_bad_count = con.execute(check_count_fact2_query).fetchone()[0]
        # get sample of bad records for troubleshooting message
        sample_bad_records = []
        if fact1_bad_count > 0:
            fact1_sample_query = f"""
                SELECT DISTINCT f.fact_id_1
                FROM fact_relationship f
                LEFT JOIN
                {table_name} t
                ON 
                    f.fact_id_1 = t.{column_name}
                WHERE f.domain_concept_id_1 = {domain_concept_id}
                AND t.{column_name} IS NULL
                LIMIT 10
            """
            fact1_sample = con.execute(fact1_sample_query).fetchall()
            sample_bad_records += [f"fact_id_1={row[0]}" for row in fact1_sample]
        elif fact2_bad_count > 0:
            fact2_sample_query = f"""
                SELECT DISTINCT f.fact_id_2
                FROM fact_relationship f
                LEFT JOIN
                {table_name} t
                ON 
                    f.fact_id_2 = t.{column_name}
                WHERE f.domain_concept_id_2 = {domain_concept_id}
                AND t.{column_name} IS NULL
                LIMIT 10
            """
            fact2_sample = con.execute(fact2_sample_query).fetchall()
            sample_bad_records += [f"fact_id_2={row[0]}" for row in fact2_sample]
        
        
        total_bad_count = fact1_bad_count + fact2_bad_count
        if total_bad_count > 0:
            total_bad_percent = 1.0 * total_bad_count / total_fact_count
            result = CheckResult(
                check_type='fact_relationship_violation',
                status = 'WARN',
                table_name=table_name,
                column_name=column_name,
                violation_pct=total_bad_percent,
                troubleshooting_message=f'There are {total_bad_count} records in fact_relationship table with domain_concept_id_1 or domain_concept_id_2 = {domain_concept_id} that do not have matching records in {table_name} table. This accounts for {total_bad_percent:.2%} of total {total_fact_count} records with this domain_concept_id in fact_relationship. Sample bad records: {", ".join(sample_bad_records)}. Please ensure all fact_ids in fact_relationship have corresponding records in the domain tables.',
            )
        else:
            result = CheckResult(
                check_type='fact_relationship_violation',
                status = 'PASS',
                table_name=table_name,
                column_name=column_name,
                violation_pct=0.0,
                troubleshooting_message=f'All fact_ids in fact_relationship table with domain_concept_id_1 or domain_concept_id_2 = {domain_concept_id} have matching records in {table_name} table.',
            )
        result.log(LOGGER, duckdb_conn=con)