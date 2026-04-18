from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_transform_installments():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """

CREATE OR REPLACE TABLE test-ceiba-col.raw_fintrust.stg_installments AS

WITH valid_loans AS (
    SELECT loan_id, term_months FROM test-ceiba-col.raw_fintrust.stg_loans WHERE is_valid = TRUE
)
SELECT
    i.installment_id,
    i.loan_id,
    i.installment_number,
    i.due_date,
    i.principal_due,
    i.interest_due,
    (i.principal_due + i.interest_due) AS total_due,
    UPPER(TRIM(i.installment_status)) AS installment_status,
    -- Indicadores de calidad
    CASE
        WHEN i.installment_id IS NULL THEN FALSE
        WHEN i.loan_id IS NULL THEN FALSE
        WHEN vl.loan_id IS NULL THEN FALSE
        WHEN i.principal_due IS NULL OR i.principal_due < 0 THEN FALSE
        WHEN i.interest_due IS NULL OR i.interest_due < 0 THEN FALSE
        WHEN UPPER(TRIM(i.installment_status))
             NOT IN ('PAID','LATE','DUE','PARTIAL') THEN FALSE
        ELSE TRUE
    END AS is_valid,
    -- Detección de anomalías que no implican invalidez pero requieren revisión
    CASE
        WHEN vl.loan_id IS NOT NULL
         AND i.installment_number > vl.term_months THEN TRUE
        ELSE FALSE
    END AS is_anomalous,
    CASE
        WHEN i.installment_id IS NULL THEN 'NULL installment_id'
        WHEN i.loan_id IS NULL THEN 'NULL loan_id'
        WHEN vl.loan_id IS NULL THEN 'loan_id no existe: ' || i.loan_id
        WHEN i.principal_due IS NULL OR i.principal_due < 0 THEN 'principal_due inválido'
        WHEN i.interest_due IS NULL OR i.interest_due < 0 THEN 'interest_due inválido'
        WHEN UPPER(TRIM(i.installment_status))
             NOT IN ('PAID','LATE','DUE','PARTIAL') THEN 'Status desconocido: ' || i.installment_status
        WHEN vl.loan_id IS NOT NULL
         AND i.installment_number > vl.term_months
            THEN 'Nro cuota (' || i.installment_number || ') supera plazo del crédito (' || vl.term_months || ')'
        ELSE NULL
    END AS quality_issue,
    CURRENT_TIMESTAMP AS _stg_loaded_at
FROM test-ceiba-col.raw_fintrust.installments i
LEFT JOIN valid_loans vl ON i.loan_id = vl.loan_id;


"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

