from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_transform_loans():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """

    CREATE OR REPLACE TABLE test-ceiba-col.raw_fintrust.stg_loans AS

    WITH valid_customers AS (
        SELECT customer_id FROM test-ceiba-col.raw_fintrust.stg_customers WHERE is_valid = TRUE
    )
    SELECT
        l.loan_id,
        l.customer_id,
        l.origination_date,
        l.principal_amount,
        l.annual_rate,
        ROUND(l.annual_rate / 12, 6) AS monthly_rate,
        l.term_months,
        UPPER(TRIM(l.loan_status)) AS loan_status,
        TRIM(l.product_type) AS product_type,
        DATE_TRUNC(l.origination_date, MONTH) AS cohort_month,
        -- Indicadores de calidad
        CASE
            WHEN l.loan_id IS NULL THEN FALSE
            WHEN l.customer_id IS NULL THEN FALSE
            WHEN vc.customer_id IS NULL THEN FALSE    
            WHEN l.principal_amount IS NULL OR l.principal_amount <= 0 THEN FALSE
            WHEN l.annual_rate IS NULL OR l.annual_rate <= 0 OR l.annual_rate >= 1 THEN FALSE
            WHEN l.term_months IS NULL OR l.term_months <= 0 THEN FALSE
            WHEN UPPER(TRIM(l.loan_status)) NOT IN ('ACTIVE','CLOSED','DEFAULT') THEN FALSE
            ELSE TRUE
        END AS is_valid,
        CASE
            WHEN l.loan_id IS NULL THEN 'NULL loan_id'
            WHEN l.customer_id IS NULL THEN 'NULL customer_id'
            WHEN vc.customer_id IS NULL THEN 'customer_id no existe: ' || l.customer_id
            WHEN l.principal_amount IS NULL OR l.principal_amount <= 0 THEN 'principal_amount inválido'
            WHEN l.annual_rate IS NULL OR l.annual_rate <= 0 OR l.annual_rate >= 1 THEN 'annual_rate inválida'
            WHEN l.term_months IS NULL OR l.term_months <= 0 THEN 'term_months inválido'
            WHEN UPPER(TRIM(l.loan_status)) NOT IN ('ACTIVE','CLOSED','DEFAULT') THEN 'loan_status desconocido: ' || l.loan_status
            ELSE NULL
        END AS quality_issue,
        CURRENT_TIMESTAMP AS _stg_loaded_at
    FROM test-ceiba-col.raw_fintrust.loans l
    LEFT JOIN valid_customers vc ON l.customer_id = vc.customer_id;



"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

