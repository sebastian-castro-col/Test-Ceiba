from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_transform_customers():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """

   CREATE OR REPLACE TABLE test-ceiba-col.raw_fintrust.stg_customers AS
    SELECT
        customer_id,
        TRIM(full_name) AS full_name,
        UPPER(LEFT(TRIM(city), 1)) || LOWER(SUBSTRING(TRIM(city), 2)) AS city,
        UPPER(TRIM(segment)) AS segment,
        monthly_income,
        created_at,
        CASE
            WHEN customer_id IS NULL THEN FALSE
            WHEN full_name IS NULL OR TRIM(full_name) = '' THEN FALSE
            WHEN city IS NULL OR TRIM(city) = '' THEN FALSE
            WHEN UPPER(TRIM(segment)) NOT IN ('MASS MARKET','PREMIUM','SME') THEN FALSE
            WHEN monthly_income IS NULL OR monthly_income <= 0 THEN FALSE
            ELSE TRUE
        END AS is_valid,
        CASE
            WHEN customer_id IS NULL THEN 'NULL customer_id'
            WHEN full_name IS NULL OR TRIM(full_name) = '' THEN 'NULL full_name'
            WHEN city IS NULL OR TRIM(city) = '' THEN 'NULL city'
            WHEN UPPER(TRIM(segment)) NOT IN ('MASS MARKET','PREMIUM','SME')
                THEN 'Segmento no reconocido: ' || segment
            WHEN monthly_income IS NULL OR monthly_income <= 0 THEN 'monthly_income inválido'
            ELSE NULL
        END AS quality_issue,
        CURRENT_TIMESTAMP AS _stg_loaded_at
    FROM test-ceiba-col.raw_fintrust.customers;

"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

