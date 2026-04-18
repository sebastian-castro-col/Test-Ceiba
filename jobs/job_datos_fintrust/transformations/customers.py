from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_transform_clients():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """

CREATE OR REPLACE TABLE `test-ceiba-col.raw_fintrust.customers_test`
AS 
SELECT * FROM `test-ceiba-col.raw_fintrust.customers` LIMIT 10
"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

