from config import PROJECT_ID
from google.cloud import bigquery
import time
from datetime import date


def generate_analytics_cartera(analysis_date=None):
    start_time = time.time()

    client = bigquery.Client(project=PROJECT_ID or None)

    # Si no se envía fecha, toma la fecha actual
    if analysis_date is None:
        analysis_date = date.today().strftime("%Y-%m-%d")

    query = f"""

CREATE OR REPLACE TABLE `test-ceiba-col.analytics.dm_cartera` AS
WITH
-- 1. Solo pagos válidos, agrupados por cuota
pagos_por_cuota AS (
    SELECT
        installment_id,
        SUM(payment_amount) AS total_paid,
        MAX(payment_date) AS last_payment_date,
        COUNT(*) AS num_payments
    FROM `test-ceiba-col.raw_fintrust.stg_payments`
    WHERE is_valid = TRUE
    GROUP BY installment_id
),

-- 2. Créditos válidos enriquecidos con datos del cliente
loans_enriched AS (
    SELECT
        l.loan_id,
        l.customer_id,
        c.full_name,
        c.city,
        c.segment,
        l.product_type,
        l.origination_date,
        l.cohort_month,
        l.principal_amount,
        l.annual_rate,
        l.monthly_rate,
        l.term_months,
        l.loan_status
    FROM `test-ceiba-col.raw_fintrust.stg_loans` l
    JOIN `test-ceiba-col.raw_fintrust.stg_customers` c
      ON l.customer_id = c.customer_id
     AND c.is_valid = TRUE
    WHERE l.is_valid = TRUE
),

-- 3. Cuotas limpias
installments_clean AS (
    SELECT *
    FROM `test-ceiba-col.raw_fintrust.stg_installments`
    WHERE is_valid = TRUE
      AND is_anomalous = FALSE
),

-- 4. Ensamble final
cartera_base AS (
    SELECT
        le.loan_id,
        le.customer_id,
        le.full_name,
        le.city,
        le.segment,
        le.product_type,
        le.origination_date,
        le.cohort_month,
        le.principal_amount AS loan_principal,
        le.annual_rate,
        le.monthly_rate,
        le.term_months,
        le.loan_status,
        ic.installment_id,
        ic.installment_number,
        ic.due_date,
        ic.principal_due,
        ic.interest_due,
        ic.total_due,
        ic.installment_status,
        COALESCE(p.total_paid, 0) AS total_paid,
        p.last_payment_date,
        COALESCE(p.num_payments, 0) AS num_payments,

        GREATEST(ic.total_due - COALESCE(p.total_paid, 0), 0) AS balance_due,

        CASE
            WHEN ic.installment_status = 'LATE' THEN TRUE
            WHEN ic.due_date < DATE('{analysis_date}')
                 AND ic.installment_status <> 'PAID' THEN TRUE
            ELSE FALSE
        END AS is_overdue,

        CASE
            WHEN ic.installment_status = 'LATE'
              OR (
                  ic.due_date < DATE('{analysis_date}')
                  AND ic.installment_status <> 'PAID'
              )
            THEN DATE_DIFF(DATE('{analysis_date}'), ic.due_date, DAY)
            ELSE 0
        END AS days_overdue,

        CASE
            WHEN ic.installment_status = 'PAID' THEN 'PAID'
            WHEN ic.due_date >= DATE('{analysis_date}') THEN 'VIGENTE'

            WHEN DATE_DIFF(DATE('{analysis_date}'), ic.due_date, DAY) BETWEEN 1 AND 30
                THEN 'MORA_1_30'

            WHEN DATE_DIFF(DATE('{analysis_date}'), ic.due_date, DAY) BETWEEN 31 AND 60
                THEN 'MORA_31_60'

            WHEN DATE_DIFF(DATE('{analysis_date}'), ic.due_date, DAY) BETWEEN 61 AND 90
                THEN 'MORA_61_90'

            WHEN DATE_DIFF(DATE('{analysis_date}'), ic.due_date, DAY) > 90
                THEN 'MORA_90_MAS'

            ELSE 'OTRO'
        END AS mora_bucket,

        DATE('{analysis_date}') AS analysis_date,
        CURRENT_TIMESTAMP() AS _loaded_at

    FROM loans_enriched le
    JOIN installments_clean ic
        ON le.loan_id = ic.loan_id
    LEFT JOIN pagos_por_cuota p
        ON ic.installment_id = p.installment_id
)

SELECT * FROM cartera_base;


CREATE OR REPLACE TABLE `test-ceiba-col.analytics.dm_resumen_credito` AS
SELECT
    loan_id,
    customer_id,
    full_name,
    city,
    segment,
    product_type,
    origination_date,
    cohort_month,
    loan_principal,
    annual_rate,
    term_months,
    loan_status,
    COUNT(installment_id) AS num_installments,
    SUM(total_due) AS total_programmed,
    SUM(total_paid) AS total_paid,
    SUM(balance_due) AS total_balance_due,
    SUM(CASE WHEN is_overdue THEN balance_due ELSE 0 END) AS saldo_mora,
    SUM(CASE
            WHEN NOT is_overdue
             AND installment_status <> 'PAID'
            THEN balance_due
            ELSE 0
        END) AS saldo_vigente,
    MAX(days_overdue) AS max_days_overdue,

    CASE
        WHEN MAX(days_overdue) = 0 THEN 'AL_DIA'
        WHEN MAX(days_overdue) <= 30 THEN 'ATRASO_LEVE'
        WHEN MAX(days_overdue) <= 90 THEN 'ATRASO_MODERADO'
        ELSE 'DETERIORO_SEVERO'
    END AS loan_health,

    analysis_date,
    CURRENT_TIMESTAMP() AS _loaded_at

FROM `test-ceiba-col.analytics.dm_cartera`
GROUP BY ALL;

"""

    query_job = client.query(query)
    query_job.result()

    execution_time = time.time() - start_time

    print(f"Proceso finalizado correctamente")
    print(f"Analysis Date: {analysis_date}")
    print(f"Tiempo Ejecución real: {execution_time:.2f} Seconds")