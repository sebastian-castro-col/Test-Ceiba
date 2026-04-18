from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_view_dashboard_bi():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """


CREATE OR REPLACE VIEW test-ceiba-col.analytics.vw_originacion_diaria AS
SELECT
    l.origination_date,
    FORMAT_DATE('%Y-%m', l.origination_date) AS mes_originacion,
    c.city,
    c.segment,
    l.product_type,
    COUNT(DISTINCT l.loan_id)                           AS num_creditos,
    SUM(l.principal_amount)                             AS monto_desembolsado,
    AVG(l.principal_amount)                             AS ticket_promedio,
    AVG(l.annual_rate)                                  AS tasa_promedio,
    AVG(l.term_months)                                  AS plazo_promedio
FROM test-ceiba-col.raw_fintrust.stg_loans l
JOIN test-ceiba-col.raw_fintrust.stg_customers c ON l.customer_id = c.customer_id
WHERE l.is_valid = TRUE AND c.is_valid = TRUE
GROUP BY l.origination_date, FORMAT_DATE('%Y-%m', l.origination_date),
         c.city, c.segment, l.product_type;

-- ----------------------------------------------------------------------------
-- VW 2: Recaudo diario (para KPI de recaudo y cobertura de mora)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW test-ceiba-col.analytics.vw_recaudo_diario AS
WITH pagos_enriquecidos AS (
    SELECT
        p.payment_id,
        p.payment_date,
        p.payment_amount,
        p.payment_channel,
        p.loan_id,
        p.installment_id,
        i.due_date,
        i.installment_status,
        -- El pago fue aplicado a una cuota ya vencida?
        CASE
            WHEN i.installment_status IN ('LATE')             THEN TRUE
            WHEN i.due_date < p.payment_date
             AND i.installment_status NOT IN ('PAID','DUE')   THEN TRUE
            ELSE FALSE
        END                                             AS aplica_mora,
        c.city,
        c.segment
    FROM test-ceiba-col.raw_fintrust.stg_payments p
    JOIN test-ceiba-col.raw_fintrust.stg_installments i  ON p.installment_id = i.installment_id
    JOIN test-ceiba-col.raw_fintrust.stg_loans l         ON p.loan_id = l.loan_id
    JOIN test-ceiba-col.raw_fintrust.stg_customers c     ON l.customer_id = c.customer_id
    WHERE p.is_valid = TRUE
      AND l.is_valid = TRUE
      AND c.is_valid = TRUE
)
SELECT
    payment_date,
    FORMAT_DATE('%Y-%m', payment_date) AS mes_pago,                   
    city,
    segment,
    payment_channel,
    COUNT(DISTINCT payment_id)                          AS num_pagos,
    SUM(payment_amount)                                 AS recaudo_total,
    SUM(CASE WHEN aplica_mora THEN payment_amount ELSE 0 END) AS recaudo_en_mora,
    SUM(CASE WHEN NOT aplica_mora THEN payment_amount ELSE 0 END) AS recaudo_vigente,
    ROUND(
        100.0 * SUM(CASE WHEN aplica_mora THEN payment_amount ELSE 0 END)
        / NULLIF(SUM(payment_amount), 0), 2
    )                                                   AS pct_recaudo_mora
FROM pagos_enriquecidos
GROUP BY payment_date, FORMAT_DATE('%Y-%m', payment_date), city, segment, payment_channel;

-- ----------------------------------------------------------------------------
-- VW 3: Estado de cartera por cohorte (mora vs vigente)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW test-ceiba-col.analytics.vw_cartera_cohorte AS
SELECT
    cohort_month,
    FORMAT_DATE('%Y-%m', cohort_month) AS cohort_label, 
    city,
    segment,
    product_type,
    mora_bucket,
    COUNT(DISTINCT loan_id)                             AS num_creditos,
    COUNT(DISTINCT installment_id)                      AS num_cuotas,
    SUM(total_due)                                      AS total_programado,
    SUM(total_paid)                                     AS total_pagado,
    SUM(balance_due)                                    AS saldo_pendiente,
    SUM(CASE WHEN is_overdue THEN balance_due ELSE 0 END) AS saldo_mora,
    SUM(CASE WHEN NOT is_overdue AND installment_status <> 'PAID'
             THEN balance_due ELSE 0 END)               AS saldo_vigente,
    MAX(days_overdue)                                   AS max_dias_mora
FROM test-ceiba-col.analytics.dm_cartera
GROUP BY cohort_month,FORMAT_DATE('%Y-%m', cohort_month),
         city, segment, product_type, mora_bucket;

-- ----------------------------------------------------------------------------
-- VW 4: Top créditos en mora (para alerta operativa)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW test-ceiba-col.analytics.vw_top_mora AS
SELECT
    loan_id,
    customer_id,
    full_name,
    city,
    segment,
    loan_status,
    loan_principal,
    max_days_overdue,
    saldo_mora,
    total_balance_due,
    total_paid,
    loan_health,
    ROW_NUMBER() OVER (ORDER BY saldo_mora DESC, max_days_overdue DESC) AS ranking_mora
FROM test-ceiba-col.analytics.dm_resumen_credito
WHERE saldo_mora > 0
ORDER BY saldo_mora DESC, max_days_overdue DESC;

-- ----------------------------------------------------------------------------
-- VW 5: Vista maestra BI (dataset completo para Power BI / Tableau / Looker)
--       Contiene todas las dimensiones y métricas en una sola tabla ancha
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW test-ceiba-col.analytics.vw_bi_completo AS
SELECT
    -- Dimensiones de tiempo
    dc.origination_date,
    dc.cohort_month,
    FORMAT_DATE('%Y-%m', dc.cohort_month) AS cohort_label, 
    dc.due_date,
    FORMAT_DATE('%Y-%m', dc.due_date) AS mes_vencimiento,
    dc.last_payment_date,
    dc.analysis_date,
    -- Dimensiones geográficas y demográficas
    dc.city,
    dc.segment,
    dc.product_type,
    -- Dimensiones del crédito
    dc.loan_id,
    dc.customer_id,
    dc.full_name,
    dc.loan_status,
    dc.loan_principal,
    dc.annual_rate,
    ROUND(dc.annual_rate * 100, 2)                      AS annual_rate_pct,
    dc.term_months,
    -- Dimensiones de la cuota
    dc.installment_id,
    dc.installment_number,
    dc.installment_status,
    dc.mora_bucket,
    dc.is_overdue,
    dc.days_overdue,
    -- Métricas financieras
    dc.principal_due,
    dc.interest_due,
    dc.total_due,
    dc.total_paid,
    dc.balance_due,
    dc.num_payments,
    -- Métricas calculadas
    ROUND(100.0 * dc.total_paid / NULLIF(dc.total_due, 0), 2) AS pct_cobrado,
    CASE WHEN dc.total_paid >= dc.total_due THEN TRUE ELSE FALSE END AS cuota_pagada_completa
FROM test-ceiba-col.analytics.dm_cartera dc;



"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

