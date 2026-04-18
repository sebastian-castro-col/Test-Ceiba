from config import PROJECT_ID
from google.cloud import bigquery
import time


def generate_transform_payments():
    start_time = time.time()
    client = bigquery.Client(project=PROJECT_ID or None)
    query = """

CREATE OR REPLACE TABLE test-ceiba-col.raw_fintrust.stg_payments AS

WITH valid_installments AS (
    -- Solo cuotas válidas (excluye I135 anomalous) para validación cruzada
    SELECT installment_id, loan_id
    FROM test-ceiba-col.raw_fintrust.stg_installments
    WHERE is_valid = TRUE AND is_anomalous = FALSE
),
payments_enriched AS (
    SELECT
        p.payment_id,
        p.loan_id,
        p.installment_id,
        p.payment_date,
        p.payment_amount,
        -- Canal NULL → 'UNKNOWN'
        COALESCE(NULLIF(TRIM(p.payment_channel),''), 'UNKNOWN') AS payment_channel,
        UPPER(TRIM(p.payment_status)) AS payment_status,
        p.loaded_at,
        vi.loan_id AS inst_loan_id,
        -- Flags de calidad
        CASE WHEN vi.installment_id IS NULL THEN TRUE ELSE FALSE END AS is_orphan,
        CASE
            WHEN vi.loan_id IS NOT NULL AND vi.loan_id <> p.loan_id THEN TRUE
            ELSE FALSE
        END AS is_loan_mismatch
    FROM raw_fintrust.payments p
    LEFT JOIN valid_installments vi ON p.installment_id = vi.installment_id
)
SELECT
    payment_id,
    loan_id,
    installment_id,
    payment_date,
    payment_amount,
    payment_channel,
    payment_status,
    loaded_at,
    is_orphan,
    is_loan_mismatch,
    -- is_valid: TRUE solo para pagos que entran al recaudo
    CASE
        WHEN payment_status NOT IN ('CONFIRMED') THEN FALSE
        WHEN payment_amount <= 0 THEN FALSE
        WHEN is_orphan = TRUE THEN FALSE
        WHEN is_loan_mismatch = TRUE THEN FALSE
        ELSE TRUE
    END AS is_valid,
    CASE
        WHEN payment_status = 'REVERSED'    THEN 'Pago revertido'
        WHEN payment_status = 'PENDING'     THEN 'Pago pendiente de confirmación'
        WHEN payment_amount <= 0            THEN 'Monto cero o negativo'
        WHEN is_orphan = TRUE               THEN 'installment_id no existe: ' || installment_id
        WHEN is_loan_mismatch = TRUE        THEN 'loan_id del pago (' || loan_id || ') no coincide con loan de la cuota'
        ELSE NULL
    END AS quality_issue,
    CURRENT_TIMESTAMP AS _stg_loaded_at
FROM payments_enriched;


-- Vista de auditoría: pagos rechazados

CREATE OR REPLACE VIEW test-ceiba-col.raw_fintrust.vw_payments_rejected AS
SELECT
    payment_id,
    loan_id,
    installment_id,
    payment_date,
    payment_amount,
    payment_channel,
    payment_status,
    quality_issue,
    _stg_loaded_at
FROM test-ceiba-col.raw_fintrust.stg_payments
WHERE is_valid = FALSE;


"""
    query_job = client.query(query)
    query_job.result()
    execution_time = time.time() - start_time
    print("Tiempo Ejecución real: " + str(execution_time) + " Seconds")

