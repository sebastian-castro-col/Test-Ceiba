-- Limpiar y estandarizar cuotas programadas
-- Reglas   :
--   1. Validar que el loan_id exista en stg_loans
--   2. Detectar installment_number anómalos (> term_months del crédito)
--   3. Validar montos principal_due e interest_due >= 0
--   4. Validar installment_status en dominio conocido

CREATE OR REPLACE TABLE staging.stg_installments AS
WITH valid_loans AS (
    SELECT loan_id, term_months FROM staging.stg_loans WHERE is_valid = TRUE
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
FROM raw_fintrust.installments i
LEFT JOIN valid_loans vl ON i.loan_id = vl.loan_id;
