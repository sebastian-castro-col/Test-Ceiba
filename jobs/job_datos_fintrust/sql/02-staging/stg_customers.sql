

-- Limpiar y estandarizar la tabla de clientes
-- Reglas   :
--   1. Eliminar espacios adicionales en nombre y ciudad
--   2. Normalizar nombre de ciudades (capitalizar)
--   3. Validar que segmento sea uno de los valores permitidos
--   4. Marcar registros con income <= 0 o customer_id nulo como inválidos

CREATE OR REPLACE TABLE staging.stg_customers AS
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
FROM raw_fintrust.customers;
