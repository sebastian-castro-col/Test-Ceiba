
-- Q03: Cartera al día vs cartera en mora por cohorte de originación
-- Incluye indicadores de deterioro temprano (primeros 3 meses de vida)

WITH cohorte_base AS (
    SELECT
        cohort_month,
        FORMAT_DATE('%Y-%m', cohort_month)                         AS cohort_label,
        segment,
        COUNT(DISTINCT loan_id)                                 AS num_creditos,
        SUM(loan_principal)                                     AS cartera_originada,
        SUM(total_due)                                          AS total_programado,
        SUM(total_paid)                                         AS total_recaudado,
        SUM(balance_due)                                        AS saldo_total,
        SUM(CASE WHEN is_overdue THEN balance_due ELSE 0 END)   AS saldo_mora,
        SUM(CASE WHEN NOT is_overdue
                  AND installment_status <> 'PAID'
                  THEN balance_due ELSE 0 END)                  AS saldo_vigente,
        MAX(days_overdue)                                       AS max_dias_mora,
        COUNT(CASE WHEN is_overdue THEN 1 END)                  AS cuotas_en_mora,
        COUNT(installment_id)                                   AS cuotas_totales
    FROM test-ceiba-col.analytics.dm_cartera
    GROUP BY cohort_month, FORMAT_DATE('%Y-%m', cohort_month) , segment
)
SELECT
    cohort_label                            AS cohorte,
    segment                                 AS segmento,
    num_creditos,
    cartera_originada,
    total_programado,
    total_recaudado,
    saldo_total,
    saldo_vigente,
    saldo_mora,
    -- Indicadores de mora
    ROUND(100.0 * saldo_mora / NULLIF(saldo_total, 0), 2)       AS pct_mora,
    ROUND(100.0 * cuotas_en_mora / NULLIF(cuotas_totales, 0), 2) AS pct_cuotas_mora,
    max_dias_mora,
    -- Cobertura de recaudo
    ROUND(100.0 * total_recaudado / NULLIF(total_programado, 0), 2) AS pct_cobertura_recaudo
FROM cohorte_base
ORDER BY cohort_month, segment;
