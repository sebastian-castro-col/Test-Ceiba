
-- Q04: Top 10 créditos con mayor atraso y saldo pendiente

SELECT
    ranking_mora                            AS ranking,
    loan_id,
    full_name                               AS cliente,
    city                                    AS ciudad,
    segment                                 AS segmento,
    loan_status                             AS estado_credito,
    loan_principal                          AS monto_original_COP,
    total_paid                              AS total_pagado_COP,
    saldo_mora                              AS saldo_en_mora_COP,
    total_balance_due                       AS saldo_total_pendiente_COP,
    max_days_overdue                        AS dias_maximo_mora,
    loan_health                             AS clasificacion_salud
FROM test-ceiba-col.analytics.vw_top_mora
WHERE ranking_mora <= 10
ORDER BY ranking_mora;
