
-- Q02: Recaudo diario total y recaudo aplicado a cuotas vencidas

SELECT
    payment_date                            AS fecha_pago,
    SUM(num_pagos)                          AS numero_pagos,
    SUM(recaudo_total)                      AS recaudo_total_COP,
    SUM(recaudo_en_mora)                    AS recaudo_en_mora_COP,
    SUM(recaudo_vigente)                    AS recaudo_vigente_COP,
    ROUND(
        100.0 * SUM(recaudo_en_mora)
        / NULLIF(SUM(recaudo_total), 0), 2
    )                                       AS pct_recaudo_mora,
    -- Canales de pago del día (para análisis de mix)
    STRING_AGG(DISTINCT payment_channel, ', ') AS canales_utilizados
FROM test-ceiba-col.analytics.vw_recaudo_diario
GROUP BY payment_date
ORDER BY payment_date DESC;
