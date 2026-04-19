
-- Q02: Recaudo diario total y recaudo aplicado a cuotas vencidas

SELECT
    payment_date AS fecha_pago,
    SUM(num_pagos) AS numero_pagos,
    SUM(recaudo_total) AS recaudo_total_COP,
    SUM(recaudo_en_mora) AS recaudo_en_mora_COP
FROM test-ceiba-col.analytics.vw_recaudo_diario
GROUP BY payment_date
ORDER BY payment_date ASC;
