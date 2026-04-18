
-- Q01: Desembolso total por día, ciudad y segmento
-- La vista ya agrega por origination_date/city/segment/product_type
-- Sumamos filas en caso de que haya más de un registro por combinación

SELECT
    origination_date                        AS fecha_desembolso,
    city                                    AS ciudad,
    segment                                 AS segmento,
    product_type                            AS tipo_producto,
    SUM(num_creditos)                       AS numero_creditos,
    SUM(monto_desembolsado)                 AS monto_total_COP,
    ROUND(SUM(monto_desembolsado) / NULLIF(SUM(num_creditos), 0), 0) AS ticket_promedio_COP,
    ROUND(AVG(tasa_promedio) * 100, 2)      AS tasa_ea_promedio_pct,
    ROUND(AVG(plazo_promedio), 1)           AS plazo_promedio_meses
FROM test-ceiba-col.analytics.vw_originacion_diaria
GROUP BY origination_date, city, segment, product_type
ORDER BY origination_date DESC, monto_total_COP DESC;
