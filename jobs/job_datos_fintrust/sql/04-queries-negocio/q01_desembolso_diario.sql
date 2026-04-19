
-- Q01: Desembolso total por día, ciudad y segmento
-- La vista ya agrega por origination_date/city/segment/product_type
-- Sumamos filas en caso de que haya más de un registro por combinación

-- Desembolsos x Dia

SELECT
    origination_date AS fecha_desembolso,
    SUM(monto_desembolsado) AS monto_total_COP
FROM test-ceiba-col.analytics.vw_originacion_diaria
GROUP BY origination_date
ORDER BY origination_date ASC;


-- Desembolsos x Ciudad

SELECT
    city,
    SUM(monto_desembolsado) AS monto_total_COP
FROM test-ceiba-col.analytics.vw_originacion_diaria
GROUP BY city
ORDER BY 2 DESC;

-- Desembolsos x Segmento

SELECT
    segment,
    SUM(monto_desembolsado) AS monto_total_COP
FROM test-ceiba-col.analytics.vw_originacion_diaria
GROUP BY segment
ORDER BY 2 DESC


-- Desembolsos x Fecha,Ciudad,Segmento

SELECT
    origination_date AS fecha_desembolso,
    city AS ciudad,
    segment AS segmento,
    SUM(monto_desembolsado) AS monto_total_COP
FROM test-ceiba-col.analytics.vw_originacion_diaria
GROUP BY origination_date, city, segment
ORDER BY origination_date DESC, monto_total_COP DESC;
