
-- Q05: Dataset listo para conectar con Power BI, Tableau o Looker

SELECT
    -- === FILTROS SUGERIDOS EN BI ===
    -- Dimensión: fecha, ciudad, segmento, tipo_producto, estado_mora
    -- Métricas: saldo_pendiente, recaudo, días_mora, porcentaje_cobertura

    -- Tiempo
    analysis_date                           AS fecha_analisis,
    origination_date                        AS fecha_desembolso,
    cohort_label                            AS cohorte_originacion,
    due_date                                AS fecha_vencimiento_cuota,
    mes_vencimiento,
    last_payment_date                       AS ultimo_pago,

    -- Geografía y demografía
    city                                    AS ciudad,
    segment                                 AS segmento,
    product_type                            AS tipo_producto,

    -- Crédito
    loan_id                                 AS id_credito,
    customer_id                             AS id_cliente,
    full_name                               AS nombre_cliente,
    loan_status                             AS estado_credito,
    loan_principal                          AS monto_desembolsado,
    ROUND(annual_rate_pct, 2)               AS tasa_ea_pct,
    term_months                             AS plazo_meses,

    -- Cuota
    installment_id                          AS id_cuota,
    installment_number                      AS numero_cuota,
    installment_status                      AS estado_cuota,
    mora_bucket                             AS bucket_mora,
    is_overdue                              AS en_mora,
    days_overdue                            AS dias_mora,

    -- Montos (COP)
    principal_due                           AS capital_programado,
    interest_due                            AS intereses_programados,
    total_due                               AS cuota_total_programada,
    total_paid                              AS total_pagado,
    balance_due                             AS saldo_pendiente,
    num_payments                            AS num_pagos_realizados,

    -- KPIs
    pct_cobrado                             AS porcentaje_cobrado,
    cuota_pagada_completa

FROM test-ceiba-col.analytics.vw_bi_completo
ORDER BY origination_date DESC, loan_id, installment_number;
