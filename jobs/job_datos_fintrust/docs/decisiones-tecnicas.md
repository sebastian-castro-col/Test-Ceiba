# Decisiones Técnicas, Supuestos y Riesgos

## Motor de base de datos: BigQuery GCP (Personal Sebastian Castro)
## Repositorio Github (Personal Sebastian Castro)
## Despliegue Continuo Github Actions

---

## Arquitectura de capas (medaillón simplificado)

```
raw_fintrust  →  staging  →  analytics
(fuente)          (limpieza)   (data mart + vistas BI)
```

- **raw_fintrust**: datos tal como llegan, sin modificaciones. Preservar integridad histórica.
- **staging**: validación, normalización y enriquecimiento. Mantiene todos los registros con flags `is_valid` e `is_anomalous`.
- **analytics**: solo datos válidos. Tablas materializadas para performance en BI.

---

## Supuestos de reglas de negocio

| # | Supuesto | Impacto |
|---|----------|---------|
| 1 | Solo pagos `CONFIRMED` con `payment_amount > 0` se contabilizan como recaudo | Excluye P103 (REVERSED), P105 (PENDING), P106 (monto=0) |
| 2 | Un pago es "aplicado a mora" si la cuota referenciada tiene `due_date < payment_date` o `installment_status = 'LATE'` | Puede diferir del criterio contable real; consultar con tesorería |
| 3 | `days_overdue` se calcula respecto a la fecha de análisis `2025-04-14` (fecha máxima de los datos de muestra) | En producción se usa `CURRENT_DATE` o un parámetro del orquestador |
| 4 | `balance_due` de una cuota = `total_due - total_paid`, con floor en 0 | Pagos excesivos (overpayment) se ignoran para la cuota individual |
| 5 | Cuotas `PARTIAL` siguen siendo deuda vigente (se aplica el saldo no cubierto) | Verificar si el sistema operativo trata PARTIAL como DUE o PAID |
| 6 | Créditos `CLOSED` pueden tener cuotas sin pagar (dato de test); en producción todos deberían estar pagadas | Incluir créditos CLOSED en el data mart para no perder historia |

---

## Anomalías de datos detectadas

| ID Registro | Tipo de anomalía | Tratamiento |
|-------------|-----------------|-------------|
| I135 | `installment_number=99` para L003 (plazo=10 meses) | Excluida de staging y analytics (`is_anomalous=TRUE`) |
| P101 | `installment_id='I999'` no existe | Excluido del recaudo (`is_orphan=TRUE`) |
| P102 | `loan_id='L013'` pero `I040` pertenece a L012; canal NULL | Excluido (`is_loan_mismatch=TRUE`); canal mapeado a 'UNKNOWN' si se incluyera |
| P103 | `payment_status='REVERSED'` | Excluido del recaudo |
| P104 | `loan_id='L014'` pero `I046` pertenece a L013 | Excluido (`is_loan_mismatch=TRUE`) |
| P105 | `payment_status='PENDING'`, fecha futura | Excluido del recaudo |
| P106 | `payment_amount=0` | Excluido del recaudo |
| P107 | Pago de `I135` (cuota fantasma) | Excluido indirectamente (I135 es anomalous, no entra a stg_installments válidos) |

---

## Estrategia de incrementalidad

**Implementación actual (simulada)**:
- La tabla `raw_fintrust.payments` tiene columna `loaded_at TIMESTAMP`.
- La tabla `raw_fintrust._pipeline_watermarks` guarda el último `loaded_at` procesado.
- En cada ejecución, el pipeline lee el watermark y reporta pagos nuevos.
- La reconstrucción completa de `dm_cartera` se hace con `CREATE OR REPLACE TABLE` (full refresh del data mart).

**En producción BigQuery recomendaría**:
```sql
-- Carga incremental de staging con MERGE
MERGE staging.stg_payments AS target
USING (
    SELECT * FROM raw_fintrust.payments
    WHERE loaded_at > @last_watermark
) AS source
ON target.payment_id = source.payment_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```
- `dm_cartera`: reconstruir solo particiones afectadas (BigQuery soporta partición por `due_date` o `cohort_month`).
- Orquestador sugerido: Cloud Run JOB GCP diario a las 6am.

---

## Manejo de errores y monitoreo

| Escenario | Manejo actual | En producción |
|-----------|--------------|---------------|
| Script SQL falla | Pipeline se detiene, error logueado | Retry con backoff exponencial en Airflow |
| Validación crítica falla | Warning en log, pipeline continúa | Alerta a PagerDuty, bloquear publicación de tablero |
| DuckDB no disponible | Error al iniciar | Health check previo, fallback a lectura de caché |
| Nuevos valores en `loan_status` | Se detectan en `val_creditos_status_conocido` | Alertar a equipo de datos para actualizar dominio |

---

## Riesgos conocidos

1. **Calidad de datos en pagos**: 7 de 107 pagos (6.5%) tienen anomalías. En producción este porcentaje debería monitorearse con alertas si supera el 2%.
2. **Fechas de referencia**: los buckets de mora dependen de `analysis_date`. Si el pipeline no recibe la fecha correcta, los indicadores de mora serán incorrectos.
3. **Granularidad de recaudo**: el modelo actual no permite multi-aplicación de un pago a varias cuotas. Si el sistema origen permite "pagos cruzados", se necesita una tabla de detalle de aplicación.
4. **Sin historial de estados**: la tabla `installments` solo guarda el estado actual. Para análisis de roll-rate (evolución de mora en el tiempo) se necesita una tabla de historial de estados.
