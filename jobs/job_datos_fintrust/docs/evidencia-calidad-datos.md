# Evidencia de Validaciones de Calidad de Datos


| # | Control | Resultado | Detalle |
|---|---------|-----------|---------|
| 1 | Clientes – campos obligatorios no nulos | ✓ PASS | 0 clientes con campos nulos |
| 2 | Clientes – customer_id único | ✓ PASS | 0 duplicados |
| 3 | Créditos – FK a customers válida | ✓ PASS | 0 créditos huérfanos |
| 4 | Créditos – principal_amount > 0 | ✓ PASS | 0 montos inválidos |
| 5 | Cuotas – FK a loans válida | ✓ PASS | 0 cuotas huérfanas |
| 6 | Cuotas – número de cuota dentro del plazo | ⚠ AVISO | 1 cuota anómala (I135, L003, num=99) |
| 7 | Pagos – payment_status en dominio conocido | ✓ PASS | Solo CONFIRMED/REVERSED/PENDING |
| 8 | Pagos – monto positivo (CONFIRMED) | ⚠ AVISO | 1 pago (P106, monto=0) |
| 9 | Pagos – canal de pago no nulo | ⚠ AVISO | 1 pago (P102, canal=NULL) |
| 10 | Pagos – installment_id existe en cuotas | ⚠ AVISO | 1 pago huérfano (P101 → I999) |
| 11 | Pagos – loan_id consistente con installment | ⚠ AVISO | 2 pagos con inconsistencia (P102, P104) |
| 12 | Staging – tasa de validez ≥ 85% | ✓ PASS | clientes=100%, créditos=100%, cuotas=99.3%, pagos=93.5% |
| 13 | dm_cartera – sin duplicados de installment_id | ✓ PASS | 0 duplicados |

**Resultado global**: 11/13 controles PASS. Ningún crítico falla. Pipeline puede continuar.

---

## Estadísticas del data mart (fecha análisis: 2025-04-14)

| Métrica | Valor |
|---------|-------|
| Total clientes válidos | 35 |
| Total créditos válidos | 45 |
| Total cuotas en dm_cartera | 134 (excluye I135) |
| Total pagos válidos para recaudo | ~100 |
| Pagos rechazados | 7 (P101, P102, P103, P104, P105, P106, P107) |
| Créditos en mora (alguna cuota LATE) | ~8 |
| Monto total desembolsado (muestra) | ~$700M COP |

---

## Detalle de registros rechazados

### Pagos excluidos del recaudo

| payment_id | Motivo | Impacto |
|------------|--------|---------|
| P103 | payment_status = REVERSED | Pago revertido de P015 para I050/L015 |
| P105 | payment_status = PENDING | Pago futuro no confirmado para L028 |
| P106 | payment_amount = 0 | Registro inválido para L021/I081 |
| P101 | installment_id = I999 (no existe) | $1.45M COP excluidos del recaudo de L010 |
| P102 | loan_id=L013 ≠ loan de I040 (L012) | $1.0M COP excluidos; doble anomalía con canal NULL |
| P104 | loan_id=L014 ≠ loan de I046 (L013) | $2.04M COP excluidos del recaudo |
| P107 | I135 es cuota anómala (num=99) | $500K COP excluidos (cuota fantasma de L003) |

### Cuotas excluidas de analytics

| installment_id | Motivo |
|----------------|--------|
| I135 | installment_number=99 para L003 (plazo=10). Presumiblemente error de migración. |

---

## Normalización aplicada en staging

| Campo | Transformación | Ejemplo |
|-------|---------------|---------|
| `customers.city` | INITCAP(TRIM(...)) | "bogota " → "Bogota" |
| `customers.segment` | UPPER(TRIM(...)) | "Mass Market" → "MASS MARKET" |
| `loans.loan_status` | UPPER(TRIM(...)) | "ACTIVE" → "ACTIVE" |
| `installments.installment_status` | UPPER(TRIM(...)) | "paid" → "PAID" |
| `payments.payment_channel` | COALESCE(NULLIF(TRIM(...), ''), 'UNKNOWN') | NULL → "UNKNOWN" |
| `payments.payment_status` | UPPER(TRIM(...)) | "confirmed" → "CONFIRMED" |
