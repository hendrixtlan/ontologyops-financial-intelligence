-- Run after: ontologyops render-sql > /tmp/metric_view.sql
-- Execute generated DDL in a Databricks SQL editor/warehouse.
-- This uses MEASURE() semantics, not standard SELECT * from a metric view.
SELECT
    business_date,
    corridor,
    acquirer_id,
    currency,
    MEASURE(failed_settlements) AS failed,
    MEASURE(settled_settlements) AS settled,
    MEASURE(attempted_settlements) AS terminal_attempts,
    MEASURE(settlement_failure_rate) AS settlement_failure_rate,
    MEASURE(failed_amount_minor) AS failed_exposure_minor
FROM main.ontologyops.payment_health
GROUP BY business_date, corridor, acquirer_id, currency
ORDER BY business_date, corridor, acquirer_id, currency;
