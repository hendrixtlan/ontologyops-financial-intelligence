-- LOCAL DEMO ONLY. Production requires least-privilege replication identities.
CREATE TABLE IF NOT EXISTS public.payment_events (
  event_id TEXT PRIMARY KEY,
  transaction_id TEXT NOT NULL,
  merchant_id TEXT NOT NULL,
  merchant_tier TEXT NOT NULL,
  acquirer_id TEXT NOT NULL,
  corridor TEXT NOT NULL,
  currency CHAR(3) NOT NULL CHECK (currency IN ('USD', 'MXN')),
  amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
  status TEXT NOT NULL CHECK (status IN ('INITIATED','AUTHORIZED','PENDING','FAILED','SETTLED')),
  event_sequence INTEGER NOT NULL CHECK (event_sequence > 0),
  updated_at TEXT NOT NULL,
  ingested_at TEXT NOT NULL
);
-- Defense-in-depth: prevent mutation of past financial event records.
CREATE OR REPLACE FUNCTION public.prohibit_payment_event_mutation()
RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'Payment event ledger is append-only: % blocked', TG_OP;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER payment_events_immutable BEFORE UPDATE OR DELETE ON public.payment_events
FOR EACH ROW EXECUTE FUNCTION public.prohibit_payment_event_mutation();

CREATE INDEX IF NOT EXISTS idx_payment_events_tx ON public.payment_events (transaction_id, event_sequence);
-- An explicitly scoped publication prevents unexpected new tables from being captured.
CREATE PUBLICATION ontologyops_publication FOR TABLE public.payment_events;
