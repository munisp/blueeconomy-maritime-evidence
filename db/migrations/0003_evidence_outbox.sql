BEGIN;

-- Transactional outbox for the evidence event publisher. Rows are appended
-- inside the same transaction that commits the package creation or the
-- validation history entry; cmd/evidence-outbox-publisher drains them to
-- Kafka (evidence.package.v1, evidence.validation.v1) at-least-once and
-- marks published_at only after an all-ISR acknowledgement.

CREATE TABLE evidence_outbox (
    event_id UUID PRIMARY KEY,
    evidence_package_id UUID NOT NULL REFERENCES evidence_packages(evidence_package_id) ON DELETE RESTRICT,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CONSTRAINT evidence_outbox_topic_allowed CHECK (topic IN ('evidence.package.v1', 'evidence.validation.v1')),
    CONSTRAINT evidence_outbox_event_type_allowed CHECK (event_type IN ('evidence.package.received', 'evidence.validation.recorded'))
);

CREATE INDEX evidence_outbox_unpublished_idx
    ON evidence_outbox (created_at) WHERE published_at IS NULL;

-- Fail-closed row-level security, consistent with the default-deny posture
-- of 0001: only sessions that assert the approved evidence service context
-- (SET app.evidence_service = 'on', applied by evidence.ConfigurePool on
-- every pooled connection) may read or append outbox rows. Every other
-- session, including the table owner, is denied.
ALTER TABLE evidence_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE evidence_outbox FORCE ROW LEVEL SECURITY;

CREATE POLICY evidence_outbox_service_policy ON evidence_outbox
    USING (current_setting('app.evidence_service', true) = 'on')
    WITH CHECK (current_setting('app.evidence_service', true) = 'on');

COMMIT;
