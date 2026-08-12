BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE evidence_packages (
    evidence_package_id UUID PRIMARY KEY,
    idempotency_key UUID NOT NULL UNIQUE,
    external_reference TEXT NOT NULL,
    evidence_type TEXT NOT NULL,
    content_sha256 CHAR(64) NOT NULL,
    content_location TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    validation_status TEXT NOT NULL,
    classification TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT evidence_packages_external_reference_not_blank CHECK (length(trim(external_reference)) > 0),
    CONSTRAINT evidence_packages_evidence_type_not_blank CHECK (length(trim(evidence_type)) > 0),
    CONSTRAINT evidence_packages_content_location_not_blank CHECK (length(trim(content_location)) > 0),
    CONSTRAINT evidence_packages_sha256_format CHECK (content_sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT evidence_packages_status_allowed CHECK (validation_status IN ('received', 'validated', 'rejected')),
    CONSTRAINT evidence_packages_classification_allowed CHECK (classification IN ('public', 'internal', 'confidential', 'restricted', 'highly_restricted'))
);

CREATE INDEX evidence_packages_external_reference_index ON evidence_packages (external_reference);
CREATE INDEX evidence_packages_received_at_index ON evidence_packages (received_at DESC);

CREATE TABLE evidence_validation_history (
    validation_history_id UUID PRIMARY KEY,
    evidence_package_id UUID NOT NULL REFERENCES evidence_packages(evidence_package_id) ON DELETE RESTRICT,
    prior_validation_status TEXT,
    validation_status TEXT NOT NULL,
    reason_code TEXT NOT NULL,
    actor_subject_reference TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    CONSTRAINT evidence_validation_history_status_allowed CHECK (validation_status IN ('received', 'validated', 'rejected')),
    CONSTRAINT evidence_validation_history_reason_code_not_blank CHECK (length(trim(reason_code)) > 0),
    CONSTRAINT evidence_validation_history_actor_not_blank CHECK (length(trim(actor_subject_reference)) > 0)
);

CREATE INDEX evidence_validation_history_package_time_index
    ON evidence_validation_history (evidence_package_id, occurred_at ASC);

CREATE FUNCTION prevent_evidence_package_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'evidence_packages rows are immutable; create validation history instead';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evidence_packages_immutable
    BEFORE UPDATE OR DELETE ON evidence_packages
    FOR EACH ROW EXECUTE FUNCTION prevent_evidence_package_mutation();

COMMIT;
