BEGIN;

-- Approved legacy s3 re-registration path (runbook: docs/legacy-s3-migration.md).
--
-- evidence_packages remains fully immutable: the evidence_packages_immutable
-- trigger from 0001_evidence.sql is intentionally left untouched. Supersession
-- of a legacy s3 package by its re-registered abfs replacement is recorded in
-- this append-only side table, so the re-registration path never issues an
-- UPDATE against evidence_packages and cannot violate the immutability
-- invariant. The PRIMARY KEY doubles as the idempotency guard: a package can
-- be superseded exactly once, which makes an interrupted migration safely
-- resumable (already-superseded rows are skipped).
CREATE TABLE evidence_package_supersession (
    evidence_package_id UUID PRIMARY KEY
        REFERENCES evidence_packages(evidence_package_id) ON DELETE RESTRICT,
    superseded_by UUID NOT NULL
        REFERENCES evidence_packages(evidence_package_id) ON DELETE RESTRICT,
    reason_code TEXT NOT NULL,
    actor_subject_reference TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    correlation_id UUID NOT NULL,
    CONSTRAINT evidence_package_supersession_no_self
        CHECK (evidence_package_id <> superseded_by),
    CONSTRAINT evidence_package_supersession_reason_not_blank
        CHECK (length(trim(reason_code)) > 0),
    CONSTRAINT evidence_package_supersession_actor_not_blank
        CHECK (length(trim(actor_subject_reference)) > 0)
);

-- Reverse lookup: given a replacement package, find what it supersedes.
CREATE INDEX evidence_package_supersession_replacement_index
    ON evidence_package_supersession (superseded_by);

-- Supersession is a one-way door: rows are append-only, mirroring the
-- evidence_packages immutability posture.
CREATE FUNCTION prevent_evidence_supersession_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'evidence_package_supersession rows are immutable; supersession is a one-way door';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evidence_package_supersession_immutable
    BEFORE UPDATE OR DELETE ON evidence_package_supersession
    FOR EACH ROW EXECUTE FUNCTION prevent_evidence_supersession_mutation();

COMMIT;
