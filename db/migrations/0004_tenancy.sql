BEGIN;

-- H2 tenancy enforcement: every evidence package is bound to the creating
-- principal's tenant; idempotency keys are scoped per tenant instead of a
-- single global namespace; validation_status is persisted on the package row
-- through a narrowed immutability trigger.

-- 1. Replace the blanket immutability trigger first so the migration itself
--    can backfill, then re-assert it with a single sanctioned transition:
--    validation_status received -> validated|rejected (recorded by
--    Store.RecordValidation inside the validation transaction).
DROP TRIGGER evidence_packages_immutable ON evidence_packages;

ALTER TABLE evidence_packages
    ADD COLUMN tenant_id TEXT NOT NULL DEFAULT '';

-- Legacy rows predate tenancy; park them under a dedicated legacy tenant so
-- no live principal (whose tenant binding is always non-empty and
-- issuer-scoped) can claim them by guessing an empty tenant.
UPDATE evidence_packages SET tenant_id = 'legacy-unmigrated' WHERE tenant_id = '';

ALTER TABLE evidence_packages ALTER COLUMN tenant_id DROP DEFAULT;
ALTER TABLE evidence_packages
    ADD CONSTRAINT evidence_packages_tenant_not_blank CHECK (length(trim(tenant_id)) > 0);

-- Idempotency keys collide cross-tenant by design of the callers; the
-- uniqueness boundary moves to (tenant_id, idempotency_key).
ALTER TABLE evidence_packages DROP CONSTRAINT evidence_packages_idempotency_key_key;
ALTER TABLE evidence_packages
    ADD CONSTRAINT evidence_packages_tenant_idempotency_unique UNIQUE (tenant_id, idempotency_key);

DROP INDEX evidence_packages_received_at_index;
CREATE INDEX evidence_packages_tenant_received_at_index ON evidence_packages (tenant_id, received_at DESC);

-- 2. Narrowed immutability: the only legal mutation is the terminal
--    validation_status transition written by RecordValidation; every other
--    UPDATE and every DELETE still raises.
CREATE OR REPLACE FUNCTION prevent_evidence_package_mutation() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.validation_status = 'received'
       AND NEW.validation_status IN ('validated', 'rejected')
       AND NEW.evidence_package_id = OLD.evidence_package_id
       AND NEW.idempotency_key = OLD.idempotency_key
       AND NEW.tenant_id = OLD.tenant_id
       AND NEW.external_reference = OLD.external_reference
       AND NEW.evidence_type = OLD.evidence_type
       AND NEW.content_sha256 = OLD.content_sha256
       AND NEW.content_location = OLD.content_location
       AND NEW.received_at = OLD.received_at
       AND NEW.classification = OLD.classification
       AND NEW.correlation_id = OLD.correlation_id
       AND NEW.created_at = OLD.created_at THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'evidence_packages rows are immutable except the terminal validation_status transition; create validation history instead';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER evidence_packages_immutable
    BEFORE UPDATE OR DELETE ON evidence_packages
    FOR EACH ROW EXECUTE FUNCTION prevent_evidence_package_mutation();

COMMIT;
