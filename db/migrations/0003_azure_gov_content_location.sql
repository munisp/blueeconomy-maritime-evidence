BEGIN;

-- Azure Government posture: new content_location values must use https or
-- abfs (s3 remains representable for approved legacy migrations and is
-- application-enforced off unless EVIDENCE_ALLOW_LEGACY_S3=true).
ALTER TABLE evidence_packages
    ADD CONSTRAINT evidence_packages_content_location_scheme
    CHECK (content_location ~ '^(https|s3|abfs)://') NOT VALID;

COMMIT;
