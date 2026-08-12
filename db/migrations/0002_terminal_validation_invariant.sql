BEGIN;

CREATE UNIQUE INDEX evidence_validation_history_one_terminal_decision
    ON evidence_validation_history (evidence_package_id)
    WHERE validation_status IN ('validated', 'rejected');

COMMIT;
