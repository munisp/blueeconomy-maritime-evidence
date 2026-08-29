package evidence

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// ReasonLegacyS3ReRegistration is the validation-history and supersession
// reason code used by the approved legacy s3 re-registration path.
const ReasonLegacyS3ReRegistration = "legacy_s3_reregistration"

// ErrAlreadySuperseded reports that a legacy package already has a
// supersession record; callers treat it as a completed unit of work and skip
// the package (resumable migration).
var ErrAlreadySuperseded = errors.New("evidence package already superseded")

// LegacyS3Package is one stored legacy s3 package and whether an approved
// replacement already supersedes it.
type LegacyS3Package struct {
	Package         Package
	Superseded      bool
	SupersededBy    string
	TargetLocation  string
	RelocationError string
}

// LegacyS3Plan is the planned re-registration of one legacy s3 package: the
// same evidentiary content re-registered under an abfs content_location.
type LegacyS3Plan struct {
	LegacyPackageID string
	IdempotencyKey  string
	TargetLocation  string
}

// PlanLegacyS3Relocation maps a legacy s3 content_location onto the approved
// abfs target prefix as <prefix>/<bucket>/<key> and validates the result
// against the default Azure Government location policy (s3 rejected, abfs
// endpoint rules enforced). The prefix must itself be a valid abfs location
// naming a base directory. Any deviation fails closed: no silent rewrite.
func PlanLegacyS3Relocation(sourceLocation, targetPrefix string) (string, error) {
	parsedSource, err := url.ParseRequestURI(sourceLocation)
	if err != nil || parsedSource.Scheme != "s3" || parsedSource.Host == "" {
		return "", fmt.Errorf("legacy content_location %q is not an absolute s3://bucket/key URL", sourceLocation)
	}
	if parsedSource.Path == "" || parsedSource.Path == "/" {
		return "", fmt.Errorf("legacy content_location %q has no object key", sourceLocation)
	}
	bucket := parsedSource.Host
	if !isLowerDNSLabel(bucket) && !isDottedDNSName(bucket) {
		return "", fmt.Errorf("legacy bucket name %q is not a valid DNS name", bucket)
	}

	parsedPrefix, err := url.ParseRequestURI(targetPrefix)
	if err != nil || parsedPrefix.Scheme != "abfs" {
		return "", fmt.Errorf("target prefix %q is not an absolute abfs URL", targetPrefix)
	}
	if err := validateABFSLocation(parsedPrefix); err != nil {
		return "", fmt.Errorf("target prefix rejected by the abfs location policy: %w", err)
	}

	target := strings.TrimSuffix(targetPrefix, "/") + "/" + bucket + parsedSource.EscapedPath()
	if err := validateContentLocation(target, LocationPolicy{}); err != nil {
		return "", fmt.Errorf("planned target location %q is not acceptable: %w", target, err)
	}
	return target, nil
}

func isDottedDNSName(value string) bool {
	if value == "" || len(value) > 253 {
		return false
	}
	for _, label := range strings.Split(value, ".") {
		if !isLowerDNSLabel(label) {
			return false
		}
	}
	return true
}

// NewMigrationCorrelationID returns a random version-4 UUID used to correlate
// one migration run across validation history and supersession rows.
func NewMigrationCorrelationID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate migration correlation id: %w", err)
	}
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}

// LegacyMigration executes the approved legacy s3 re-registration path
// against PostgreSQL. Every mutation runs inside one transaction per package:
// the replacement package insert, its initial validation history row and the
// supersession record commit together or not at all.
type LegacyMigration struct {
	pool *pgxpool.Pool
}

func NewLegacyMigration(pool *pgxpool.Pool) *LegacyMigration {
	return &LegacyMigration{pool: pool}
}

// ListLegacyS3Packages returns every stored package whose content_location
// uses the deprecated s3 scheme, oldest first, annotated with its supersession
// state and (when a target prefix is supplied) the planned abfs relocation.
func (m *LegacyMigration) ListLegacyS3Packages(ctx context.Context, targetPrefix string) ([]LegacyS3Package, error) {
	rows, err := m.pool.Query(ctx, `
		SELECT p.evidence_package_id, p.idempotency_key, p.external_reference, p.evidence_type,
		       p.content_sha256, p.content_location, p.received_at, p.classification, p.correlation_id,
		       p.created_at, p.validation_status,
		       s.superseded_by
		FROM evidence_packages p
		LEFT JOIN evidence_package_supersession s
		       ON s.evidence_package_id = p.evidence_package_id
		WHERE p.content_location LIKE 's3://%'
		ORDER BY p.created_at ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list legacy s3 evidence packages: %w", err)
	}
	defer rows.Close()

	var packages []LegacyS3Package
	for rows.Next() {
		var legacy LegacyS3Package
		var supersededBy *string
		if err := scanPackage(rows, &legacy.Package, &supersededBy); err != nil {
			return nil, fmt.Errorf("scan legacy s3 evidence package: %w", err)
		}
		if supersededBy != nil {
			legacy.Superseded = true
			legacy.SupersededBy = *supersededBy
		}
		if targetPrefix != "" {
			target, planErr := PlanLegacyS3Relocation(legacy.Package.ContentLocation, targetPrefix)
			if planErr != nil {
				legacy.RelocationError = planErr.Error()
			} else {
				legacy.TargetLocation = target
			}
		}
		packages = append(packages, legacy)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy s3 evidence packages: %w", err)
	}
	return packages, nil
}

// RegisterReplacement atomically registers the verified abfs replacement for
// one legacy package and records the supersession link. The legacy row itself
// is never updated: evidence_packages stays immutable and old readers keep
// working. Content copy and SHA-256 verification must already have succeeded
// (see StagedCommandCopier.CopyAndVerify); this function performs no object
// store I/O.
//
// occurredAt is the re-registration instant recorded in validation history;
// the replacement row retains the legacy received_at because the evidentiary
// content and its receipt are unchanged.
// RegisterReplacement re-registers one legacy s3 package at its verified
// target location and supersedes the legacy row — the chain-of-custody
// operation of the legacy migration runbook. Traced as a custody operation:
// the span carries the correlation id only, never package or actor identity.
func (m *LegacyMigration) RegisterReplacement(
	ctx context.Context,
	legacy Package,
	targetLocation string,
	actorSubjectReference string,
	correlationID string,
	occurredAt time.Time,
) (replacementPackage Package, err error) {
	ctx, span := custodyTracer().Start(ctx, "evidence.legacy.register_replacement",
		trace.WithAttributes(attribute.String("evidence.correlation_id", correlationID)))
	defer func() { endCustodySpan(span, err) }()
	if strings.TrimSpace(actorSubjectReference) == "" {
		return Package{}, errors.New("actor subject reference is required for re-registration")
	}
	if !isUUID(correlationID) {
		return Package{}, errors.New("correlation id must be a UUID")
	}
	if !strings.HasPrefix(legacy.ContentLocation, "s3://") {
		return Package{}, fmt.Errorf("package %s is not a legacy s3 package", legacy.EvidencePackageID)
	}
	// The replacement is validated against the default policy: the deprecated
	// s3 scheme is not acceptable for the new row even when the operator flag
	// is set for reading legacy data.
	replacement := CreateRequest{
		IdempotencyKey:    "00000000-0000-4000-8000-000000000000", // replaced server-side
		ExternalReference: legacy.ExternalReference,
		EvidenceType:      legacy.EvidenceType,
		ContentSHA256:     legacy.ContentSHA256,
		ContentLocation:   targetLocation,
		ReceivedAt:        legacy.ReceivedAt,
		Classification:    legacy.Classification,
		CorrelationID:     correlationID,
	}
	if err := replacement.ValidateWithPolicy(LocationPolicy{}); err != nil {
		return Package{}, fmt.Errorf("replacement package metadata rejected: %w", err)
	}

	transaction, err := m.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Package{}, fmt.Errorf("begin re-registration transaction: %w", err)
	}
	defer func() { _ = transaction.Rollback(ctx) }()

	var stillS3 bool
	err = transaction.QueryRow(ctx, `
		SELECT p.content_location LIKE 's3://%'
		FROM evidence_packages p
		WHERE p.evidence_package_id = $1
		FOR KEY SHARE
	`, legacy.EvidencePackageID).Scan(&stillS3)
	if errors.Is(err, pgx.ErrNoRows) {
		return Package{}, ErrNotFound
	}
	if err != nil {
		return Package{}, fmt.Errorf("lock legacy evidence package: %w", err)
	}
	if !stillS3 {
		return Package{}, fmt.Errorf("package %s no longer uses the legacy s3 scheme", legacy.EvidencePackageID)
	}

	var alreadySuperseded bool
	err = transaction.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM evidence_package_supersession WHERE evidence_package_id = $1
		)
	`, legacy.EvidencePackageID).Scan(&alreadySuperseded)
	if err != nil {
		return Package{}, fmt.Errorf("check supersession state: %w", err)
	}
	if alreadySuperseded {
		return Package{}, ErrAlreadySuperseded
	}

	var created Package
	err = scanPackage(transaction.QueryRow(ctx, `
		INSERT INTO evidence_packages (
			evidence_package_id, idempotency_key, external_reference, evidence_type,
			content_sha256, content_location, received_at, validation_status, classification, correlation_id
		) VALUES (gen_random_uuid(), gen_random_uuid(), $1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING evidence_package_id, idempotency_key, external_reference, evidence_type,
		          content_sha256, content_location, received_at, classification, correlation_id,
		          created_at, validation_status
	`, replacement.ExternalReference, replacement.EvidenceType, replacement.ContentSHA256,
		replacement.ContentLocation, replacement.ReceivedAt.UTC(), StatusReceived,
		replacement.Classification, replacement.CorrelationID), &created)
	if err != nil {
		return Package{}, fmt.Errorf("register replacement evidence package: %w", err)
	}

	_, err = transaction.Exec(ctx, `
		INSERT INTO evidence_validation_history (
			validation_history_id, evidence_package_id, prior_validation_status, validation_status,
			reason_code, actor_subject_reference, occurred_at, correlation_id
		) VALUES (gen_random_uuid(), $1, NULL, $2, $3, $4, $5, $6)
	`, created.EvidencePackageID, StatusReceived, ReasonLegacyS3ReRegistration,
		actorSubjectReference, occurredAt.UTC(), correlationID)
	if err != nil {
		return Package{}, fmt.Errorf("record replacement validation history: %w", err)
	}

	_, err = transaction.Exec(ctx, `
		INSERT INTO evidence_package_supersession (
			evidence_package_id, superseded_by, reason_code,
			actor_subject_reference, occurred_at, correlation_id
		) VALUES ($1, $2, $3, $4, $5, $6)
	`, legacy.EvidencePackageID, created.EvidencePackageID, ReasonLegacyS3ReRegistration,
		actorSubjectReference, occurredAt.UTC(), correlationID)
	if err != nil {
		return Package{}, fmt.Errorf("record supersession link: %w", err)
	}

	if err := transaction.Commit(ctx); err != nil {
		return Package{}, fmt.Errorf("commit re-registration transaction: %w", err)
	}
	return created, nil
}
