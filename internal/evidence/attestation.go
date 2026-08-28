package evidence

import (
	"errors"
	"fmt"
	"time"

	"github.com/munisp/blueeconomy-maritime-evidence/internal/provenance"
)

// SigningKeyID is the provenance key id this service signs attestations
// with; consumers resolve the matching public key from the fleet key
// directory.
const SigningKeyID = "maritime-evidence-1"

// ProducerName identifies this service in every signed artifact.
const ProducerName = "maritime-evidence"

// ReregistrationAttestationSchema versions the signed re-registration
// attestation document.
const ReregistrationAttestationSchema = "blueeconomy.maritime-evidence.reregistration-attestation.v1"

// ReregistrationAttestation is the signed audit artifact emitted for every
// completed legacy evidence re-registration. provenance.signature is the
// fleet scheme: a JWS compact serialization (EdDSA/Ed25519) over the
// JCS-canonicalized (RFC 8785) JSON of the full attestation excluding the
// signature field.
type ReregistrationAttestation struct {
	SchemaVersion        string `json:"schemaVersion"`
	Producer             string `json:"producer"`
	LegacyPackageID      string `json:"legacyPackageId"`
	ReplacementPackageID string `json:"replacementPackageId"`
	TargetLocation       string `json:"targetLocation"`
	Actor                string `json:"actor"`
	CorrelationID        string `json:"correlationId"`
	OccurredAt           string `json:"occurredAt"`
	Provenance           struct {
		Signature string `json:"signature"`
	} `json:"provenance"`
}

// SignReregistrationAttestation builds and seals the attestation for one
// completed re-registration. It fails closed on a missing signer or invalid
// identifiers; a re-registration without its attestation is incomplete.
func SignReregistrationAttestation(signer *provenance.Signer, legacyPackageID, replacementPackageID, targetLocation, actor, correlationID string, occurredAt time.Time) (ReregistrationAttestation, error) {
	if signer == nil {
		return ReregistrationAttestation{}, errors.New("provenance signer is required")
	}
	if legacyPackageID == "" || replacementPackageID == "" || targetLocation == "" || actor == "" || correlationID == "" {
		return ReregistrationAttestation{}, errors.New("attestation identifiers are required")
	}
	if occurredAt.IsZero() {
		return ReregistrationAttestation{}, errors.New("attestation timestamp is required")
	}
	attestation := ReregistrationAttestation{
		SchemaVersion:        ReregistrationAttestationSchema,
		Producer:             ProducerName,
		LegacyPackageID:      legacyPackageID,
		ReplacementPackageID: replacementPackageID,
		TargetLocation:       targetLocation,
		Actor:                actor,
		CorrelationID:        correlationID,
		OccurredAt:           occurredAt.UTC().Format(time.RFC3339),
	}
	signature, err := signer.SignEnvelope(attestation)
	if err != nil {
		return ReregistrationAttestation{}, fmt.Errorf("sign re-registration attestation: %w", err)
	}
	attestation.Provenance.Signature = signature
	return attestation, nil
}
