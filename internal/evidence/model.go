package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"
)

const (
	StatusReceived  = "received"
	StatusValidated = "validated"
	StatusRejected  = "rejected"
)

var validClassifications = map[string]struct{}{
	"public": {}, "internal": {}, "confidential": {}, "restricted": {}, "highly_restricted": {},
}

// CreateRequest contains metadata and a digest/reference to externally retained evidence.
// Raw evidence content is intentionally outside this service boundary.
type CreateRequest struct {
	IdempotencyKey    string    `json:"idempotency_key"`
	ExternalReference string    `json:"external_reference"`
	EvidenceType      string    `json:"evidence_type"`
	ContentSHA256     string    `json:"content_sha256"`
	ContentLocation   string    `json:"content_location"`
	ReceivedAt        time.Time `json:"received_at"`
	Classification    string    `json:"classification"`
	CorrelationID     string    `json:"correlation_id"`
}

type Package struct {
	EvidencePackageID string    `json:"evidence_package_id"`
	IdempotencyKey    string    `json:"idempotency_key"`
	ExternalReference string    `json:"external_reference"`
	EvidenceType      string    `json:"evidence_type"`
	ContentSHA256     string    `json:"content_sha256"`
	ContentLocation   string    `json:"content_location"`
	ReceivedAt        time.Time `json:"received_at"`
	Classification    string    `json:"classification"`
	CorrelationID     string    `json:"correlation_id"`
	CreatedAt         time.Time `json:"created_at"`
	ValidationStatus  string    `json:"validation_status"`
}

type ValidationRequest struct {
	ValidationStatus      string    `json:"validation_status"`
	ReasonCode            string    `json:"reason_code"`
	ActorSubjectReference string    `json:"actor_subject_reference"`
	OccurredAt            time.Time `json:"occurred_at"`
	CorrelationID         string    `json:"correlation_id"`
}

func DecodeCreateRequest(body []byte) (CreateRequest, error) {
	var request CreateRequest
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return CreateRequest{}, fmt.Errorf("decode evidence package request: %w", err)
	}
	if err := request.Validate(); err != nil {
		return CreateRequest{}, err
	}
	return request, nil
}

func (r CreateRequest) Validate() error {
	if !isUUID(r.IdempotencyKey) {
		return errors.New("idempotency_key must be a UUID")
	}
	if !isUUID(r.CorrelationID) {
		return errors.New("correlation_id must be a UUID")
	}
	if strings.TrimSpace(r.ExternalReference) == "" || len(r.ExternalReference) > 512 {
		return errors.New("external_reference is required and must not exceed 512 characters")
	}
	if strings.TrimSpace(r.EvidenceType) == "" || len(r.EvidenceType) > 128 {
		return errors.New("evidence_type is required and must not exceed 128 characters")
	}
	if !validSHA256(r.ContentSHA256) {
		return errors.New("content_sha256 must be a lower-case SHA-256 hexadecimal digest")
	}
	if err := validateContentLocation(r.ContentLocation); err != nil {
		return err
	}
	if r.ReceivedAt.IsZero() {
		return errors.New("received_at is required")
	}
	if _, found := validClassifications[r.Classification]; !found {
		return errors.New("classification is not an approved value")
	}
	return nil
}

func (r ValidationRequest) Validate() error {
	if r.ValidationStatus != StatusValidated && r.ValidationStatus != StatusRejected {
		return errors.New("validation_status must be validated or rejected")
	}
	if strings.TrimSpace(r.ReasonCode) == "" || len(r.ReasonCode) > 128 {
		return errors.New("reason_code is required and must not exceed 128 characters")
	}
	if strings.TrimSpace(r.ActorSubjectReference) == "" || len(r.ActorSubjectReference) > 512 {
		return errors.New("actor_subject_reference is required and must not exceed 512 characters")
	}
	if r.OccurredAt.IsZero() {
		return errors.New("occurred_at is required")
	}
	if !isUUID(r.CorrelationID) {
		return errors.New("correlation_id must be a UUID")
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}

func validateContentLocation(value string) error {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("content_location must be an absolute object-storage or HTTPS URL")
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("content_location must not contain credentials, query parameters or fragments")
	}
	if parsed.Scheme != "https" && parsed.Scheme != "s3" {
		return errors.New("content_location must use https or s3")
	}
	return nil
}

func isUUID(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if !((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') || (character >= 'A' && character <= 'F')) {
			return false
		}
	}
	return true
}
