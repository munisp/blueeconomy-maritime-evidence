package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
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

// LocationPolicy controls which content_location schemes are accepted. The
// Azure Government posture accepts https and abfs (ADLS Gen2 on
// dfs.core.usgovcloudapi.net); the legacy s3 scheme is deprecated and accepted
// only under an explicit operator opt-in.
type LocationPolicy struct {
	// AllowLegacyS3 accepts s3: content locations. It must be enabled only by
	// approved legacy-data migrations, never for new evidence intake.
	AllowLegacyS3 bool
}

// EnvAllowLegacyS3 is the fail-closed operator flag for the deprecated s3
// scheme. Only the exact value "true" enables it.
const EnvAllowLegacyS3 = "EVIDENCE_ALLOW_LEGACY_S3"

// LocationPolicyFromEnv resolves the effective policy. Unset means s3 is
// rejected; an unrecognised value fails closed instead of being silently
// interpreted.
func LocationPolicyFromEnv() (LocationPolicy, error) {
	switch value := strings.TrimSpace(os.Getenv(EnvAllowLegacyS3)); value {
	case "", "false":
		return LocationPolicy{}, nil
	case "true":
		return LocationPolicy{AllowLegacyS3: true}, nil
	default:
		return LocationPolicy{}, fmt.Errorf("%s must be true or false when set", EnvAllowLegacyS3)
	}
}

func (r CreateRequest) Validate() error {
	policy, err := LocationPolicyFromEnv()
	if err != nil {
		return err
	}
	return r.ValidateWithPolicy(policy)
}

// ValidateWithPolicy validates against an explicit content-location policy.
func (r CreateRequest) ValidateWithPolicy(policy LocationPolicy) error {
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
	if err := validateContentLocation(r.ContentLocation, policy); err != nil {
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

func validateContentLocation(value string, policy LocationPolicy) error {
	parsed, err := url.ParseRequestURI(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("content_location must be an absolute object-storage or HTTPS URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return errors.New("content_location must not contain query parameters or fragments")
	}
	switch parsed.Scheme {
	case "https":
		if parsed.User != nil {
			return errors.New("content_location must not contain credentials")
		}
		return nil
	case "s3":
		if !policy.AllowLegacyS3 {
			return errors.New("content_location scheme s3 is deprecated under the Azure Government posture; set EVIDENCE_ALLOW_LEGACY_S3=true only for approved legacy migrations")
		}
		if parsed.User != nil {
			return errors.New("content_location must not contain credentials")
		}
		return nil
	case "abfs":
		return validateABFSLocation(parsed)
	default:
		return errors.New("content_location must use https or abfs (s3 is deprecated)")
	}
}

// validateABFSLocation enforces the ADLS Gen2 Azure Government form
// abfs://<filesystem>@<account>.dfs.core.usgovcloudapi.net/<object-path>.
// The userinfo position carries the filesystem (addressing, not a credential);
// passwords, ports, query and fragment remain prohibited.
func validateABFSLocation(parsed *url.URL) error {
	user := parsed.User
	if user == nil {
		return errors.New("abfs content_location must name a filesystem: abfs://<filesystem>@<account>.dfs.core.usgovcloudapi.net/<path>")
	}
	if _, hasPassword := user.Password(); hasPassword {
		return errors.New("abfs content_location must not contain credentials")
	}
	filesystem := user.Username()
	if len(filesystem) < 3 || len(filesystem) > 63 || !isLowerDNSLabel(filesystem) {
		return errors.New("abfs filesystem name must be 3-63 lower-case letters, digits or hyphens")
	}
	const govDFSSuffix = ".dfs.core.usgovcloudapi.net"
	host := parsed.Host
	if parsed.Port() != "" || !strings.HasSuffix(host, govDFSSuffix) {
		return errors.New("abfs content_location must target an Azure Government ADLS Gen2 endpoint (dfs.core.usgovcloudapi.net) without a port")
	}
	account := strings.TrimSuffix(host, govDFSSuffix)
	if len(account) < 3 || len(account) > 24 || !isLowerAlnum(account) {
		return errors.New("abfs account name must be 3-24 lower-case letters or digits")
	}
	if parsed.Path == "" || parsed.Path == "/" {
		return errors.New("abfs content_location must include an object path")
	}
	return nil
}

func isLowerDNSLabel(value string) bool {
	if value == "" || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') || character == '-') {
			return false
		}
	}
	return true
}

func isLowerAlnum(value string) bool {
	if value == "" {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9')) {
			return false
		}
	}
	return true
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
