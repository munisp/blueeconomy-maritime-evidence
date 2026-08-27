package evidence

import (
	"strings"
	"testing"
	"time"
)

func TestCreateRequestRejectsCredentialBearingObjectLocation(t *testing.T) {
	request := validCreateRequest()
	request.ContentLocation = "https://access-key:secret@object.example.invalid/evidence/document"
	if err := request.Validate(); err == nil {
		t.Fatal("expected credential-bearing content location to be rejected")
	}
}

func TestCreateRequestRejectsMalformedDigest(t *testing.T) {
	request := validCreateRequest()
	request.ContentSHA256 = "not-a-sha256-digest"
	if err := request.Validate(); err == nil {
		t.Fatal("expected malformed digest to be rejected")
	}
}

func TestValidationRejectsReceivedAsTerminalAction(t *testing.T) {
	request := ValidationRequest{
		ValidationStatus:      StatusReceived,
		ReasonCode:            "test",
		ActorSubjectReference: "subject",
		OccurredAt:            time.Now().UTC(),
		CorrelationID:         "01b0e43b-f3a3-43f4-aa3d-113f67e1581a",
	}
	if err := request.Validate(); err == nil {
		t.Fatal("expected received status to be rejected for validation transition")
	}
}

func validCreateRequest() CreateRequest {
	return CreateRequest{
		IdempotencyKey:    "01b0e43b-f3a3-43f4-aa3d-113f67e1581a",
		ExternalReference: "non-production-input-validation-only",
		EvidenceType:      "metadata-validation",
		ContentSHA256:     strings.Repeat("a", 64),
		ContentLocation:   "https://object.example.invalid/evidence/document",
		ReceivedAt:        time.Now().UTC(),
		Classification:    "internal",
		CorrelationID:     "77c0e43b-f3a3-43f4-aa3d-113f67e1581a",
	}
}

func TestContentLocationAcceptsAzureGovernmentABFS(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "")
	request := validCreateRequest()
	request.ContentLocation = "abfs://evidence@stblueeconomygov.dfs.core.usgovcloudapi.net/packages/2026/08/pkg-001.bin"
	if err := request.Validate(); err != nil {
		t.Fatalf("expected Azure Government abfs location to be accepted, got %v", err)
	}
}

func TestContentLocationRejectsCommercialABFSEndpoint(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "")
	request := validCreateRequest()
	request.ContentLocation = "abfs://evidence@stblueeconomy.dfs.core.windows.net/packages/pkg-001.bin"
	if err := request.Validate(); err == nil {
		t.Fatal("expected commercial dfs.core.windows.net endpoint to be rejected")
	}
}

func TestContentLocationRejectsCredentialBearingABFS(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "")
	request := validCreateRequest()
	request.ContentLocation = "abfs://evidence:secret@stblueeconomygov.dfs.core.usgovcloudapi.net/pkg-001.bin"
	if err := request.Validate(); err == nil {
		t.Fatal("expected credential-bearing abfs location to be rejected")
	}
}

func TestContentLocationRejectsABFSWithoutFilesystemOrPath(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "")
	for _, location := range []string{
		"abfs://stblueeconomygov.dfs.core.usgovcloudapi.net/pkg-001.bin",
		"abfs://evidence@stblueeconomygov.dfs.core.usgovcloudapi.net/",
	} {
		request := validCreateRequest()
		request.ContentLocation = location
		if err := request.Validate(); err == nil {
			t.Fatalf("expected %q to be rejected", location)
		}
	}
}

func TestContentLocationRejectsS3ByDefault(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "")
	request := validCreateRequest()
	request.ContentLocation = "s3://legacy-bucket/evidence/document"
	if err := request.Validate(); err == nil {
		t.Fatal("expected s3 scheme to be rejected by default under the Azure Government posture")
	}
}

func TestContentLocationAcceptsS3WithLegacyFlag(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "true")
	request := validCreateRequest()
	request.ContentLocation = "s3://legacy-bucket/evidence/document"
	if err := request.Validate(); err != nil {
		t.Fatalf("expected s3 to be accepted with the legacy flag, got %v", err)
	}
}

func TestLocationPolicyRejectsGarbageFlagValue(t *testing.T) {
	t.Setenv(EnvAllowLegacyS3, "yes-please")
	request := validCreateRequest()
	if err := request.Validate(); err == nil {
		t.Fatal("expected a garbage EVIDENCE_ALLOW_LEGACY_S3 value to fail closed")
	}
}
