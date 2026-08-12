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
