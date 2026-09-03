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

// TestValidationTransitionMatrix pins the validation state machine: from the
// received state only validated and rejected are legal terminal transitions;
// anything else (including re-received) is rejected before persistence.
func TestValidationTransitionMatrix(t *testing.T) {
	base := ValidationRequest{
		ReasonCode:            "integrity_confirmed",
		ActorSubjectReference: "service:validator",
		OccurredAt:            time.Now().UTC(),
		CorrelationID:         "22222222-2222-4222-8222-222222222222",
	}
	for _, status := range []string{StatusValidated, StatusRejected} {
		request := base
		request.ValidationStatus = status
		if err := request.Validate(); err != nil {
			t.Fatalf("terminal transition %s must be legal: %v", status, err)
		}
	}
	for _, status := range []string{"", StatusReceived, "pending", "VALIDATED"} {
		request := base
		request.ValidationStatus = status
		if err := request.Validate(); err == nil {
			t.Fatalf("transition %q from received must be rejected", status)
		}
	}
}
