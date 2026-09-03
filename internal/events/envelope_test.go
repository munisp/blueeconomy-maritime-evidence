package events

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"testing"
	"time"
)

func testSigner(t *testing.T) (*Signer, ed25519.PublicKey) {
	t.Helper()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	signer, err := NewSigner(privateKey, "7")
	if err != nil {
		t.Fatalf("build signer: %v", err)
	}
	return signer, publicKey
}

func TestMessageSignVerifyRoundTrip(t *testing.T) {
	signer, publicKey := testSigner(t)
	envelope, err := Message("evidence.package.received", TopicPackage,
		"22222222-2222-4222-8222-222222222222", "11111111-1111-4111-8111-111111111111",
		json.RawMessage(`{"content_sha256":"277089d91c0bdf4f2e6862ba7e4a07605119431f5d13f726dd352b06f1b206a9"}`),
		map[string]string{"classification": "internal"},
		Provenance{PrincipalID: "service:evidence", PrincipalRole: "evidence-producer"},
		time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC), signer)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	if envelope.EnvelopeVersion != "1.0" || envelope.Producer != Producer {
		t.Fatalf("unexpected envelope header: %+v", envelope)
	}
	if envelope.Provenance.Signature == "" {
		t.Fatal("envelope is unsigned")
	}
	if !envelope.VerifySignature(publicKey) {
		t.Fatal("envelope did not verify against the producer public key")
	}
	// The FHIR entry must be a message Bundle carrying the domain payload.
	var bundle struct {
		ResourceType string `json:"resourceType"`
		Type         string `json:"type"`
		Entry        []struct {
			Resource struct {
				ResourceType string `json:"resourceType"`
			} `json:"resource"`
		} `json:"entry"`
	}
	if err := json.Unmarshal(envelope.FHIR, &bundle); err != nil {
		t.Fatalf("decode FHIR bundle: %v", err)
	}
	if bundle.ResourceType != "Bundle" || bundle.Type != "message" || len(bundle.Entry) != 1 || bundle.Entry[0].Resource.ResourceType != "Basic" {
		t.Fatalf("FHIR bundle shape is invalid: %s", envelope.FHIR)
	}
}

func TestMessageNilSignerFailsClosed(t *testing.T) {
	if _, err := Message("evidence.package.received", TopicPackage, "corr", "subject",
		json.RawMessage(`{}`), nil,
		Provenance{PrincipalID: "p", PrincipalRole: "r"}, time.Now(), nil); err == nil {
		t.Fatal("a nil signer must fail closed")
	}
}

func TestMessageRejectsForeignTopic(t *testing.T) {
	signer, _ := testSigner(t)
	if _, err := Message("evidence.package.received", "ports.booking.v1", "corr", "subject",
		json.RawMessage(`{}`), nil,
		Provenance{PrincipalID: "p", PrincipalRole: "r"}, time.Now(), signer); err == nil {
		t.Fatal("a non-evidence topic must be rejected")
	}
}

func TestVerifyRejectsTamperedEnvelope(t *testing.T) {
	signer, publicKey := testSigner(t)
	envelope, err := Message("evidence.validation.recorded", TopicValidation, "corr", "subject",
		json.RawMessage(`{"validation_status":"validated"}`), nil,
		Provenance{PrincipalID: "p", PrincipalRole: "r"}, time.Now(), signer)
	if err != nil {
		t.Fatalf("build message: %v", err)
	}
	envelope.EventType = "evidence.package.received"
	if envelope.VerifySignature(publicKey) {
		t.Fatal("a tampered envelope must not verify")
	}
}

func TestSignerFromEnvFailsClosed(t *testing.T) {
	t.Setenv(SigningKeyEnv, "")
	t.Setenv(SigningKeyEpochEnv, "")
	if _, err := SignerFromEnv(); err == nil {
		t.Fatal("missing signing key must fail closed")
	}
}

func TestKeyIDCarriesMaritimeEvidenceEpoch(t *testing.T) {
	signer, _ := testSigner(t)
	if signer.KeyID() != "maritime-evidence-7" {
		t.Fatalf("unexpected kid: %s", signer.KeyID())
	}
}
